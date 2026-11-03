package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/sde/urlforge/internal/model"
)

var (
	ErrURLNotFound = errors.New("url not found")
	ErrURLExpired  = errors.New("url has expired")
)

// URLRepository defines persistent storage operations for URLs
type URLRepository interface {
	Create(ctx context.Context, u *model.URL) error
	GetByShortCode(ctx context.Context, shortCode string) (*model.URL, error)
	Deactivate(ctx context.Context, shortCode string) error
	GetAllActiveShortCodes(ctx context.Context, limit int) ([]string, error)
	Ping(ctx context.Context) error
}

// PostgresURLRepository implements URLRepository using database/sql
type PostgresURLRepository struct {
	primaryDB *sql.DB
	replicaDB *sql.DB
}

// NewPostgresURLRepository initializes the repository with connection pool tuning
func NewPostgresURLRepository(primary *sql.DB, replica *sql.DB) *PostgresURLRepository {
	// Tune primary connection pool for high-throughput writes
	primary.SetMaxOpenConns(50)
	primary.SetMaxIdleConns(25)
	primary.SetConnMaxLifetime(5 * time.Minute)

	if replica != nil {
		// Tune replica connection pool for read spikes
		replica.SetMaxOpenConns(100)
		replica.SetMaxIdleConns(50)
		replica.SetConnMaxLifetime(5 * time.Minute)
	} else {
		replica = primary
	}

	return &PostgresURLRepository{
		primaryDB: primary,
		replicaDB: replica,
	}
}

// Create inserts a newly generated URL record into the primary database
func (r *PostgresURLRepository) Create(ctx context.Context, u *model.URL) error {
	query := `
		INSERT INTO urls (id, short_code, original_url, user_id, custom_alias, expires_at, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	now := time.Now()
	u.CreatedAt = now
	u.UpdatedAt = now
	u.IsActive = true

	_, err := r.primaryDB.ExecContext(ctx, query,
		u.ID,
		u.ShortCode,
		u.OriginalURL,
		u.UserID,
		u.CustomAlias,
		u.ExpiresAt,
		u.IsActive,
		u.CreatedAt,
		u.UpdatedAt,
	)

	return err
}

// GetByShortCode queries the read replica for an active, unexpired URL
func (r *PostgresURLRepository) GetByShortCode(ctx context.Context, shortCode string) (*model.URL, error) {
	query := `
		SELECT id, short_code, original_url, user_id, custom_alias, expires_at, is_active, created_at, updated_at
		FROM urls
		WHERE short_code = $1 AND is_active = TRUE
		LIMIT 1
	`

	var u model.URL
	err := r.replicaDB.QueryRowContext(ctx, query, shortCode).Scan(
		&u.ID,
		&u.ShortCode,
		&u.OriginalURL,
		&u.UserID,
		&u.CustomAlias,
		&u.ExpiresAt,
		&u.IsActive,
		&u.CreatedAt,
		&u.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrURLNotFound
		}
		return nil, err
	}

	// Validate expiration timestamp if set
	if u.ExpiresAt != nil && time.Now().After(*u.ExpiresAt) {
		return nil, ErrURLExpired
	}

	return &u, nil
}

// Deactivate sets is_active to false on the primary database
func (r *PostgresURLRepository) Deactivate(ctx context.Context, shortCode string) error {
	query := `UPDATE urls SET is_active = FALSE, updated_at = NOW() WHERE short_code = $1`
	res, err := r.primaryDB.ExecContext(ctx, query, shortCode)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrURLNotFound
	}
	return nil
}

// GetAllActiveShortCodes retrieves active codes for warming Bloom Filters at startup
func (r *PostgresURLRepository) GetAllActiveShortCodes(ctx context.Context, limit int) ([]string, error) {
	query := `SELECT short_code FROM urls WHERE is_active = TRUE LIMIT $1`
	rows, err := r.replicaDB.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var codes []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, err
		}
		codes = append(codes, code)
	}

	return codes, rows.Err()
}

// Ping verifies connectivity to the database
func (r *PostgresURLRepository) Ping(ctx context.Context) error {
	return r.primaryDB.PingContext(ctx)
}
