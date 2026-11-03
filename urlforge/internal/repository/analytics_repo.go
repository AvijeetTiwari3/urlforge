package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/sde/urlforge/internal/model"
)

// AnalyticsRepository defines operations for recording and reading click telemetry
type AnalyticsRepository interface {
	RecordClick(ctx context.Context, event *model.ClickEvent) error
	GetSummary(ctx context.Context, shortCode string, duration time.Duration) (*model.AnalyticsResponse, error)
}

// PostgresAnalyticsRepository implements AnalyticsRepository backed by TimescaleDB / Postgres
type PostgresAnalyticsRepository struct {
	db *sql.DB
}

// NewPostgresAnalyticsRepository initializes analytics repository
func NewPostgresAnalyticsRepository(db *sql.DB) *PostgresAnalyticsRepository {
	return &PostgresAnalyticsRepository{db: db}
}

// RecordClick inserts a click event into the time-series hypertable
func (r *PostgresAnalyticsRepository) RecordClick(ctx context.Context, event *model.ClickEvent) error {
	query := `
		INSERT INTO click_events (short_code, clicked_at, ip_hash, country, device, referrer)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := r.db.ExecContext(ctx, query,
		event.ShortCode,
		event.ClickedAt,
		event.IPHash,
		event.Country,
		event.Device,
		event.Referrer,
	)
	return err
}

// GetSummary aggregates metrics for a specific short code over the given duration
func (r *PostgresAnalyticsRepository) GetSummary(ctx context.Context, shortCode string, duration time.Duration) (*model.AnalyticsResponse, error) {
	since := time.Now().Add(-duration)

	resp := &model.AnalyticsResponse{
		ShortCode:       shortCode,
		DeviceBreakdown: make(map[string]int64),
		TopCountries:    []model.CountryMetric{},
		ClicksOverTime:  []model.HourlyMetric{},
	}

	// 1. Total and Unique Clicks
	countQuery := `
		SELECT COUNT(*), COUNT(DISTINCT ip_hash)
		FROM click_events
		WHERE short_code = $1 AND clicked_at >= $2
	`
	err := r.db.QueryRowContext(ctx, countQuery, shortCode, since).Scan(&resp.TotalClicks, &resp.UniqueClicks)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	// 2. Country aggregation
	countryQuery := `
		SELECT COALESCE(country, 'Unknown'), COUNT(*)
		FROM click_events
		WHERE short_code = $1 AND clicked_at >= $2
		GROUP BY country
		ORDER BY COUNT(*) DESC
		LIMIT 10
	`
	rows, err := r.db.QueryContext(ctx, countryQuery, shortCode, since)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var cm model.CountryMetric
			if err := rows.Scan(&cm.Country, &cm.Clicks); err == nil {
				resp.TopCountries = append(resp.TopCountries, cm)
			}
		}
	}

	// 3. Device aggregation
	deviceQuery := `
		SELECT COALESCE(device, 'other'), COUNT(*)
		FROM click_events
		WHERE short_code = $1 AND clicked_at >= $2
		GROUP BY device
	`
	devRows, err := r.db.QueryContext(ctx, deviceQuery, shortCode, since)
	if err == nil {
		defer devRows.Close()
		for devRows.Next() {
			var device string
			var count int64
			if err := devRows.Scan(&device, &count); err == nil {
				resp.DeviceBreakdown[device] = count
			}
		}
	}

	// 4. Hourly time-series bucket
	hourlyQuery := `
		SELECT date_trunc('hour', clicked_at) AS hr, COUNT(*)
		FROM click_events
		WHERE short_code = $1 AND clicked_at >= $2
		GROUP BY hr
		ORDER BY hr ASC
	`
	hrRows, err := r.db.QueryContext(ctx, hourlyQuery, shortCode, since)
	if err == nil {
		defer hrRows.Close()
		for hrRows.Next() {
			var hm model.HourlyMetric
			if err := hrRows.Scan(&hm.Hour, &hm.Clicks); err == nil {
				resp.ClicksOverTime = append(resp.ClicksOverTime, hm)
			}
		}
	}

	return resp, nil
}
