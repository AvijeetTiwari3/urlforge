package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"time"

	"github.com/sde/urlforge/internal/bloom"
	"github.com/sde/urlforge/internal/cache"
	"github.com/sde/urlforge/internal/idgen"
	"github.com/sde/urlforge/internal/model"
	"github.com/sde/urlforge/internal/repository"
)

var (
	ErrInvalidURL       = errors.New("invalid or malformed URL")
	ErrInvalidAlias     = errors.New("custom alias can only contain alphanumeric characters, hyphens, and underscores (3-30 chars)")
	ErrAliasAlreadyUsed = errors.New("custom alias is already in use")
	customAliasRegex    = regexp.MustCompile(`^[a-zA-Z0-9_-]{3,30}$`)
)

// URLService defines the business interface for URL manipulation
type URLService interface {
	Shorten(ctx context.Context, req model.ShortenRequest) (*model.ShortenResponse, error)
	GetOriginalURL(ctx context.Context, shortCode string) (string, error)
	Deactivate(ctx context.Context, shortCode string) error
	WarmupBloomFilter(ctx context.Context) error
}

type urlService struct {
	idGen       *idgen.Generator
	repo        repository.URLRepository
	cache       cache.URLCache
	bloomFilter *bloom.Filter
	baseURL     string
}

// NewURLService creates an instance of urlService
func NewURLService(
	idGen *idgen.Generator,
	repo repository.URLRepository,
	cache cache.URLCache,
	bloomFilter *bloom.Filter,
	baseURL string,
) URLService {
	return &urlService{
		idGen:       idGen,
		repo:        repo,
		cache:       cache,
		bloomFilter: bloomFilter,
		baseURL:     baseURL,
	}
}

// Shorten creates a unique short URL according to business rules
func (s *urlService) Shorten(ctx context.Context, req model.ShortenRequest) (*model.ShortenResponse, error) {
	// 1. Validate target URL
	parsedURL, err := url.ParseRequestURI(req.OriginalURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return nil, ErrInvalidURL
	}

	var shortCode string
	var id int64

	// 2. Generate or validate alias
	if req.CustomAlias != nil && *req.CustomAlias != "" {
		alias := *req.CustomAlias
		if !customAliasRegex.MatchString(alias) {
			return nil, ErrInvalidAlias
		}
		// Check if alias already exists
		existing, _ := s.repo.GetByShortCode(ctx, alias)
		if existing != nil {
			return nil, ErrAliasAlreadyUsed
		}
		shortCode = alias
		id, err = s.idGen.NextID()
		if err != nil {
			return nil, fmt.Errorf("failed to generate snowflake id: %w", err)
		}
	} else {
		// Use distributed Snowflake ID generator
		snowflakeID, err := s.idGen.NextID()
		if err != nil {
			return nil, fmt.Errorf("failed to generate snowflake id: %w", err)
		}
		id = snowflakeID
		shortCode = idgen.EncodeBase62(snowflakeID)
	}

	// 3. Compute expiration
	var expiresAt *time.Time
	if req.ExpiresInDays != nil && *req.ExpiresInDays > 0 {
		exp := time.Now().Add(time.Duration(*req.ExpiresInDays) * 24 * time.Hour)
		expiresAt = &exp
	}

	// 4. Persist to primary database
	urlEntity := &model.URL{
		ID:          id,
		ShortCode:   shortCode,
		OriginalURL: req.OriginalURL,
		UserID:      req.UserID,
		CustomAlias: req.CustomAlias,
		ExpiresAt:   expiresAt,
		IsActive:    true,
	}

	if err := s.repo.Create(ctx, urlEntity); err != nil {
		return nil, fmt.Errorf("database insert error: %w", err)
	}

	// 5. Update Bloom filter
	s.bloomFilter.Add(shortCode)

	// 6. Pre-warm Redis cache (24hr TTL)
	ttl := 24 * time.Hour
	if expiresAt != nil {
		remaining := time.Until(*expiresAt)
		if remaining < ttl {
			ttl = remaining
		}
	}
	_ = s.cache.Set(ctx, shortCode, req.OriginalURL, ttl)

	return &model.ShortenResponse{
		ShortCode:   shortCode,
		ShortURL:    fmt.Sprintf("%s/%s", s.baseURL, shortCode),
		OriginalURL: req.OriginalURL,
		ExpiresAt:   expiresAt,
		CreatedAt:   urlEntity.CreatedAt,
	}, nil
}

// GetOriginalURL resolves short code to original URL via Bloom Filter + Cache-Aside pattern
func (s *urlService) GetOriginalURL(ctx context.Context, shortCode string) (string, error) {
	// Optimization 1: Bloom Filter check (O(1))
	// If the filter says NO, it is GUARANTEED not in the database! Saves DB & Redis lookup.
	if !s.bloomFilter.Test(shortCode) {
		return "", repository.ErrURLNotFound
	}

	// Optimization 2: Redis Cache Check
	cachedURL, err := s.cache.Get(ctx, shortCode)
	if err == nil && cachedURL != "" {
		return cachedURL, nil
	}

	// Optimization 3: Read from PostgreSQL Replica on cache miss
	dbURL, err := s.repo.GetByShortCode(ctx, shortCode)
	if err != nil {
		return "", err
	}

	// Calculate appropriate TTL
	ttl := 24 * time.Hour
	if dbURL.ExpiresAt != nil {
		remaining := time.Until(*dbURL.ExpiresAt)
		if remaining <= 0 {
			return "", repository.ErrURLExpired
		}
		if remaining < ttl {
			ttl = remaining
		}
	}

	// Populate Redis Cache asynchronously or synchronously
	_ = s.cache.Set(ctx, shortCode, dbURL.OriginalURL, ttl)

	return dbURL.OriginalURL, nil
}

// Deactivate disables a short URL and evicts it from cache
func (s *urlService) Deactivate(ctx context.Context, shortCode string) error {
	if err := s.repo.Deactivate(ctx, shortCode); err != nil {
		return err
	}
	return s.cache.Delete(ctx, shortCode)
}

// WarmupBloomFilter seeds the in-memory Bloom filter with existing active URLs on pod startup
func (s *urlService) WarmupBloomFilter(ctx context.Context) error {
	codes, err := s.repo.GetAllActiveShortCodes(ctx, 100000)
	if err != nil {
		return err
	}
	for _, code := range codes {
		s.bloomFilter.Add(code)
	}
	log.Printf("[bloom-warmup] seeded Bloom Filter with %d active short codes", len(codes))
	return nil
}
