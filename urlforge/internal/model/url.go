package model

import (
	"time"
)

// URL represents the core database entity for shortened URLs
type URL struct {
	ID          int64      `json:"id" db:"id"`
	ShortCode   string     `json:"short_code" db:"short_code"`
	OriginalURL string     `json:"original_url" db:"original_url"`
	UserID      *string    `json:"user_id,omitempty" db:"user_id"`
	CustomAlias *string    `json:"custom_alias,omitempty" db:"custom_alias"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty" db:"expires_at"`
	IsActive    bool       `json:"is_active" db:"is_active"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at"`
}

// ShortenRequest defines payload required to create a new short URL
type ShortenRequest struct {
	OriginalURL   string  `json:"original_url" binding:"required,url"`
	CustomAlias   *string `json:"custom_alias,omitempty"`
	ExpiresInDays *int    `json:"expires_in_days,omitempty"`
	UserID        *string `json:"user_id,omitempty"`
}

// ShortenResponse represents the API response after a successful short URL creation
type ShortenResponse struct {
	ShortCode   string     `json:"short_code"`
	ShortURL    string     `json:"short_url"`
	OriginalURL string     `json:"original_url"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}
