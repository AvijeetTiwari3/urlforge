package service

import (
	"context"
	"time"

	"github.com/sde/urlforge/internal/model"
	"github.com/sde/urlforge/internal/repository"
)

// AnalyticsService defines operations for retrieving analytics
type AnalyticsService interface {
	GetAnalytics(ctx context.Context, shortCode string, duration time.Duration) (*model.AnalyticsResponse, error)
}

type analyticsService struct {
	repo repository.AnalyticsRepository
}

// NewAnalyticsService creates a new analytics service instance
func NewAnalyticsService(repo repository.AnalyticsRepository) AnalyticsService {
	return &analyticsService{repo: repo}
}

// GetAnalytics queries the click aggregated telemetry
func (s *analyticsService) GetAnalytics(ctx context.Context, shortCode string, duration time.Duration) (*model.AnalyticsResponse, error) {
	if duration <= 0 {
		duration = 7 * 24 * time.Hour // Default 7 days
	}
	return s.repo.GetSummary(ctx, shortCode, duration)
}
