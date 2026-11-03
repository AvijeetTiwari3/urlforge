package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sde/urlforge/internal/cache"
	"github.com/sde/urlforge/internal/repository"
)

// HealthHandler manages Kubernetes probes and health checks
type HealthHandler struct {
	repo  repository.URLRepository
	cache cache.URLCache
}

// NewHealthHandler creates a new HealthHandler instance
func NewHealthHandler(repo repository.URLRepository, cache cache.URLCache) *HealthHandler {
	return &HealthHandler{repo: repo, cache: cache}
}

// Liveness returns 200 OK as long as the process is alive
func (h *HealthHandler) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "UP"})
}

// Readiness verifies active connections to downstream dependencies (DB, Redis)
func (h *HealthHandler) Readiness(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := h.repo.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "DOWN",
			"error":  "database unavailable: " + err.Error(),
		})
		return
	}

	if err := h.cache.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "DOWN",
			"error":  "cache unavailable: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "READY"})
}
