package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sde/urlforge/internal/service"
)

// AnalyticsHandler serves aggregated telemetry data
type AnalyticsHandler struct {
	analyticsService service.AnalyticsService
}

// NewAnalyticsHandler creates an AnalyticsHandler instance
func NewAnalyticsHandler(analyticsService service.AnalyticsService) *AnalyticsHandler {
	return &AnalyticsHandler{analyticsService: analyticsService}
}

// GetAnalytics handles GET /api/v1/analytics/:shortCode?range=7d
func (h *AnalyticsHandler) GetAnalytics(c *gin.Context) {
	shortCode := c.Param("shortCode")
	rangeParam := c.DefaultQuery("range", "7d")

	var duration time.Duration
	switch rangeParam {
	case "24h":
		duration = 24 * time.Hour
	case "30d":
		duration = 30 * 24 * time.Hour
	default:
		duration = 7 * 24 * time.Hour
	}

	summary, err := h.analyticsService.GetAnalytics(c.Request.Context(), shortCode, duration)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to retrieve analytics"})
		return
	}

	c.JSON(http.StatusOK, summary)
}
