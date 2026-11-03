package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sde/urlforge/internal/kafka"
	"github.com/sde/urlforge/internal/middleware"
	"github.com/sde/urlforge/internal/model"
	"github.com/sde/urlforge/internal/repository"
	"github.com/sde/urlforge/internal/service"
)

// RedirectHandler processes high-throughput redirect lookups
type RedirectHandler struct {
	urlService service.URLService
	producer   kafka.EventProducer
}

// NewRedirectHandler creates a new RedirectHandler instance
func NewRedirectHandler(urlService service.URLService, producer kafka.EventProducer) *RedirectHandler {
	return &RedirectHandler{
		urlService: urlService,
		producer:   producer,
	}
}

// Redirect handles GET /:shortCode
func (h *RedirectHandler) Redirect(c *gin.Context) {
	shortCode := c.Param("shortCode")
	if shortCode == "" || len(shortCode) > 50 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid short code"})
		return
	}

	// 1. Resolve original target URL
	targetURL, err := h.urlService.GetOriginalURL(c.Request.Context(), shortCode)
	if err != nil {
		if errors.Is(err, repository.ErrURLNotFound) {
			middleware.RedirectCounter.WithLabelValues("not_found").Inc()
			c.JSON(http.StatusNotFound, gin.H{"error": "short URL not found"})
			return
		}
		if errors.Is(err, repository.ErrURLExpired) {
			middleware.RedirectCounter.WithLabelValues("expired").Inc()
			c.JSON(http.StatusGone, gin.H{"error": "short URL has expired"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve URL"})
		return
	}

	middleware.RedirectCounter.WithLabelValues("success").Inc()

	// 2. Dispatch click telemetry asynchronously to Kafka
	if h.producer != nil {
		userAgent := c.GetHeader("User-Agent")
		referrer := c.GetHeader("Referer")
		clientIP := c.ClientIP()

		// Hash IP to preserve privacy (GDPR compliant)
		hash := sha256.Sum256([]byte(clientIP))
		ipHash := hex.EncodeToString(hash[:16])

		device := detectDevice(userAgent)
		country := c.GetHeader("CF-IPCountry") // Read from Cloudflare/LB header if present
		if country == "" {
			country = "US" // Default fallback
		}

		event := model.ClickEvent{
			ShortCode: shortCode,
			ClickedAt: time.Now(),
			IPHash:    ipHash,
			UserAgent: userAgent,
			Referrer:  referrer,
			Country:   country,
			Device:    device,
		}

		h.producer.PublishClick(event)
	}

	// 3. HTTP 302 Found allows caching at browser while maintaining per-click telemetry
	c.Redirect(http.StatusFound, targetURL)
}

func detectDevice(ua string) string {
	uaLower := strings.ToLower(ua)
	if strings.Contains(uaLower, "tablet") || strings.Contains(uaLower, "ipad") {
		return "tablet"
	}
	if strings.Contains(uaLower, "mobile") || strings.Contains(uaLower, "android") || strings.Contains(uaLower, "iphone") {
		return "mobile"
	}
	return "desktop"
}
