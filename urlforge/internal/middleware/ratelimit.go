package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sde/urlforge/internal/ratelimit"
)

// RateLimiter returns a Gin middleware enforcing token bucket limits per client IP
func RateLimiter(limiter ratelimit.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		allowed, err := limiter.Allow(c.Request.Context(), ip)
		if err != nil {
			// Fail open on rate limiter technical failure to protect availability
			c.Next()
			return
		}

		if !allowed {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":   "rate limit exceeded",
				"message": "Too many requests. Please slow down.",
			})
			return
		}

		c.Next()
	}
}
