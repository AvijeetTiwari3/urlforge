package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "urlforge_http_requests_total",
			Help: "Total number of HTTP requests processed by endpoint and status code.",
		},
		[]string{"method", "path", "status"},
	)

	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "urlforge_http_request_duration_seconds",
			Help:    "Latency histogram of HTTP requests.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		},
		[]string{"method", "path"},
	)

	RedirectCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "urlforge_redirects_total",
			Help: "Count of redirection outcomes (cache_hit, cache_miss, not_found).",
		},
		[]string{"result"},
	)
)

// PrometheusMetrics returns a Gin middleware tracking latency and request volume
func PrometheusMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start).Seconds()

		status := strconv.Itoa(c.Writer.Status())
		path := c.FullPath()
		if path == "" {
			path = "unknown"
		}

		HttpRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		HttpRequestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}
