package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/sde/urlforge/internal/bloom"
	"github.com/sde/urlforge/internal/cache"
	"github.com/sde/urlforge/internal/config"
	"github.com/sde/urlforge/internal/handler"
	"github.com/sde/urlforge/internal/idgen"
	"github.com/sde/urlforge/internal/kafka"
	"github.com/sde/urlforge/internal/middleware"
	"github.com/sde/urlforge/internal/ratelimit"
	"github.com/sde/urlforge/internal/repository"
	"github.com/sde/urlforge/internal/service"
)

func main() {
	cfg := config.Load()
	log.Printf("[main] starting URLForge API (environment=%s, worker_id=%d)...", cfg.Environment, cfg.WorkerID)

	if cfg.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	// 1. Connect to PostgreSQL Primary
	primaryDB, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[main] failed to connect to primary database: %v", err)
	}
	defer primaryDB.Close()

	// Auto-run schema migrations to ensure tables exist
	if err := runMigrations(primaryDB); err != nil {
		log.Printf("[main] warning: database migration check: %v", err)
	} else {
		log.Println("[main] database schema migrations verified successfully")
	}

	// Connect to Read Replica (if configured differently)
	var replicaDB *sql.DB
	if cfg.ReadReplicaURL != "" && cfg.ReadReplicaURL != cfg.DatabaseURL {
		replicaDB, err = sql.Open("postgres", cfg.ReadReplicaURL)
		if err != nil {
			log.Printf("[main] warning: failed to connect to read replica, using primary: %v", err)
			replicaDB = primaryDB
		} else {
			defer replicaDB.Close()
		}
	} else {
		replicaDB = primaryDB
	}

	// 2. Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisURL,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     100,
	})
	defer rdb.Close()

	// 3. Initialize Distributed Snowflake ID Generator
	generator, err := idgen.NewGenerator(cfg.WorkerID)
	if err != nil {
		log.Fatalf("[main] failed to initialize snowflake generator: %v", err)
	}

	// 4. Initialize Bloom Filter (1 million items capacity, 0.5% FPR)
	bloomFilter := bloom.NewFilter(1_000_000, 0.005)

	// 5. Initialize Repositories and Cache
	urlRepo := repository.NewPostgresURLRepository(primaryDB, replicaDB)
	analyticsRepo := repository.NewPostgresAnalyticsRepository(primaryDB)
	urlCache := cache.NewRedisCache(rdb)

	// 6. Warm up Bloom filter asynchronously
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := urlRepo.Ping(ctx); err == nil {
			svc := service.NewURLService(generator, urlRepo, urlCache, bloomFilter, cfg.BaseURL)
			_ = svc.WarmupBloomFilter(ctx)
		}
	}()

	// 7. Initialize Rate Limiter (Token bucket: 100 requests capacity, 50 req/sec refill)
	var limiter ratelimit.Limiter
	redisLimiter, err := ratelimit.NewRedisLimiter(rdb, 100, 50.0)
	if err != nil {
		log.Printf("[main] warning: failed to load redis rate limit script, using in-memory limiter: %v", err)
		limiter = ratelimit.NewInMemoryLimiter(100, 50.0)
	} else {
		limiter = redisLimiter
	}

	// 8. Initialize Kafka Event Producer
	producer := kafka.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaTopic, 10000)
	defer producer.Close()

	// 9. Initialize Business Services
	urlSvc := service.NewURLService(generator, urlRepo, urlCache, bloomFilter, cfg.BaseURL)
	analyticsSvc := service.NewAnalyticsService(analyticsRepo)

	// 10. Initialize Handlers
	shortenH := handler.NewShortenHandler(urlSvc)
	redirectH := handler.NewRedirectHandler(urlSvc, producer)
	analyticsH := handler.NewAnalyticsHandler(analyticsSvc)
	healthH := handler.NewHealthHandler(urlRepo, urlCache)

	// 11. Setup Gin Router
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(middleware.StructuredLogger())
	router.Use(middleware.PrometheusMetrics())

	// Root Welcome & Health Endpoint for Cloud Load Balancers & Recruiters
	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service":      "URLForge Distributed URL Shortener Engine",
			"status":       "HEALTHY",
			"version":      "1.0.0",
			"architecture": "Go + PostgreSQL + Redis + Kafka + Snowflake ID Generator",
			"endpoints": gin.H{
				"health":    "GET /health",
				"ready":     "GET /ready",
				"shorten":   "POST /api/v1/shorten",
				"redirect":  "GET /:shortCode",
				"analytics": "GET /api/v1/analytics/:shortCode",
				"metrics":   "GET /metrics",
			},
		})
	})

	// Health & Metrics
	router.GET("/health", healthH.Liveness)
	router.GET("/ready", healthH.Readiness)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API Group with Rate Limiting
	api := router.Group("/api/v1")
	api.Use(middleware.RateLimiter(limiter))
	{
		api.POST("/shorten", shortenH.Shorten)
		api.GET("/analytics/:shortCode", analyticsH.GetAnalytics)
	}

	// Catch-all Redirect Route
	router.GET("/:shortCode", redirectH.Redirect)

	// 12. Graceful HTTP Server Execution
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("[main] server listening on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[main] HTTP server failure: %v", err)
		}
	}()

	// Wait for OS shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[main] graceful shutdown signal received...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[main] server forced to shutdown: %v", err)
	}

	log.Println("[main] server exited successfully")
}

func runMigrations(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS urls (
		id            BIGINT PRIMARY KEY,
		short_code    VARCHAR(50) NOT NULL UNIQUE,
		original_url  TEXT NOT NULL,
		user_id       VARCHAR(64),
		custom_alias  VARCHAR(50) UNIQUE,
		expires_at    TIMESTAMPTZ,
		is_active     BOOLEAN NOT NULL DEFAULT TRUE,
		created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_urls_short_code_active ON urls(short_code) WHERE is_active = TRUE;
	CREATE INDEX IF NOT EXISTS idx_urls_expires_at ON urls(expires_at) WHERE expires_at IS NOT NULL;

	CREATE TABLE IF NOT EXISTS click_events (
		id          BIGSERIAL,
		short_code  VARCHAR(50) NOT NULL,
		clicked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		ip_hash     VARCHAR(64),
		country     VARCHAR(10),
		device      VARCHAR(20),
		referrer    TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_clicks_short_code_time ON click_events(short_code, clicked_at DESC);
	`
	_, err := db.Exec(schema)
	return err
}
