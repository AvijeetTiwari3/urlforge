package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/sde/urlforge/internal/config"
	"github.com/sde/urlforge/internal/kafka"
	"github.com/sde/urlforge/internal/repository"
)

func main() {
	cfg := config.Load()
	log.Println("[worker] starting URLForge analytics ingestion worker...")

	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[worker] database connection failed: %v", err)
	}
	defer db.Close()

	analyticsRepo := repository.NewPostgresAnalyticsRepository(db)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, "urlforge-analytics-group", analyticsRepo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go consumer.Start(ctx)

	<-quit
	log.Println("[worker] stopping analytics worker gracefully...")
	cancel()
	log.Println("[worker] worker stopped.")
}
