package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sde/urlforge/internal/model"
	"github.com/sde/urlforge/internal/repository"
)

// Consumer consumes click events from Kafka and persists them to analytics storage
type Consumer struct {
	reader *kafka.Reader
	repo   repository.AnalyticsRepository
}

// NewConsumer creates a Kafka consumer group reader
func NewConsumer(brokers []string, topic string, groupID string, repo repository.AnalyticsRepository) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader: reader,
		repo:   repo,
	}
}

// Start begins the event processing loop until ctx is canceled
func (c *Consumer) Start(ctx context.Context) {
	log.Println("[kafka-consumer] started consuming click events...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[kafka-consumer] shutting down reader...")
			_ = c.reader.Close()
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[kafka-consumer] fetch error: %v", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			var event model.ClickEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("[kafka-consumer] deserialization error: %v", err)
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			if err := c.repo.RecordClick(ctx, &event); err != nil {
				log.Printf("[kafka-consumer] failed to persist click: %v", err)
			} else {
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[kafka-consumer] commit error: %v", err)
				}
			}
		}
	}
}
