package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sde/urlforge/internal/model"
)

// EventProducer abstracts publishing click events
type EventProducer interface {
	PublishClick(event model.ClickEvent)
	Close() error
}

// KafkaProducer implements high-throughput async event publishing
type KafkaProducer struct {
	writer *kafka.Writer
	queue  chan model.ClickEvent
	done   chan struct{}
}

// NewKafkaProducer creates a buffered, asynchronous Kafka event producer
func NewKafkaProducer(brokers []string, topic string, bufferSize int) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true, // Non-blocking write
	}

	kp := &KafkaProducer{
		writer: writer,
		queue:  make(chan model.ClickEvent, bufferSize),
		done:   make(chan struct{}),
	}

	go kp.worker()
	return kp
}

// worker batches messages from the in-memory channel to Kafka
func (kp *KafkaProducer) worker() {
	for {
		select {
		case event := <-kp.queue:
			data, err := json.Marshal(event)
			if err != nil {
				log.Printf("[kafka-producer] failed to marshal event: %v", err)
				continue
			}

			err = kp.writer.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(event.ShortCode),
				Value: data,
				Time:  event.ClickedAt,
			})
			if err != nil {
				log.Printf("[kafka-producer] failed to write message: %v", err)
			}
		case <-kp.done:
			return
		}
	}
}

// PublishClick pushes an event onto the non-blocking channel
func (kp *KafkaProducer) PublishClick(event model.ClickEvent) {
	select {
	case kp.queue <- event:
	default:
		log.Printf("[kafka-producer] buffer full, dropping event for code: %s", event.ShortCode)
	}
}

// Close gracefully flushes the writer
func (kp *KafkaProducer) Close() error {
	close(kp.done)
	return kp.writer.Close()
}
