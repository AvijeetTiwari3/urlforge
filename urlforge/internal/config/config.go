package config

import (
	"os"
	"strconv"
	"strings"
)

// Config encapsulates runtime configuration options loaded from environment variables
type Config struct {
	Port           string
	BaseURL        string
	DatabaseURL    string
	ReadReplicaURL string
	RedisURL       string
	KafkaBrokers   []string
	KafkaTopic     string
	WorkerID       int64
	Environment    string
}

// Load loads configuration from environment variables with safe defaults
func Load() *Config {
	port := getEnv("PORT", "8080")
	baseURL := getEnv("BASE_URL", "http://localhost:"+port)
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/urlforge?sslmode=disable")
	replicaURL := getEnv("READ_REPLICA_URL", dbURL) // Default to primary if no replica is set
	redisURL := getEnv("REDIS_URL", "localhost:6379")
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "urlforge.clicks")
	env := getEnv("ENVIRONMENT", "development")

	workerIDStr := getEnv("WORKER_ID", "1")
	workerID, err := strconv.ParseInt(workerIDStr, 10, 64)
	if err != nil {
		workerID = 1
	}

	brokers := strings.Split(kafkaBrokersStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	return &Config{
		Port:           port,
		BaseURL:        baseURL,
		DatabaseURL:    dbURL,
		ReadReplicaURL: replicaURL,
		RedisURL:       redisURL,
		KafkaBrokers:   brokers,
		KafkaTopic:     kafkaTopic,
		WorkerID:       workerID,
		Environment:    env,
	}
}

func getEnv(key, defaultVal string) string {
	if val, exists := os.LookupEnv(key); exists && val != "" {
		return val
	}
	return defaultVal
}
