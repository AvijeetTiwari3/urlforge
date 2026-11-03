package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// URLCache defines interface for short URL caching operations
type URLCache interface {
	Get(ctx context.Context, shortCode string) (string, error)
	Set(ctx context.Context, shortCode string, originalURL string, ttl time.Duration) error
	Delete(ctx context.Context, shortCode string) error
	Ping(ctx context.Context) error
}

// RedisCache implements URLCache backed by a Redis cluster or single node
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache creates a new RedisCache client
func NewRedisCache(client *redis.Client) *RedisCache {
	return &RedisCache{client: client}
}

// Get retrieves the original URL for a given short code
func (r *RedisCache) Get(ctx context.Context, shortCode string) (string, error) {
	key := "url:" + shortCode
	return r.client.Get(ctx, key).Result()
}

// Set stores the short code mapping with a specified time to live
func (r *RedisCache) Set(ctx context.Context, shortCode string, originalURL string, ttl time.Duration) error {
	key := "url:" + shortCode
	return r.client.Set(ctx, key, originalURL, ttl).Err()
}

// Delete removes a short code from cache (e.g. on URL deactivation)
func (r *RedisCache) Delete(ctx context.Context, shortCode string) error {
	key := "url:" + shortCode
	return r.client.Del(ctx, key).Err()
}

// Ping checks Redis connectivity
func (r *RedisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}
