package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenBucketLuaScript provides an atomic Redis-based token bucket rate limiter
const TokenBucketLuaScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2]) -- tokens per millisecond
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    local generated = delta * refill_rate
    tokens = math.min(capacity, tokens + generated)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    local ttl = math.ceil(capacity / refill_rate)
    redis.call('PEXPIRE', key, ttl)
    return 1
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    local ttl = math.ceil(capacity / refill_rate)
    redis.call('PEXPIRE', key, ttl)
    return 0
end
`

// Limiter interface allows switching between distributed Redis and local in-memory
type Limiter interface {
	Allow(ctx context.Context, identifier string) (bool, error)
}

// RedisLimiter implements distributed token bucket rate limiting on Redis
type RedisLimiter struct {
	client     *redis.Client
	capacity   int64
	refillRate float64 // tokens per ms
	scriptSHA  string
}

// NewRedisLimiter initializes and pre-loads the Lua script into Redis
func NewRedisLimiter(client *redis.Client, capacity int64, refillPerSecond float64) (*RedisLimiter, error) {
	refillRateMs := refillPerSecond / 1000.0

	sha, err := client.ScriptLoad(context.Background(), TokenBucketLuaScript).Result()
	if err != nil {
		return nil, err
	}

	return &RedisLimiter{
		client:     client,
		capacity:   capacity,
		refillRate: refillRateMs,
		scriptSHA:  sha,
	}, nil
}

// Allow evaluates if a request with given identifier is permitted
func (r *RedisLimiter) Allow(ctx context.Context, identifier string) (bool, error) {
	key := "ratelimit:" + identifier
	nowMs := time.Now().UnixMilli()

	result, err := r.client.EvalSha(ctx, r.scriptSHA, []string{key}, r.capacity, r.refillRate, nowMs, 1).Int()
	if err != nil {
		// If NOSCRIPT error occurred (e.g. Redis restarted), fallback to Eval
		if redis.HasErrorPrefix(err, "NOSCRIPT") {
			result, err = r.client.Eval(ctx, TokenBucketLuaScript, []string{key}, r.capacity, r.refillRate, nowMs, 1).Int()
			if err != nil {
				return false, err
			}
			return result == 1, nil
		}
		return false, err
	}

	return result == 1, nil
}

// InMemoryBucket maintains state for a single entity in local memory
type InMemoryBucket struct {
	tokens      float64
	lastUpdated time.Time
}

// InMemoryLimiter provides fallback rate limiting when Redis is not active
type InMemoryLimiter struct {
	mu           sync.Mutex
	buckets      map[string]*InMemoryBucket
	capacity     float64
	refillRateMs float64
}

// NewInMemoryLimiter creates an in-memory token bucket limiter
func NewInMemoryLimiter(capacity float64, refillPerSecond float64) *InMemoryLimiter {
	return &InMemoryLimiter{
		buckets:      make(map[string]*InMemoryBucket),
		capacity:     capacity,
		refillRateMs: refillPerSecond / 1000.0,
	}
}

// Allow evaluates rate limit locally in memory
func (m *InMemoryLimiter) Allow(_ context.Context, identifier string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	bucket, exists := m.buckets[identifier]

	if !exists {
		m.buckets[identifier] = &InMemoryBucket{
			tokens:      m.capacity - 1.0,
			lastUpdated: now,
		}
		return true, nil
	}

	deltaMs := float64(now.Sub(bucket.lastUpdated).Milliseconds())
	bucket.tokens = bucket.tokens + (deltaMs * m.refillRateMs)
	if bucket.tokens > m.capacity {
		bucket.tokens = m.capacity
	}
	bucket.lastUpdated = now

	if bucket.tokens >= 1.0 {
		bucket.tokens -= 1.0
		return true, nil
	}

	return false, nil
}
