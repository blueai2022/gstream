package gstream

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisRegistry implements the registry interface using Redis.
type redisRegistry struct {
	client *redis.Client
	ctx    context.Context
}

func newRedisRegistry(cfg RedisConfig) (*redisRegistry, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &redisRegistry{
		client: client,
		ctx:    ctx,
	}, nil
}

func (r *redisRegistry) Set(key, target string) error {
	if err := r.client.Set(r.ctx, key, target, 0).Err(); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return nil
}

func (r *redisRegistry) Get(key string) (string, error) {
	val, err := r.client.Get(r.ctx, key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("key %s not found", key)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}
	return val, nil
}

func (r *redisRegistry) Delete(key string) error {
	if err := r.client.Del(r.ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

func (r *redisRegistry) Close() error {
	return r.client.Close()
}
