package ro

import (
	"context"
	"sync"
	"time"

	"github.com/nzai/log"
)

var (
	keyPool = sync.Pool{
		New: func() interface{} {
			return &Key{}
		},
	}
)

type Key struct {
	key string
}

func NewKey(key string) *Key {
	k := keyPool.Get().(*Key)
	k.key = key
	return k
}

func (k Key) Del(ctx context.Context) error {
	start := time.Now()
	err := MustGetRedis(ctx).Del(ctx, k.key).Err()
	if err != nil {
		log.Warn(ctx, "delete key failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "delete key successfully",
		log.String("key", k.key),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (k Key) Expire(ctx context.Context, expiration time.Duration) error {
	start := time.Now()
	err := MustGetRedis(ctx).Expire(ctx, k.key, expiration).Err()
	if err != nil {
		log.Warn(ctx, "expire key failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "expire value successfully",
		log.String("key", k.key),
		log.Duration("expiration", expiration),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (k Key) ExpireAt(ctx context.Context, t time.Time) error {
	start := time.Now()
	err := MustGetRedis(ctx).ExpireAt(ctx, k.key, t).Err()
	if err != nil {
		log.Warn(ctx, "expire key failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "expire value successfully",
		log.String("key", k.key),
		log.Time("time", t),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (k Key) TTL(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	ttl, err := MustGetRedis(ctx).TTL(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "get key ttl failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return 0, err
	}

	log.Debug(ctx, "get key ttl successfully",
		log.String("key", k.key),
		log.Duration("ttl", ttl),
		log.Duration("duration", time.Since(start)))

	return ttl, nil
}

func (k Key) Exists(ctx context.Context) (bool, error) {
	start := time.Now()
	count, err := MustGetRedis(ctx).Exists(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "check key exists failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return false, err
	}

	exists := count > 0
	log.Debug(ctx, "check key exists successfully",
		log.String("key", k.key),
		log.Bool("exists", exists),
		log.Duration("duration", time.Since(start)))

	return exists, nil
}

func (k Key) Key() string {
	return k.key
}
