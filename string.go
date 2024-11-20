package ro

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nzai/log"
)

var (
	stringKeyPool = &sync.Pool{
		New: func() interface{} {
			return &StringKey{}
		},
	}
	stringParameterKeyPool = &sync.Pool{
		New: func() interface{} {
			return &StringParameterKey{}
		},
	}
)

type StringKey struct {
	*Key
}

func NewStringKey(key string) *StringKey {
	k := stringKeyPool.Get().(*StringKey)
	k.Key = NewKey(key)
	return k
}

func (k StringKey) Get(ctx context.Context) (string, error) {
	start := time.Now()
	value, err := MustGetRedis(ctx).Get(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "get key value failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return "", err
	}

	log.Debug(ctx, "get value successfully",
		log.String("key", k.key),
		log.String("value", value),
		log.Duration("duration", time.Since(start)))

	return value, nil
}

func (k StringKey) GetDefault(ctx context.Context, defaultValue string) string {
	value, err := k.Get(ctx)
	if err != nil {
		log.Debug(ctx, "get value failed, use default value instead",
			log.Err(err), log.String("key", k.key),
			log.String("defaultValue", defaultValue))
		return defaultValue
	}

	return value
}

func (k StringKey) GetInt(ctx context.Context) (int, error) {
	value, err := k.Get(ctx)
	if err != nil {
		return 0, err
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		log.Warn(ctx, "get int value failed", log.Err(err), log.String("value", value))
		return 0, err
	}

	return intValue, nil
}

func (k StringKey) GetInt64(ctx context.Context) (int64, error) {
	value, err := k.Get(ctx)
	if err != nil {
		return 0, err
	}

	intValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Warn(ctx, "get int value failed", log.Err(err), log.String("value", value))
		return 0, err
	}

	return intValue, nil
}

func (k StringKey) GetObject(ctx context.Context, obj interface{}) error {
	value, err := k.Get(ctx)
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(value), obj)
	if err != nil {
		log.Warn(ctx, "get object failed",
			log.Err(err),
			log.String("value", value),
			log.Any("obj", obj))
		return err
	}

	return nil
}

func (k StringKey) Set(ctx context.Context, value string, expiration time.Duration) error {
	start := time.Now()
	err := MustGetRedis(ctx).Set(ctx, k.key, value, expiration).Err()
	if err != nil {
		log.Warn(ctx, "set key value failed",
			log.Err(err),
			log.String("key", k.key),
			log.String("value", value),
			log.Duration("expiration", expiration),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "set value successfully",
		log.String("key", k.key),
		log.String("value", value),
		log.Duration("expiration", expiration),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (k StringKey) SetInt(ctx context.Context, value int, expiration time.Duration) error {
	return k.Set(ctx, strconv.Itoa(value), expiration)
}

func (k StringKey) SetInt64(ctx context.Context, value int64, expiration time.Duration) error {
	return k.Set(ctx, strconv.FormatInt(value, 10), expiration)
}

func (k StringKey) SetObject(ctx context.Context, obj interface{}, expiration time.Duration) error {
	buffer, err := json.Marshal(obj)
	if err != nil {
		log.Warn(ctx, "marshal object failed",
			log.Err(err),
			log.Any("obj", obj))
		return err
	}

	return k.Set(ctx, string(buffer), expiration)
}

func (k StringKey) SetNX(ctx context.Context, value string, expiration time.Duration) (bool, error) {
	start := time.Now()
	success, err := MustGetRedis(ctx).SetNX(ctx, k.key, value, expiration).Result()
	if err != nil {
		log.Warn(ctx, "setnx key value failed",
			log.Err(err),
			log.String("key", k.key),
			log.String("value", value),
			log.Duration("expiration", expiration),
			log.Duration("duration", time.Since(start)))
		return false, err
	}

	log.Debug(ctx, "setnx value finished",
		log.String("key", k.key),
		log.String("value", value),
		log.Duration("expiration", expiration),
		log.Bool("success", success),
		log.Duration("duration", time.Since(start)))

	return success, nil
}

func (k StringKey) GetLocker(ctx context.Context, expiration time.Duration, handler func(context.Context) error) error {
	start := time.Now()
	got, err := k.SetNX(ctx, "", expiration)
	if err != nil {
		return err
	}

	if !got {
		log.Debug(ctx, "get locker failed",
			log.String("key", k.key),
			log.Duration("expiration", expiration),
			log.Duration("duration", time.Since(start)))
		return nil
	}

	log.Debug(ctx, "get locker successfully",
		log.String("key", k.key),
		log.Duration("expiration", expiration),
		log.Duration("duration", time.Since(start)))

	ctxWithTimeout, cancel := context.WithTimeout(ctx, expiration)
	defer cancel()

	funcDone := make(chan error)
	go func() {
		defer func() {
			if err1 := recover(); err1 != nil {
				log.Warn(ctxWithTimeout, "handler panic", log.Any("recover error", err1))
				funcDone <- fmt.Errorf("handler panic: %+v", err1)
			}
		}()

		// call handler
		funcDone <- handler(ctxWithTimeout)
	}()

	select {
	case err = <-funcDone:
		log.Debug(ctxWithTimeout, "locker handler done",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("expiration", expiration),
			log.Duration("duration", time.Since(start)))
	case <-ctxWithTimeout.Done():
		// context deadline exceeded
		err = ctxWithTimeout.Err()
		log.Warn(ctxWithTimeout, "locker context deadline exceeded",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("expiration", expiration))
	}

	return err
}

func (k StringKey) Increase(ctx context.Context) (int64, error) {
	start := time.Now()
	newValue, err := MustGetRedis(ctx).Incr(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "increase value failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return 0, err
	}

	log.Debug(ctx, "increase value successfully",
		log.String("key", k.key),
		log.Int64("newValue", newValue),
		log.Duration("duration", time.Since(start)))

	return newValue, nil
}

func (k StringKey) IncreaseBy(ctx context.Context, value int64) (int64, error) {
	start := time.Now()
	newValue, err := MustGetRedis(ctx).IncrBy(ctx, k.key, value).Result()
	if err != nil {
		log.Warn(ctx, "increase by value failed",
			log.Err(err),
			log.String("key", k.key),
			log.Int64("value", value),
			log.Duration("duration", time.Since(start)))
		return 0, err
	}

	log.Debug(ctx, "increase by value successfully",
		log.String("key", k.key),
		log.Int64("value", value),
		log.Int64("newValue", newValue),
		log.Duration("duration", time.Since(start)))

	return newValue, nil
}

type StringParameterKey struct {
	pattern string
}

func NewStringParameterKey(pattern string) *StringParameterKey {
	k := stringParameterKeyPool.Get().(*StringParameterKey)
	k.pattern = pattern
	return k
}

func (k StringParameterKey) Param(parameters ...interface{}) *StringKey {
	return NewStringKey(fmt.Sprintf(k.pattern, parameters...))
}
