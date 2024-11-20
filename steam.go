package ro

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nzai/log"
	"github.com/redis/go-redis/v9"
)

var (
	streamKeyPool = &sync.Pool{
		New: func() interface{} {
			return &StreamKey{}
		},
	}
	streamParameterKeyPool = &sync.Pool{
		New: func() interface{} {
			return &StreamParameterKey{}
		},
	}
)

type StreamKey struct {
	*Key
}

func NewStreamKey(key string) *StreamKey {
	k := streamKeyPool.Get().(*StreamKey)
	k.Key = NewKey(key)
	return k
}

type StreamParameterKey struct {
	pattern string
}

func NewStreamParameterKey(pattern string) *StreamParameterKey {
	k := streamParameterKeyPool.Get().(*StreamParameterKey)
	k.pattern = pattern
	return k
}

func (k StreamParameterKey) Param(parameters ...interface{}) *StreamKey {
	return NewStreamKey(fmt.Sprintf(k.pattern, parameters...))
}

func (s StreamKey) XAdd(ctx context.Context, id string, maxLen, limit int64, values map[string]interface{}) (string, error) {
	arg := &redis.XAddArgs{
		Stream: s.key,
		MaxLen: maxLen,
		Limit:  limit,
		ID:     id,
		Values: values,
	}

	start := time.Now()
	messageID, err := MustGetRedis(ctx).XAdd(ctx, arg).Result()
	if err != nil {
		log.Warn(ctx, "xadd failed",
			log.Err(err),
			log.Any("arg", arg),
			log.Duration("duration", time.Since(start)))
		return "", err
	}

	log.Debug(ctx, "xadd successfully",
		log.Any("arg", arg),
		log.String("messageID", messageID),
		log.Duration("duration", time.Since(start)))

	return messageID, nil
}

func (s StreamKey) XAddToEnd(ctx context.Context, values ...string) (string, error) {
	if len(values) < 2 || len(values)%2 != 0 {
		return "", ErrBadRequest
	}

	v := make(map[string]interface{}, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		v[values[index]] = values[index+1]
	}

	return s.XAdd(ctx, "*", 0, 0, v)
}

func (s StreamKey) XAck(ctx context.Context, group string, ids ...string) (int64, error) {
	start := time.Now()
	reply, err := MustGetRedis(ctx).XAck(ctx, s.key, group, ids...).Result()
	if err != nil {
		log.Warn(ctx, "xack message failed",
			log.Err(err),
			log.String("key", s.key),
			log.String("group", group),
			log.Strings("ids", ids),
			log.Duration("duration", time.Since(start)))
		return 0, err
	}

	log.Debug(ctx, "xack message successfully",
		log.String("key", s.key),
		log.String("group", group),
		log.Strings("ids", ids),
		log.Int64("reply", reply),
		log.Duration("duration", time.Since(start)))

	return reply, nil
}

func (s StreamKey) XGroupCreate(ctx context.Context, name, pos string) error {
	start := time.Now()
	_, err := MustGetRedis(ctx).XGroupCreateMkStream(ctx, s.key, name, pos).Result()
	if err != nil {
		log.Warn(ctx, "xgroup create group failed",
			log.Err(err),
			log.String("key", s.key),
			log.String("group", name),
			log.String("pos", pos),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "xgroup create group successfully",
		log.String("key", s.key),
		log.String("group", name),
		log.String("pos", pos),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (s StreamKey) XGroupCreateFromBegining(ctx context.Context, name string) error {
	return s.XGroupCreate(ctx, name, "0")
}

func (s StreamKey) XGroupCreateFromEnd(ctx context.Context, name string) error {
	return s.XGroupCreate(ctx, name, "$")
}

func (s StreamKey) XGroupRead(ctx context.Context, group, consumer string, count int64, block time.Duration, noAck bool) ([]redis.XMessage, error) {
	start := time.Now()
	arg := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{s.key, ">"},
		Count:    count,
		Block:    block,
		NoAck:    noAck,
	}

	streams, err := MustGetRedis(ctx).XReadGroup(ctx, arg).Result()
	if err == redis.Nil {
		log.Debug(ctx, "xgroupread no message found",
			log.Any("arg", arg),
			log.Duration("duration", time.Since(start)))
		return nil, ErrRecordNotFound
	}

	if err != nil {
		log.Warn(ctx, "xgroupread failed",
			log.Err(err),
			log.Any("arg", arg),
			log.Duration("duration", time.Since(start)))
		return nil, err
	}

	if len(streams) == 0 || streams[0].Stream != s.key {
		log.Warn(ctx, "invalid streams",
			log.Err(err),
			log.Any("arg", arg),
			log.Any("streams", streams),
			log.Duration("duration", time.Since(start)))
		return nil, ErrInvalidResultCount
	}

	log.Debug(ctx, "xgroupread successfully",
		log.Any("arg", arg),
		log.Any("messages", streams[0].Messages),
		log.Duration("duration", time.Since(start)))

	return streams[0].Messages, nil
}
