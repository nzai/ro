package ro

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nzai/log"
)

var (
	setKeyPool = sync.Pool{
		New: func() interface{} {
			return &SetKey{}
		},
	}
	setParameterKeyPool = sync.Pool{
		New: func() interface{} {
			return &SetParameterKey{}
		},
	}
)

type SetKey struct {
	*Key
}

func NewSetKey(key string) *SetKey {
	k := setKeyPool.Get().(*SetKey)
	k.Key = NewKey(key)
	return k
}

func (k SetKey) SAdd(ctx context.Context, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	start := time.Now()
	parameters := make([]interface{}, len(members))
	for index, member := range members {
		parameters[index] = member
	}

	err := MustGetRedis(ctx).SAdd(ctx, k.key, parameters...).Err()
	if err != nil {
		log.Warn(ctx, "set members failed",
			log.Err(err),
			log.String("key", k.key),
			log.Any("members", parameters),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "set member successfully",
		log.String("key", k.key),
		log.Any("members", parameters),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (k SetKey) SRem(ctx context.Context, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	start := time.Now()
	parameters := make([]interface{}, len(members))
	for index, member := range members {
		parameters[index] = member
	}

	err := MustGetRedis(ctx).SRem(ctx, k.key, parameters...).Err()
	if err != nil {
		log.Warn(ctx, "del members failed",
			log.Err(err),
			log.String("key", k.key),
			log.Any("members", parameters),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "del member successfully",
		log.String("key", k.key),
		log.Any("members", parameters),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (k SetKey) SIsMember(ctx context.Context, member string) (bool, error) {
	start := time.Now()
	isMember, err := MustGetRedis(ctx).SIsMember(ctx, k.key, member).Result()
	if err != nil {
		log.Warn(ctx, "check ismember failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return false, err
	}

	log.Debug(ctx, "check ismember successfully",
		log.String("key", k.key),
		log.Bool("isMember", isMember),
		log.Duration("duration", time.Since(start)))

	return isMember, nil
}

func (k SetKey) SMembers(ctx context.Context) ([]string, error) {
	start := time.Now()
	members, err := MustGetRedis(ctx).SMembers(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "get members failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return nil, err
	}

	log.Debug(ctx, "get members successfully",
		log.String("key", k.key),
		log.Any("members", members),
		log.Duration("duration", time.Since(start)))

	return members, nil
}

func (k SetKey) SMembersMap(ctx context.Context) (map[string]struct{}, error) {
	start := time.Now()
	members, err := MustGetRedis(ctx).SMembersMap(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "get members map failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return nil, err
	}

	log.Debug(ctx, "get member map successfully",
		log.String("key", k.key),
		log.Any("members", members),
		log.Duration("duration", time.Since(start)))

	return members, nil
}

func (k SetKey) SCard(ctx context.Context) (int64, error) {
	start := time.Now()
	count, err := MustGetRedis(ctx).SCard(ctx, k.key).Result()
	if err != nil {
		log.Warn(ctx, "get members count failed",
			log.Err(err),
			log.String("key", k.key),
			log.Duration("duration", time.Since(start)))
		return 0, err
	}

	log.Debug(ctx, "get members count successfully",
		log.String("key", k.key),
		log.Int64("count", count),
		log.Duration("duration", time.Since(start)))

	return count, nil
}

type SetParameterKey struct {
	pattern string
}

func NewSetParameterKey(pattern string) *SetParameterKey {
	k := setParameterKeyPool.Get().(*SetParameterKey)
	k.pattern = pattern
	return k
}

func (k SetParameterKey) Param(parameters ...interface{}) *SetKey {
	return NewSetKey(fmt.Sprintf(k.pattern, parameters...))
}
