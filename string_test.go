package ro

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStringKey_SetAndGet(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test1")
	defer key.Del(ctx)

	value := "111"
	expiration := time.Millisecond * 50

	err := key.Set(ctx, value, expiration)
	if err != nil {
		t.Errorf("set string value failed due to %v", err)
	}

	get, err := key.Get(ctx)
	if err != nil {
		t.Errorf("get string value failed due to %v", err)
	}

	if get != value {
		t.Errorf("get string value not equal, want %s, get %s", value, get)
	}

	time.Sleep(expiration * time.Duration(2))

	_, err = key.Get(ctx)
	if err != redis.Nil {
		t.Errorf("string value expire failed")
	}
}

func TestStringKey_SetAndGetInt(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test2")
	defer key.Del(ctx)

	value := 222

	err := key.SetInt(ctx, value, 0)
	if err != nil {
		t.Errorf("set int value failed due to %v", err)
	}

	get, err := key.GetInt(ctx)
	if err != nil {
		t.Errorf("get int value failed due to %v", err)
	}

	if get != value {
		t.Errorf("get int value not equal, want %d, get %d", value, get)
	}
}

func TestStringKey_SetAndGetInt64(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test3")
	defer key.Del(ctx)

	value := int64(333)

	err := key.SetInt64(ctx, value, 0)
	if err != nil {
		t.Errorf("set int64 value failed due to %v", err)
	}

	get, err := key.GetInt64(ctx)
	if err != nil {
		t.Errorf("get int64 value failed due to %v", err)
	}

	if get != value {
		t.Errorf("get int64 value not equal, want %d, get %d", value, get)
	}
}

type testStruct struct {
	AAA string
	BBB int
}

func TestStringKey_SetAndGetObject(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test4")
	defer key.Del(ctx)

	obj := &testStruct{
		AAA: "aaa",
		BBB: 444,
	}

	err := key.SetObject(ctx, obj, 0)
	if err != nil {
		t.Errorf("set object value failed due to %v", err)
	}

	get := &testStruct{}
	err = key.GetObject(ctx, get)
	if err != nil {
		t.Errorf("get object value failed due to %v", err)
	}

	if !reflect.DeepEqual(get, obj) {
		t.Errorf("get object value not equal, want %v, get %v", obj, get)
	}

	key1 := NewStringKey("test41")
	err = key1.GetObject(ctx, get)
	if err != nil && err != redis.Nil {
		t.Errorf("get object value failed due to %v", err)
	}
}

func TestStringKey_GetLocker(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test5")
	var err error
	var total int64

	N := 1000
	running := make(chan struct{}, N)
	defer close(running)

	done := make(chan struct{}, N)
	defer close(done)

	cond := sync.NewCond(&sync.Mutex{})

	expiration := time.Millisecond * 500
	for index := 0; index < N; index++ {
		go func() {
			running <- struct{}{}
			defer func() {
				done <- struct{}{}
			}()

			cond.L.Lock()
			cond.Wait()
			defer cond.L.Unlock()

			err = key.GetLocker(ctx, expiration, func(ctx context.Context) error {
				atomic.AddInt64(&total, 1)
				return nil
			})
			if err != nil {
				t.Errorf("get locker failed due to %v", err)
			}
		}()
	}

	for index := 0; index < N; index++ {
		<-running
	}

	cond.L.Lock()
	cond.Broadcast()
	cond.L.Unlock()

	for index := 0; index < N; index++ {
		<-done
	}

	if total != 1 {
		t.Errorf("get unexpect result, want 1, get %d", total)
	}
}
