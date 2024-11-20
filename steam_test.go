package ro

import (
	"context"
	"testing"
	"time"
)

func TestStreamKey_XAddToEnd(t *testing.T) {
	ctx := context.Background()

	key := NewStreamKey("stream1")
	defer key.Del(ctx)

	_, err := key.XAddToEnd(ctx, "f1", "1", "f2", "2")
	if err != nil {
		t.Errorf("xadd failed due to %v", err)
	}
}

func TestStreamKey_XGroupRead(t *testing.T) {
	ctx := context.Background()

	key := NewStreamKey("stream2")
	defer key.Del(ctx)

	err := key.XGroupCreateFromEnd(ctx, "group1")
	if err != nil {
		t.Errorf("failed to create group due to %v", err)
	}

	go func() {
		messages, err := key.XGroupRead(ctx, "group1", "consumer1", 1, time.Second, false)
		if err != nil {
			t.Errorf("failed to read from group due to %v", err)
		}

		if len(messages) != 2 {
			t.Errorf("failed to read from group due to %v", err)
		}
	}()

	_, err = key.XAddToEnd(ctx, "fff", "1", "b", "2")
	if err != nil {
		t.Fatalf("failed to create group due to %v", err)
	}
}
