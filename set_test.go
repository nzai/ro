package ro

import (
	"context"
	"testing"
)

func TestSetKey_All(t *testing.T) {
	ctx := context.Background()

	key := NewSetKey("test2")
	defer key.Del(ctx)

	member := "aaa"
	err := key.SAdd(ctx, member)
	if err != nil {
		t.Errorf("sadd failed due to %v", err)
	}

	found, err := key.SIsMember(ctx, member)
	if err != nil {
		t.Errorf("sismember failed due to %v", err)
	}

	if !found {
		t.Errorf("memeber %s should be exists", member)
	}

	err = key.SRem(ctx, member)
	if err != nil {
		t.Errorf("srem failed due to %v", err)
	}

	found, err = key.SIsMember(ctx, member)
	if err != nil {
		t.Errorf("sismember failed due to %v", err)
	}

	if found {
		t.Errorf("memeber %s should not be exists", member)
	}
}
