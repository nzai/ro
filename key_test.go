package ro

import (
	"context"
	"testing"
)

func TestKey_Exists(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test:abc")
	err := key.Set(ctx, "a", 0)
	if err != nil {
		t.Errorf("set string value failed due to %v", err)
	}

	exists, err := key.Exists(ctx)
	if err != nil {
		t.Errorf("check key exists failed due to %v", err)
	}

	if !exists {
		t.Errorf("check key exists failed, want true, get %v", exists)
	}

	err = key.Del(ctx)
	if err != nil {
		t.Errorf("delete key failed due to %v", err)
	}

	exists, err = key.Exists(ctx)
	if err != nil {
		t.Errorf("check key exists failed due to %v", err)
	}

	if exists {
		t.Errorf("check key exists failed, want false, get %v", exists)
	}
}

func TestKey_Del(t *testing.T) {
	ctx := context.Background()

	key := NewStringKey("test:dce")
	err := key.Set(ctx, "a", 0)
	if err != nil {
		t.Errorf("set string value failed due to %v", err)
	}

	type fields struct {
		key string
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := Key{
				key: tt.fields.key,
			}
			if err := k.Del(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Key.Del() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
