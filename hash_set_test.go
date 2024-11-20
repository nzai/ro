package ro

import (
	"context"
	"reflect"
	"testing"
)

func TestHashSetKey_HMGet(t *testing.T) {
	ctx := context.Background()

	key := "hs111"
	k := NewHashSetKey(key)
	defer k.Del(ctx)

	err := k.HSet(ctx, "1", "1")
	if err != nil {
		t.Errorf("set hashset field value failed due to %v", err)
	}

	err = k.HSet(ctx, "2", "2")
	if err != nil {
		t.Errorf("set hashset field value failed due to %v", err)
	}

	type fields struct {
		Key Key
	}
	type args struct {
		ctx    context.Context
		fields []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name:   "1",
			fields: fields{Key: Key{key: key}},
			args: args{
				ctx:    ctx,
				fields: []string{"1", "2", "not exists"},
			},
			want: map[string]string{
				"1": "1",
				"2": "2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := k.HMGet(tt.args.ctx, tt.args.fields)
			if (err != nil) != tt.wantErr {
				t.Errorf("HashSetKey.HMGet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HashSetKey.HMGet() = %v, want %v", got, tt.want)
			}
		})
	}

}
