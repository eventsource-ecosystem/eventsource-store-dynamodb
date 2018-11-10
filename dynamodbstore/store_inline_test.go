package dynamodbstore

import (
	"context"
	"testing"
)

func TestStore_CheckIdempotent(t *testing.T) {
	s := &Store{}
	err := s.checkIdempotent(context.Background(), "abc")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
}
