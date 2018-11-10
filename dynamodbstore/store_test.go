package dynamodbstore

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/eventsource-ecosystem/eventsource"
)

func TestStore_ImplementsStore(t *testing.T) {
	v, err := New("blah")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var _ eventsource.Store = v
}

func TestStore_SaveEmpty(t *testing.T) {
	s, err := New("blah")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = s.Save(context.Background(), "abc")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
}

func TestStore_SaveAndFetch(t *testing.T) {
	t.Parallel()

	api := dynamodbOrSkip(t)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := New(tableName,
			WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		err = store.Save(ctx, aggregateID, history...)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		found, err := store.Load(ctx, aggregateID, 0, 0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := found, history; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := len(found), len(history); got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestStore_SaveAndLoadFromVersion(t *testing.T) {
	t.Parallel()

	api := dynamodbOrSkip(t)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := New(tableName,
			WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		err = store.Save(ctx, aggregateID, history...)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		found, err := store.Load(ctx, aggregateID, 2, 0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := found, history[1:]; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := len(found), len(history)-1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestStore_SaveIdempotent(t *testing.T) {
	t.Parallel()

	api := dynamodbOrSkip(t)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := New(tableName,
			WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		// initial save
		err = store.Save(ctx, aggregateID, history...)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		// When - save it again
		err = store.Save(ctx, aggregateID, history...)
		// Then - verify no errors e.g. idempotent
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		found, err := store.Load(ctx, aggregateID, 0, 0)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := found, history; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestStore_SaveOptimisticLock(t *testing.T) {
	t.Parallel()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
		return
	}

	s := session.Must(session.NewSession(aws.NewConfig().
		WithRegion("blah").
		WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
		WithEndpoint(endpoint)))
	api := dynamodb.New(s)

	TempTable(t, api, func(tableName string) {
		ctx := context.Background()
		store, err := New(tableName,
			WithDynamoDB(api),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		aggregateID := "abc"
		initial := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
		}
		// initial save
		err = store.Save(ctx, aggregateID, initial...)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		overlap := eventsource.History{
			{
				Version: 2,
				Data:    []byte("c"),
			},
			{
				Version: 3,
				Data:    []byte("d"),
			},
		}
		// save overlapping events; should not be allowed
		err = store.Save(ctx, aggregateID, overlap...)
		if err == nil {
			t.Fatalf("got nil; want not nil")
		}
	})
}

func TestStore_LoadPartition(t *testing.T) {
	t.Parallel()

	api := dynamodbOrSkip(t)

	TempTable(t, api, func(tableName string) {
		store, err := New(tableName,
			WithDynamoDB(api),
			WithEventPerItem(2),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		aggregateID := "abc"
		history := eventsource.History{
			{
				Version: 1,
				Data:    []byte("a"),
			},
			{
				Version: 2,
				Data:    []byte("b"),
			},
			{
				Version: 3,
				Data:    []byte("c"),
			},
		}
		ctx := context.Background()
		err = store.Save(ctx, aggregateID, history...)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		found, err := store.Load(ctx, aggregateID, 0, 1)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := len(found), 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := found, history[0:1]; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
