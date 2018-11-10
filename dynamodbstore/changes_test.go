package dynamodbstore

import (
	"reflect"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/eventsource-ecosystem/eventsource"
)

func TestRawEvents(t *testing.T) {

	testCases := map[string]struct {
		Record   events.DynamoDBStreamRecord
		Expected []eventsource.Record
	}{
		"simple": {
			Record: events.DynamoDBStreamRecord{
				NewImage: map[string]events.DynamoDBAttributeValue{
					"_1": events.NewBinaryAttribute([]byte("a")),
					"_2": events.NewBinaryAttribute([]byte("b")),
					"_3": events.NewBinaryAttribute([]byte("c")),
				},
				OldImage: map[string]events.DynamoDBAttributeValue{
					"_1": events.NewBinaryAttribute([]byte("a")),
				},
			},
			Expected: []eventsource.Record{
				{
					Version: 2,
					Data:    []byte("b"),
				},
				{
					Version: 3,
					Data:    []byte("c"),
				},
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			records, err := Changes(tc.Record)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}
			if got, want := len(records), 2; got != want {
				t.Fatalf("got %v; want nil", err)
			}
			if got, want := records, tc.Expected; !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	}
}

func TestTableName(t *testing.T) {
	tableName, err := TableName("arn:aws:dynamodb:us-west-2:528688496454:table/table-local-orgs/stream/2017-03-14T04:49:34.930")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	if got, want := tableName, "table-local-orgs"; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}

	_, err = TableName("bogus")
	if err == nil {
		t.Fatalf("got nil; want not nil")
	}
}
