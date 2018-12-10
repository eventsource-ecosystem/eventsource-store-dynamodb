package lib

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestJsonDecode(t *testing.T) {
	testCases := map[string]struct {
		Input          string
		HasResourceArn bool
		Tags           []Tag
		TagKeys        []string
	}{
		"CreateTable": {
			Input: "testdata/CreateTable.json",
		},
		"TagResource": {
			Input:          "testdata/TagResource.json",
			HasResourceArn: true,
			Tags: []Tag{
				{
					Key: "eventsource",
				},
			},
		},
		"UntagResource": {
			Input:          "testdata/UntagResource.json",
			HasResourceArn: true,
			TagKeys: []string{
				"glip",
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			data, err := ioutil.ReadFile(tc.Input)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			var wrapper events.CloudWatchEvent
			err = json.Unmarshal(data, &wrapper)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			//fmt.Println(string(wrapper.Detail))

			var event DynamoDBEvent
			err = json.Unmarshal(wrapper.Detail, &event)
			if err != nil {
				t.Fatalf("got %v; want nil", err)
			}

			if event.EventSource == "" {
				t.Fatalf("got blank string; want not nil")
			}
			if event.EventTime == "" {
				t.Fatalf("got blank string; want not nil")
			}
			if event.EventName == "" {
				t.Fatalf("got blank string; want not nil")
			}
			if tc.HasResourceArn {
				if event.RequestParameters.ResourceArn == "" {
					t.Fatalf("got blank string; want not nil")
				}
			}

			if got, want := event.RequestParameters.Tags, tc.Tags; !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v; want %v", got, want)
			}
		})
	}
}
