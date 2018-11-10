package dynamodbstore

import (
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestMakeCreateTableInput(t *testing.T) {
	api := dynamodbOrSkip(t)

	t.Run("default", func(t *testing.T) {
		tableName := "default-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		input := MakeCreateTableInput(tableName, 20, 30)

		_, err := api.CreateTable(input)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		_, err = api.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("kitchen-sink", func(t *testing.T) {
		rcap := int64(35)
		wcap := int64(25)

		tableName := "kitchen-sink-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		input := MakeCreateTableInput(tableName, rcap, wcap,
			WithRegion("us-west-2"),
			WithEventPerItem(200),
			WithDynamoDB(api),
			WithDebug(ioutil.Discard),
		)

		_, err := api.CreateTable(input)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		out, err := api.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := *out.Table.ProvisionedThroughput.ReadCapacityUnits, rcap; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
		if got, want := *out.Table.ProvisionedThroughput.WriteCapacityUnits, wcap; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}

		_, err = api.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}
