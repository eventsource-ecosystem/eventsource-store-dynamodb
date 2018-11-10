package dynamodbstore

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func dynamodbOrSkip(t *testing.T) *dynamodb.DynamoDB {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.SkipNow()
	}

	s := session.Must(session.NewSession(aws.NewConfig().
		WithRegion("blah").
		WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")).
		WithEndpoint(endpoint)))
	return dynamodb.New(s)
}
