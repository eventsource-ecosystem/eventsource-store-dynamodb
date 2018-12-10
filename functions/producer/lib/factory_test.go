package lib

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/eventsource-ecosystem/eventsource"
)

func TestFactory(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.SkipNow()
	}

	roleARN := os.Getenv("ROLE_ARN")
	if roleARN == "" {
		t.SkipNow()
	}

	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-west-2"
	}

	ctx := context.Background()
	s := session.Must(session.NewSession(aws.NewConfig()))

	Register(NewFirehoseFactory(firehose.New(s), roleARN))
	Register(NewSNSFactory(sns.New(s)))
	Register(NewSQSFactory(sqs.New(s)))

	p, err := MakeProducer(ctx, dynamodb.New(s), "arn:aws:dynamodb:us-west-2:950816970505:table/aaa")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	records := []eventsource.Record{
		{
			Data: []byte("abc"),
		},
		{
			Data: []byte("def"),
		},
		{
			Data: []byte("ghi"),
		},
	}

	err = p(ctx, "abc", records)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
}
