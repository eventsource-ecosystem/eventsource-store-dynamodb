package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/dynamodbstore"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/functions/producer/lib"
)

var reStreamSuffix = regexp.MustCompile(`/stream/[^/]+$`)

type Handler struct {
	dynamodb        dynamodbiface.DynamoDBAPI
	firehoseRoleARN string

	mutex     sync.Mutex
	producers map[string]lib.Producer
}

func (h *Handler) handleRecord(ctx context.Context, record events.DynamoDBEventRecord) error {
	tableArn := reStreamSuffix.ReplaceAllString(record.EventSourceArn, "")

	attr, ok := record.Change.Keys[dynamodbstore.HashKey]
	if !ok {
		return fmt.Errorf("unable to determine hash key for table, %v", tableArn)
	}
	aggregateID := attr.String()

	records, err := dynamodbstore.Changes(record.Change)
	if err != nil {
		return err
	}

	h.mutex.Lock()
	fn, ok := h.producers[tableArn]
	h.mutex.Unlock()

	if !ok {
		v, err := lib.MakeProducer(ctx, h.dynamodb, tableArn)
		if err != nil {
			return err
		}
		fn = v
	}

	if err := fn(ctx, aggregateID, records); err != nil {
		return err
	}

	return nil
}

func (h *Handler) Handle(ctx context.Context, event events.DynamoDBEvent) error {
	for _, record := range event.Records {
		if err := h.handleRecord(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	firehoseRoleARN := os.Getenv("FIREHOSE_ROLE_ARN")
	if firehoseRoleARN == "" {
		log.Fatalln("FIREHOSE_ROLE_ARN env variable not set")
	}

	kmsARN := os.Getenv("KEY_ARN")
	if kmsARN == "" {
		log.Fatalln("KEY_ARN env variable not set")
	}

	s := session.Must(session.NewSession(aws.NewConfig()))
	h := &Handler{
		dynamodb:        dynamodb.New(s),
		firehoseRoleARN: firehoseRoleARN,
		producers:       map[string]lib.Producer{},
	}

	lib.Register(lib.NewFirehoseFactory(firehose.New(s), firehoseRoleARN, kmsARN))
	lib.Register(lib.NewSNSFactory(sns.New(s)))
	lib.Register(lib.NewSQSFactory(sqs.New(s)))

	lambda.Start(h.Handle)
}
