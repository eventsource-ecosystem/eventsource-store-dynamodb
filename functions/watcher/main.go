package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	awslambda "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/functions/watcher/lib"
)

type Handler struct {
	dynamodb     dynamodbiface.DynamoDBAPI
	lambda       lambdaiface.LambdaAPI
	functionName string
}

func (h Handler) Handle(ctx context.Context, raw events.CloudWatchEvent) (err error) {
	var event lib.DynamoDBEvent
	if err := json.Unmarshal(raw.Detail, &event); err != nil {
		return fmt.Errorf("unable to unmarshal CloudWatchEvent.Detail - %v", err)
	}

	if event.ErrorCode != "" {
		log.Printf("ignoring failed event, %v - %v %v\n", event.EventName, event.ErrorCode, event.ErrorMessage)
		return nil
	}

	switch event.EventName {
	case "TagResource":
		return lib.HandleTag(ctx, h.dynamodb, h.lambda, event.RequestParameters, h.functionName)
	case "UntagResource":
		return lib.HandleUntag(ctx, event.RequestParameters)
	default:
		log.Printf("ignoring dynamodb event, %v\n", event.EventName)
		return nil
	}
}

func main() {
	functionName := os.Getenv("FUNCTION_NAME")
	if functionName == "" {
		log.Fatalln("FUNCTION_NAME env variable not set")
	}
	log.Println("watcher configured with lambda function,", functionName)

	s := session.Must(session.NewSession(aws.NewConfig()))
	h := Handler{
		dynamodb:     dynamodb.New(s),
		lambda:       awslambda.New(s),
		functionName: functionName,
	}

	lambda.Start(h.Handle)
}
