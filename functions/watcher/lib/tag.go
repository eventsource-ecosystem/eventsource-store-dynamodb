package lib

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/awstag"
)

func HandleTag(ctx context.Context, dynamodbAPI dynamodbiface.DynamoDBAPI, lambdaAPI lambdaiface.LambdaAPI, req TagRequest, functionName string) error {
	for _, tag := range req.Tags {
		if tag.Key != awstag.Core {
			continue
		}

		tableName := extractTableName(req.ResourceArn)
		streamARN, err := lookupStreamARN(ctx, dynamodbAPI, tableName)
		if err != nil {
			return err
		}

		if streamARN == nil {
			log.Println("dynamodb streams not enabled for table,", tableName)
			return nil
		}

		if err := attachLambda(ctx, lambdaAPI, streamARN, functionName); err != nil {
			return err
		}
	}

	return nil
}

func extractTableName(resourceArn string) string {
	segments := strings.Split(resourceArn, "/")
	return segments[len(segments)-1]
}

func lookupStreamARN(ctx context.Context, api dynamodbiface.DynamoDBAPI, tableName string) (*string, error) {
	output, err := api.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	if err != nil {
		return nil, fmt.Errorf("unable to describe table, %v - %v", tableName, err)
	}

	return output.Table.LatestStreamArn, nil
}

func attachLambda(ctx context.Context, lambdaAPI lambdaiface.LambdaAPI, streamARN *string, functionName string) error {
	input := &lambda.CreateEventSourceMappingInput{
		EventSourceArn:   streamARN,
		FunctionName:     aws.String(functionName),
		StartingPosition: aws.String("LATEST"),
	}
	if _, err := lambdaAPI.CreateEventSourceMappingWithContext(ctx, input); err != nil {
		if v, ok := err.(awserr.Error); ok {
			switch v.Code() {
			case lambda.ErrCodeResourceConflictException:
				return nil
			}
		}

		return err
	}

	return nil
}
