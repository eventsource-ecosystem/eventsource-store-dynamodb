package lib

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type ProducerFactory func(ctx context.Context, tableArn string, tags []*dynamodb.Tag) ([]Producer, error)

type Factory struct {
}

var factories []ProducerFactory

func Register(factory ProducerFactory) {
	factories = append(factories, factory)
}

func makeProducerFromTags(ctx context.Context, tableArn string, tags []*dynamodb.Tag) (Producer, error) {
	var p ProducerSlice
	for _, factory := range factories {
		producer, err := factory(ctx, tableArn, tags)
		if err != nil {
			return nil, err
		}

		p = append(p, producer...)
	}

	return p.Produce, nil
}

func MakeProducer(ctx context.Context, api dynamodbiface.DynamoDBAPI, tableArn string) (Producer, error) {
	output, err := api.ListTagsOfResourceWithContext(ctx, &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableArn),
	})
	if err != nil {
		if v, ok := err.(awserr.Error); ok {
			return nil, fmt.Errorf("unable to list tags for table, %v - %v %v", tableArn, v.Code(), v.Message())
		}
		return nil, fmt.Errorf("unable to list tags for table, %v - %v", tableArn, err)
	}

	return makeProducerFromTags(ctx, tableArn, output.Tags)
}
