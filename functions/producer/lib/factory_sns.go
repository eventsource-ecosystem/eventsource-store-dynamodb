package lib

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/eventsource-ecosystem/eventsource"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/awstag"
)

func NewSNSFactory(api snsiface.SNSAPI) ProducerFactory {
	return func(ctx context.Context, tableArn string, tags []*dynamodb.Tag) ([]Producer, error) {
		value, ok := findTagValue(tags, awstag.SNS)
		if !ok {
			return nil, nil
		}

		var producers []Producer
		for _, name := range strings.Split(value, awstag.Separator) {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}

			log.Printf("creating topic if not exists, %v, for table, %v\n", name, tableArn)
			input := &sns.CreateTopicInput{Name: aws.String(name)}
			output, err := api.CreateTopicWithContext(ctx, input)
			if err != nil {
				return nil, fmt.Errorf("unable to create topic, %v - %v", name, err)
			}

			producers = append(producers, makeProducerSNS(api, output.TopicArn))
		}

		return producers, nil
	}
}

func makeProducerSNS(api snsiface.SNSAPI, topicArn *string) Producer {
	return func(ctx context.Context, aggregateID string, records []eventsource.Record) error {
		for _, record := range records {
			message := base64.StdEncoding.EncodeToString(record.Data)
			input := sns.PublishInput{
				Message:  aws.String(message),
				TopicArn: topicArn,
			}

			if _, err := api.PublishWithContext(ctx, &input); err != nil {
				return fmt.Errorf("unable to publish message to topic, %v", *topicArn)
			}
		}

		return nil
	}
}
