package lib

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/eventsource-ecosystem/eventsource"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/awstag"
)

func randomID() string {
	data := [16]byte{}
	_, err := rand.Read(data[:])
	if err != nil {
		log.Fatalln(err)
	}

	return hex.EncodeToString(data[:])
}

func NewSQSFactory(api sqsiface.SQSAPI) ProducerFactory {
	return func(ctx context.Context, tableArn string, tags []*dynamodb.Tag) ([]Producer, error) {
		value, ok := findTagValue(tags, awstag.SQS)
		if !ok {
			return nil, nil
		}

		var producers []Producer
		for _, name := range strings.Split(value, awstag.Separator) {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}

			log.Printf("creating queue if not exists, %v, for table, %v\n", name, tableArn)
			input := &sqs.CreateQueueInput{QueueName: aws.String(name)}
			output, err := api.CreateQueueWithContext(ctx, input)
			if err != nil {
				return nil, fmt.Errorf("unable to create topic, %v - %v", name, err)
			}

			producers = append(producers, makeProducerSQS(api, output.QueueUrl))
		}

		return producers, nil
	}
}

func makeProducerSQS(api sqsiface.SQSAPI, queueUrl *string) Producer {
	return func(ctx context.Context, aggregateID string, records []eventsource.Record) error {
		for n := len(records); n > 0; n = len(records) {
			if n > 10 {
				n = 10
			}

			input := sqs.SendMessageBatchInput{QueueUrl: queueUrl}
			for i := 0; i < n; i++ {
				var (
					record  = records[i]
					message = base64.StdEncoding.EncodeToString(record.Data)
					isFifo  = strings.HasSuffix(*queueUrl, ".fifo")
					entry   = sqs.SendMessageBatchRequestEntry{
						Id:          aws.String(strconv.Itoa(i + 1)),
						MessageBody: aws.String(message),
					}
				)

				if isFifo {
					entry.MessageDeduplicationId = aws.String(randomID())
					entry.MessageGroupId = aws.String(aggregateID)
				}

				input.Entries = append(input.Entries, &entry)
			}

			if _, err := api.SendMessageBatchWithContext(ctx, &input); err != nil {
				return fmt.Errorf("unable to send messages to %v - %v", *queueUrl, err)
			}

			records = records[n:]
		}

		return nil
	}
}
