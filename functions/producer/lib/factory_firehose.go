package lib

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/eventsource-ecosystem/eventsource"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/awstag"
)

func NewFirehoseFactory(api firehoseiface.FirehoseAPI, roleARN, keyARN string) ProducerFactory {
	return func(ctx context.Context, tableArn string, tags []*dynamodb.Tag) ([]Producer, error) {
		value, ok := findTagValue(tags, awstag.Firehose)
		if !ok {
			return nil, nil
		}

		values := strings.Split(value, awstag.Separator)
		if len(values) != 2 {
			return nil, fmt.Errorf("invalid firehose delivery stream tag: expected {streamName}:{bucket}, got %v", value)
		}

		var (
			streamName, bucket = values[0], values[1]
			segments           = strings.Split(tableArn, "/")
			tableName          = segments[len(segments)-1]
		)

		return []Producer{
			makeFirehoseProducer(api, tableName, streamName, bucket, roleARN, keyARN),
		}, nil
	}
}

func waitForStreamActive(ctx context.Context, api firehoseiface.FirehoseAPI, streamName string) error {
	describeInput := firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String(streamName),
	}

	for {
		output, err := api.DescribeDeliveryStreamWithContext(ctx, &describeInput)
		if err != nil {
			return fmt.Errorf("unable to describe delivery stream, %v", streamName)
		}

		switch *output.DeliveryStreamDescription.DeliveryStreamStatus {
		case firehose.DeliveryStreamStatusActive:
			return nil

		default:
			log.Printf("waiting for delivery stream to become active, %v", streamName)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func createStreamIfNotExists(ctx context.Context, api firehoseiface.FirehoseAPI, tableName, streamName, bucket, roleARN, keyARN string) error {
	describeInput := firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String(streamName),
	}
	if _, err := api.DescribeDeliveryStreamWithContext(ctx, &describeInput); err == nil {
		log.Printf("found existing delivery stream, %v\n", streamName)
		return nil
	}

	if !strings.HasSuffix(tableName, "/") {
		tableName += "/"
	}

	log.Printf("creating delivery stream, %v, with bucket, %v\n", streamName, bucket)
	bucketARN := fmt.Sprintf("arn:aws:s3:::%v", bucket)
	createInput := firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String(streamName),
		DeliveryStreamType: aws.String(firehose.DeliveryStreamTypeDirectPut),
		ExtendedS3DestinationConfiguration: &firehose.ExtendedS3DestinationConfiguration{
			BucketARN:                aws.String(bucketARN),
			BufferingHints:           &firehose.BufferingHints{},
			CloudWatchLoggingOptions: &firehose.CloudWatchLoggingOptions{},
			EncryptionConfiguration: &firehose.EncryptionConfiguration{
				KMSEncryptionConfig: &firehose.KMSEncryptionConfig{
					AWSKMSKeyARN: aws.String(keyARN),
				},
			},
			Prefix:  aws.String(tableName),
			RoleARN: aws.String(roleARN),
		},
		Tags: []*firehose.Tag{
			{
				Key:   aws.String(awstag.Core),
				Value: aws.String(""),
			},
		},
	}
	if _, err := api.CreateDeliveryStreamWithContext(ctx, &createInput); err != nil {
		if v, ok := err.(awserr.Error); ok {
			return fmt.Errorf("unable to create delivery stream, %v - %v %v", streamName, v.Code(), v.Message())
		}
		return fmt.Errorf("unable to create delivery stream, %v - %v", streamName, err)
	}

	return nil
}

func makeFirehoseProducer(api firehoseiface.FirehoseAPI, tableName, streamName, bucket, roleARN, keyARN string) Producer {
	var (
		mutex      = &sync.Mutex{}
		allStreams = map[string]struct{}{}
	)

	return func(ctx context.Context, aggregateID string, records []eventsource.Record) error {
		mutex.Lock()
		_, ok := allStreams[streamName]
		mutex.Unlock()

		if !ok {
			if err := createStreamIfNotExists(ctx, api, tableName, streamName, bucket, roleARN, keyARN); err != nil {
				return err
			}

			if err := waitForStreamActive(ctx, api, streamName); err != nil {
				return err
			}

			mutex.Lock()
			allStreams[streamName] = struct{}{}
			mutex.Unlock()
		}

		for n := len(records); n > 0; n = len(records) {
			if n > 100 {
				n = 100
			}

			encoder := base64.StdEncoding
			input := firehose.PutRecordBatchInput{DeliveryStreamName: aws.String(streamName)}
			for i := 0; i < n; i++ {
				record := records[i]

				// base64 encode data with newline at the end
				data := make([]byte, encoder.EncodedLen(len(record.Data))+1)
				encoder.Encode(data, record.Data)
				data = append(data, '\n')

				input.Records = append(input.Records, &firehose.Record{
					Data: data,
				})
			}

			if _, err := api.PutRecordBatchWithContext(ctx, &input); err != nil {
				return fmt.Errorf("firehose.PutRecordBatch failed - %v", err)
			}

			records = records[n:]
		}

		return nil
	}
}
