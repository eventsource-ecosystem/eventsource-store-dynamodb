package dynamodbstore

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/eventsource-ecosystem/eventsource-store-dynamodb/awstag"
)

const (
	defaultReadCapacity  = 3
	defaultWriteCapacity = 3
)

type infraOptions struct {
	hashKey                    string
	rangeKey                   string
	billingMode                string
	readCapacity               int64
	writeCapacity              int64
	pointInTimeRecoveryEnabled bool
	isLocal                    bool
	sns                        struct {
		topicNames []string
	}
	sqs struct {
		queueNames []string
	}
	firehose struct {
		enabled    bool
		bucket     string
		streamName string
	}
}

type InfraOption func(i *infraOptions)

// WithProvisionedThroughput indicates provisioned throughput should be used.  By default,
// eventsource tables use PAY_PER_REQUEST
func WithProvisionedThroughput(enabled bool, readCapacity, writeCapacity int64) InfraOption {
	return func(i *infraOptions) {
		if enabled {
			i.billingMode = dynamodb.BillingModeProvisioned
		} else {
			i.billingMode = dynamodb.BillingModePayPerRequest
		}

		i.readCapacity = readCapacity
		i.writeCapacity = writeCapacity
	}
}

// WithLocalDynamoDB provides compatibility with the amazon/dynamodb-local docker image which
// (as of this writing) is not api compatible with the dynamodb service.
func WithLocalDynamoDB(isLocal bool) InfraOption {
	return func(i *infraOptions) {
		i.isLocal = isLocal
	}
}

// WithFirehose indicates kinesis firehose should be used to persist events to s3
func WithFirehose(enabled bool, streamName, bucket string) InfraOption {
	return func(i *infraOptions) {
		i.firehose.enabled = enabled
		i.firehose.bucket = bucket
		i.firehose.streamName = streamName
	}
}

func WithPointInTimeRecovery(enabled bool) InfraOption {
	return func(i *infraOptions) {
		i.pointInTimeRecoveryEnabled = true
	}
}

func WithSNS(topicNames ...string) InfraOption {
	return func(i *infraOptions) {
		i.sns.topicNames = append(i.sns.topicNames, topicNames...)
	}
}

func WithSQS(queueNames ...string) InfraOption {
	return func(i *infraOptions) {
		i.sqs.queueNames = append(i.sqs.queueNames, queueNames...)
	}
}

func makeInfraOptions(opts ...InfraOption) infraOptions {
	options := infraOptions{
		hashKey:       HashKey,
		rangeKey:      RangeKey,
		billingMode:   dynamodb.BillingModePayPerRequest,
		readCapacity:  defaultReadCapacity,
		writeCapacity: defaultWriteCapacity,
	}

	for _, opt := range opts {
		opt(&options)
	}

	return options
}

// MakeCreateTableInput is a utility tool to write the default table definition for creating the aws tables
func MakeCreateTableInput(tableName string, opts ...InfraOption) *dynamodb.CreateTableInput {
	options := makeInfraOptions(opts...)

	input := &dynamodb.CreateTableInput{
		BillingMode: aws.String(options.billingMode),
		TableName:   aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(options.hashKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String(options.rangeKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeN),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(options.hashKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String(options.rangeKey),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
		},
	}

	if options.isLocal {
		options.billingMode = dynamodb.BillingModeProvisioned
		options.readCapacity = 5
		options.writeCapacity = 5
	}

	if options.billingMode == dynamodb.BillingModeProvisioned {
		input.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(options.readCapacity),
			WriteCapacityUnits: aws.Int64(options.writeCapacity),
		}
	}

	return input
}

func createTagsIfNotPresent(ctx context.Context, api dynamodbiface.DynamoDBAPI, tableArn *string, opts ...InfraOption) error {
	var currentTags = map[string]struct{}{}
	var token *string
	for {
		output, err := api.ListTagsOfResourceWithContext(ctx, &dynamodb.ListTagsOfResourceInput{
			ResourceArn: tableArn,
		})
		if err != nil {
			return fmt.Errorf("unable to list tags for resource, %v - %v", *tableArn, err)
		}

		for _, tag := range output.Tags {
			currentTags[*tag.Key] = struct{}{}
		}

		token = output.NextToken
		if token == nil {
			break
		}
	}

	var options = makeInfraOptions(opts...)

	// amazon/dynamodb-local does not support tagging
	if !options.isLocal {
		var tags []*dynamodb.Tag

		if _, ok := currentTags[awstag.Core]; !ok {
			tags = append(tags, &dynamodb.Tag{
				Key:   aws.String(awstag.Core),
				Value: aws.String(""),
			})
		}
		if _, ok := currentTags[awstag.Firehose]; !ok && options.firehose.enabled {
			tags = append(tags, &dynamodb.Tag{
				Key:   aws.String(awstag.Firehose),
				Value: aws.String(options.firehose.streamName + awstag.Separator + options.firehose.bucket),
			})
		}
		if _, ok := currentTags[awstag.SNS]; !ok && len(options.sns.topicNames) > 0 {
			tags = append(tags, &dynamodb.Tag{
				Key:   aws.String(awstag.SNS),
				Value: aws.String(strings.Join(options.sns.topicNames, awstag.Separator)),
			})
		}
		if _, ok := currentTags[awstag.SQS]; !ok && len(options.sqs.queueNames) > 0 {
			tags = append(tags, &dynamodb.Tag{
				Key:   aws.String(awstag.SQS),
				Value: aws.String(strings.Join(options.sqs.queueNames, awstag.Separator)),
			})
		}

		if len(tags) > 0 {
			input := dynamodb.TagResourceInput{
				ResourceArn: tableArn,
				Tags:        tags,
			}
			if _, err := api.TagResourceWithContext(ctx, &input); err != nil {
				return fmt.Errorf("unable to tag resource, %v - %v", *tableArn, err)
			}
		}
	}

	return nil
}

func CreateTableIfNotExists(ctx context.Context, api dynamodbiface.DynamoDBAPI, tableName string, opts ...InfraOption) error {
	var input = MakeCreateTableInput(tableName, opts...)

	// create the table
	//
	var tableArn *string
	if output, err := api.CreateTableWithContext(ctx, input); err == nil {
		tableArn = output.TableDescription.TableArn

	} else if v, ok := err.(awserr.Error); !ok || v.Code() != dynamodb.ErrCodeResourceInUseException {
		return fmt.Errorf("unable to create table, %v - %v", tableName, err)

	} else {
		describeTableInput := &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}
		output, err := api.DescribeTableWithContext(ctx, describeTableInput)
		if err != nil {
			return fmt.Errorf("dynamodb.DescribeTable failed with %v", err)
		}
		tableArn = output.Table.TableArn
	}

	// wait for the table to exist
	//
	describeTableInput := &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}
	if err := api.WaitUntilTableExistsWithContext(ctx, describeTableInput); err != nil {
		return fmt.Errorf("error while waiting for table, %v, to be created - %v", tableName, err)
	}

	// tag the table
	//
	if err := createTagsIfNotPresent(ctx, api, tableArn, opts...); err != nil {
		return err
	}

	// updated point in time recovery settings
	//
	if err := updateBackups(ctx, api, tableName, opts...); err != nil {
		return err
	}

	return nil
}

func updateBackups(ctx context.Context, api dynamodbiface.DynamoDBAPI, tableName string, opts ...InfraOption) error {
	options := makeInfraOptions(opts...)
	if !options.pointInTimeRecoveryEnabled {
		return nil
	}

	if !options.pointInTimeRecoveryEnabled {
		return nil
	}

	output, err := api.DescribeContinuousBackupsWithContext(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String(tableName)})
	if err != nil {
		return fmt.Errorf("unable to determine continuous backup status for table, %v - %v", tableName, err)
	}

	status := output.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus
	if *status == dynamodb.ContinuousBackupsStatusEnabled {
		return nil
	}

	input := dynamodb.UpdateContinuousBackupsInput{
		PointInTimeRecoverySpecification: &dynamodb.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
		TableName: aws.String(tableName),
	}

	for {
		if _, err := api.UpdateContinuousBackupsWithContext(ctx, &input); err != nil {
			if v, ok := err.(awserr.Error); ok && v.Code() == dynamodb.ErrCodeContinuousBackupsUnavailableException {
				log.Println("waiting for continuous backup to be available ...")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
				continue
			}

			return fmt.Errorf("unable to enable point in time recovery for table, %v - %v", tableName, err)
		}

		log.Println("enabling point in time recovery for table,", tableName)
		return nil
	}
}
