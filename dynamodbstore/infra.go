package dynamodbstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	defaultReadCapacity  = 3
	defaultWriteCapacity = 3
	tagCore              = "eventsource"
	tagSNS               = "eventsource.sns"
	tagSQS               = "eventsource.sqs"
	tagFirehose          = "eventsource.firehose"
)

type infraOptions struct {
	hashKey       string
	rangeKey      string
	billingMode   string
	readCapacity  int64
	writeCapacity int64
	sns           struct {
		topicNames []string
	}
	sqs struct {
		queueNames []string
	}
	firehose struct {
		enabled bool
		bucket  string
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

// WithFirehose indicates kinesis firehose should be used to persist events to s3
func WithFirehose(enabled bool, bucket string) InfraOption {
	return func(i *infraOptions) {
		i.firehose.enabled = enabled
		i.firehose.bucket = bucket
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
	var tags []*dynamodb.Tag

	if _, ok := currentTags[tagCore]; !ok {
		tags = append(tags, &dynamodb.Tag{
			Key: aws.String(tagCore),
		})
	}
	if _, ok := currentTags[tagFirehose]; !ok && options.firehose.enabled {
		tags = append(tags, &dynamodb.Tag{
			Key:   aws.String(tagFirehose),
			Value: aws.String(options.firehose.bucket),
		})
	}
	if _, ok := currentTags[tagSNS]; !ok && len(options.sns.topicNames) > 0 {
		tags = append(tags, &dynamodb.Tag{
			Key:   aws.String(tagSNS),
			Value: aws.String(strings.Join(options.sns.topicNames, ",")),
		})
	}
	if _, ok := currentTags[tagSQS]; !ok && len(options.sqs.queueNames) > 0 {
		tags = append(tags, &dynamodb.Tag{
			Key:   aws.String(tagSQS),
			Value: aws.String(strings.Join(options.sqs.queueNames, ",")),
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

	return nil
}

func CreateTableIfNotExists(ctx context.Context, api dynamodbiface.DynamoDBAPI, tableName string, opts ...InfraOption) error {
	input := MakeCreateTableInput(tableName, opts...)

	// create the table
	//
	output, err := api.CreateTableWithContext(ctx, input)
	if err != nil {
		if v, ok := err.(awserr.Error); !ok || v.Code() != dynamodb.ErrCodeResourceInUseException {
			return fmt.Errorf("unable to create table, %v - %v", tableName, err)
		}
	}

	// wait for the table to exist
	//
	describeTableInput := &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}
	if err := api.WaitUntilTableExistsWithContext(ctx, describeTableInput); err != nil {
		return fmt.Errorf("error while waiting for table, %v, to be created - %v", tableName, err)
	}

	// tag the table
	//
	if err := createTagsIfNotPresent(ctx, api, output.TableDescription.TableArn, opts...); err != nil {
		return err
	}

	return nil
}
