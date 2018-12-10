package lib

import (
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func findTagValue(tags []*dynamodb.Tag, tagKey string) (string, bool) {
	for _, tag := range tags {
		if tag.Key == nil || tag.Value == nil || *tag.Key != tagKey {
			continue
		}

		return strings.TrimSpace(*tag.Value), true
	}

	return "", false
}
