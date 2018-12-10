package lib

type Tag struct {
	Key   string
	Value string
}

type TagRequest struct {
	ResourceArn string
	TagKeys     []string // TagKeys is used to remove Tags
	Tags        []Tag    // Tags is used to insert tags
}

type DynamoDBEvent struct {
	EventSource       string
	EventTime         string
	EventName         string
	ErrorCode         string
	ErrorMessage      string
	RequestParameters TagRequest
}
