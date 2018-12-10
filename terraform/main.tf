provider "aws" {
  region = "${var.region}"
}

provider "archive" {
}

data "aws_iam_policy_document" "policy" {
  statement {
    sid = ""
    effect = "Allow"

    principals {
      identifiers = [
        "lambda.amazonaws.com"]
      type = "Service"
    }

    actions = [
      "sts:AssumeRole"
    ]
  }
}

#-- producer --------------------------------------------------------------

data "archive_file" "producer" {
  type = "zip"
  source_file = "producer"
  output_path = "producer.zip"
}

resource "aws_iam_role" "producer" {
  name = "${var.namespace}-producer"
  assume_role_policy = "${data.aws_iam_policy_document.policy.json}"
}

resource "aws_iam_role_policy" "producer" {
  role = "${aws_iam_role.producer.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "sns:*",
        "sqs:*",
        "firehose:*"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "xray:*",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "logs:*",
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_cloudwatch_log_group" "producer" {
  name = "/aws/lambda/${var.namespace}-producer"

  tags = [
    {
      eventsource = ""
    }
  ]
}

resource "aws_lambda_function" "producer" {
  function_name = "${var.namespace}-producer"

  filename = "${data.archive_file.producer.output_path}"
  source_code_hash = "${data.archive_file.producer.output_base64sha256}"

  description = "publishes events to firehose, sns, or sqs based on tag configuration"
  role = "${aws_iam_role.producer.arn}"
  handler = "producer"
  runtime = "go1.x"
  timeout = 300

  tracing_config {
    mode = "Active"
  }

  tags = [
    {
      eventsource = ""
    }
  ]
}

#-- watcher ---------------------------------------------------------------

data "archive_file" "watcher" {
  type = "zip"
  source_file = "watcher"
  output_path = "watcher.zip"
}

resource "aws_cloudwatch_event_rule" "watcher" {
  name = "${var.namespace}-watcher"
  description = "associates ${var.namespace}-producer to dynamodb streams based on tag events"

  event_pattern = <<EOF
{
  "source": [
    "aws.dynamodb"
  ]
}
EOF
}

resource "aws_cloudwatch_event_target" "watcher" {
  rule = "${aws_cloudwatch_event_rule.watcher.name}"
  target_id = "EventSourceWatcher"
  arn = "${aws_lambda_function.watcher.arn}"
}

resource "aws_iam_role" "watcher" {
  name = "${var.namespace}-watcher"
  assume_role_policy = "${data.aws_iam_policy_document.policy.json}"
}

resource "aws_iam_role_policy" "watcher" {
  role = "${aws_iam_role.watcher.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateEventSourceMapping"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "xray:*",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "logs:*",
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_cloudwatch_log_group" "watcher" {
  name = "/aws/lambda/${var.namespace}-watcher"

  tags = [
    {
      eventsource = ""
    }
  ]
}


resource "aws_lambda_function" "watcher" {
  function_name = "${var.namespace}-watcher"

  filename = "${data.archive_file.watcher.output_path}"
  source_code_hash = "${data.archive_file.watcher.output_base64sha256}"

  description = "attach producer to dynamodb streams based on cloudwatch dynamodb tag events"
  role = "${aws_iam_role.watcher.arn}"
  handler = "watcher"
  runtime = "go1.x"
  timeout = 90

  depends_on = [
    "aws_iam_role_policy.watcher"
  ]

  tracing_config {
    mode = "Active"
  }

  environment {
    variables {
      FUNCTION_NAME = "${aws_lambda_function.producer.function_name}"
    }
  }

  tags = [
    {
      eventsource = ""
    }
  ]
}
