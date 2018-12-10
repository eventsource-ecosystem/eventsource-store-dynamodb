#!/bin/bash

set -euv

ROOT="$(cd $(dirname $0)/.. && pwd)"
TF="${ROOT}/terraform"

GOOS=linux GOARCH=amd64 go build -o ${TF}/watcher ${ROOT}/functions/watcher/main.go
GOOS=linux GOARCH=amd64 go build -o ${TF}/producer ${ROOT}/functions/producer/main.go

terraform init
terraform plan
