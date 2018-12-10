terraform
------------------------------------------------

`terraform` contains scripts to build the environment

```bash
cd terraform
GOOS=linux GOARCH=amd64 go build -o watcher ../functions/watcher/main.go
GOOS=linux GOARCH=amd64 go build -o producer ../functions/producer/main.go

terraform init
terraform plan
terraform apply
```
