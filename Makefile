SHELL := /bin/bash
DATE := $(shell date +%Y%m%d-%H:%M:%S)
HASH := $(shell git rev-parse HEAD)
GOVERSION := $(shell go version)
LDFLAGS := -X 'main.date=${DATE}' -X 'main.hash=${HASH}' -X 'main.goversion=${GOVERSION}'

build:
	GO111MODULE=on go mod tidy
	GOOS=linux GOARCH=amd64 go build -o hermes-lambda
	zip hermes-lambda.zip hermes-lambda credential.json

upload:
	aws s3 cp hermes-lambda.zip s3://${S3Bucket}/lambda/hermes-lambda.zip

deploy: build upload
	aws cloudformation create-stack --region ap-northeast-1 --stack-name hermes-lambda --capabilities CAPABILITY_NAMED_IAM --template-body file://template.yaml \
	--parameters \
	ParameterKey=S3Bucket,ParameterValue=${S3Bucket} \
	ParameterKey=S3Key,ParameterValue=lambda/hermes-lambda.zip

update: build upload
	aws cloudformation update-stack --region ap-northeast-1 --stack-name hermes-lambda --capabilities CAPABILITY_NAMED_IAM --template-body file://template.yaml \
	--parameters \
	ParameterKey=S3Bucket,ParameterValue=${S3Bucket} \
	ParameterKey=S3Key,ParameterValue=lambda/hermes-lambda.zip

mysql:
	set -x
	-docker pull mysql
	-docker stop mysql
	-docker rm mysql
	docker run --name mysql -e MYSQL_ROOT_PASSWORD=secret -p 3306:3306 -d mysql
	docker ps
	# mysql -h127.0.0.1 -P3306 -uroot -psecret -Dhermes

start-mysql:
	set -x
	docker start mysql
	docker ps
	# mysql -h127.0.0.1 -P3306 -uroot -psecret -Dhermes

.PHONY: test
test:
	go test -cover $(shell go list ./... | grep -v /vendor/ | grep -v /build/) -v

