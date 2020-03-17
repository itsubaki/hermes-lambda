SHELL := /bin/bash
DATE := $(shell date +%Y%m%d-%H:%M:%S)
HASH := $(shell git rev-parse HEAD)
GOVERSION := $(shell go version)
LDFLAGS := -X 'main.date=${DATE}' -X 'main.hash=${HASH}' -X 'main.goversion=${GOVERSION}'

build:
	GOOS=linux GOARCH=amd64 go build -o hermes-lambda
	zip handler.zip hermes-lambda

upload:
	aws s3 cp handler.zip s3://${S3Bucket}/lambda/hermes-lambda.zip

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

runmysql:
	set -x
	-docker pull mysql
	-docker stop mysql
	-docker rm mysql
	docker run --name mysql -e MYSQL_ROOT_PASSWORD=secret -p 3306:3306 -d mysql
	# mysql -h127.0.0.1 -P3306 -uroot -psecret


.PHONY: test
test:
	go test -cover $(shell go list ./... | grep -v /vendor/ | grep -v /build/) -v

