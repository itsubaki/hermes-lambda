package main

import (
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	l "github.com/itsubaki/hermes-lambda/pkg/infrastructure/lambda"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(l.Handle2)
	log.Println("finished")
}
