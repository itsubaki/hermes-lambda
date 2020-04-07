package main

import (
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes-lambda/mackerel"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(mackerel.Handle)
	log.Println("finished")
}
