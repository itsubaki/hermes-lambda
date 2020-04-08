package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes-lambda/internal"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(handle)
	log.Println("finished")
}

func handle(ctx context.Context) error {
	e := internal.Environ()
	log.Printf("env=%#v", e)

	l, err := internal.New(e)
	if err != nil {
		return fmt.Errorf("new hermes-lambda: %v", err)
	}

	values, err := l.Fetch()
	if err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	if err := internal.PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
		return fmt.Errorf("post service metric values: %v", err)
	}

	return nil
}
