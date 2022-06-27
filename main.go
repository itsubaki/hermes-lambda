package main

import (
	"context"
	"fmt"
	"log"
	"os"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes-lambda/infrastructure/config"
	"github.com/itsubaki/hermes-lambda/infrastructure/lambda"
	"github.com/itsubaki/hermes-lambda/infrastructure/lambda/bigquery"
	"github.com/itsubaki/hermes-lambda/infrastructure/lambda/database"
	"github.com/itsubaki/hermes-lambda/infrastructure/lambda/mackerel"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("start")

	if len(os.Getenv("AWS_LAMBDA_FUNCTION_NAME")) > 0 {
		awslambda.Start(handle)
		log.Println("finished")
		return
	}

	if err := handle(context.TODO()); err != nil {
		log.Printf("handle: %v", err)
	}

	log.Println("finished")
}

func handle(ctx context.Context) error {
	log.Printf("context: %v", ctx)

	c := config.New()
	log.Printf("config=%v\n", c)

	h := lambda.Default(c.Output)
	h.Add("database", database.New(c))
	h.Add("mackerel", mackerel.New(c))
	h.Add("bigquery", bigquery.New(c))

	if err := h.Run(); err != nil {
		return fmt.Errorf("run: %v", err)
	}

	return nil
}
