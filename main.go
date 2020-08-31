package main

import (
	"context"
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/environ"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/lambda"

	awslambda "github.com/aws/aws-lambda-go/lambda"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("start")

	// if err := handle(context.TODO()); err != nil {
	// 	log.Printf("handle: %v", err)
	// }

	awslambda.Start(handle)
	log.Println("finished")
}

func handle(c context.Context) error {
	log.Printf("context: %v", c)

	e := environ.New()
	log.Printf("env=%#v", e)

	if err := lambda.Default(e).Run(); err != nil {
		return fmt.Errorf("run: %v", err)
	}

	return nil
}
