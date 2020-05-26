package main

import (
	"context"
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("start")

	lambda.Start(handle)
	log.Println("finished")
}

func handle(c context.Context) error {
	log.Printf("context: %v", c)

	e := infrastructure.Environ()
	log.Printf("env=%#v", e)

	h, err := New(e)
	if err != nil {
		return fmt.Errorf("new: %v", err)
	}

	if err := h.Run(); err != nil {
		return fmt.Errorf("run: %v", err)
	}

	return nil
}
