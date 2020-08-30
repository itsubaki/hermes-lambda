package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/environ"
	lmda "github.com/itsubaki/hermes-lambda/pkg/infrastructure/lambda"

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

	e := environ.New()
	log.Printf("env=%#v", e)

	h, err := lmda.New(e)
	if err != nil {
		return fmt.Errorf("new: %v", err)
	}

	for _, o := range e.Output {
		if strings.ToLower(o) == "bigquery" {
			if err := h.Run(); err != nil {
				return fmt.Errorf("output to bigquery: %v", err)
			}
		}

		if strings.ToLower(o) == "mackerel" {
			v, err := h.MetricValues()
			if err != nil {
				return fmt.Errorf("metric values of mackerel: %v", err)
			}
			if err := h.PostServiceMetricValues(v); err != nil {
				return fmt.Errorf("output to mackerel: %v", err)
			}
		}

		if strings.ToLower(o) == "database" {
			if err := h.Store(); err != nil {
				return fmt.Errorf("output to database: %v", err)
			}
		}
	}

	return nil
}
