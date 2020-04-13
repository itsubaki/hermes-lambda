package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"

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

	h, err := internal.New(e)
	if err != nil {
		return fmt.Errorf("new hermes-lambda: %v", err)
	}
	defer h.Close()

	if err := h.Fetch(); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	items, err := h.Items()
	if err != nil {
		return fmt.Errorf("items: %v", err)
	}

	for _, i := range items {
		if err := h.DataSet.CreateIfNotExists(bigquery.TableMetadata{
			Name:   i.TableName,
			Schema: i.TableSchema,
		}); err != nil {
			return fmt.Errorf("create table=%s: %v", i.TableName, err)
		}

		if err := h.Put(i.TableName, i.Items); err != nil {
			return fmt.Errorf("put=%s: %v", i.TableName, err)
		}
	}

	if len(e.MackerelServiceName) < 1 {
		return nil
	}

	values, err := h.MetricValues()
	if err != nil {
		return fmt.Errorf("metric values: %v", err)
	}

	if err := h.PostServiceMetricValues(values); err != nil {
		return fmt.Errorf("post service metric values: %v", err)
	}

	return nil
}
