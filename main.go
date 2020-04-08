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

	values, err := h.MetricValues()
	if err != nil {
		return fmt.Errorf("metric values: %v", err)
	}

	if err := h.PostServiceMetricValues(values); err != nil {
		return fmt.Errorf("post service metric values: %v", err)
	}

	for _, p := range e.Period {
		table, schema, items, err := h.AccountCostItems(p)
		if err != nil {
			return fmt.Errorf("account cost items: %v", err)
		}
		if err := h.DataSet.CreateIfNotExists(bigquery.TableMetadata{
			Name:   table,
			Schema: schema,
		}); err != nil {
			return fmt.Errorf("create table=%s: %v", err)
		}
		if err := h.Put(table, items); err != nil {
			return fmt.Errorf("put=%s: %v", table, err)
		}
	}

	for _, p := range e.Period {
		table, schema, items, err := h.UtilizationItems(p)
		if err != nil {
			return fmt.Errorf("utilization items: %v", err)
		}
		if err := h.DataSet.CreateIfNotExists(bigquery.TableMetadata{
			Name:   table,
			Schema: schema,
		}); err != nil {
			return fmt.Errorf("create table=%s: %v", err)
		}
		if err := h.Put(table, items); err != nil {
			return fmt.Errorf("put=%s: %v", table, err)
		}
	}

	return nil
}
