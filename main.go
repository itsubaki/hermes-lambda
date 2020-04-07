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

	s3, err := internal.New()
	if err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	p := &internal.Pricing{Storage: s3}
	if err := p.Fetch(e.BucketName, e.Region); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	ac := &internal.AccountCost{Storage: s3}
	if err := ac.Fetch(e.Period, e.BucketName); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	cc := &internal.Utilization{Storage: s3, SuppressWarning: true}
	if err := cc.Fetch(e.Period, e.BucketName); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	m := &internal.Metric{
		AccountCost:      ac,
		Utilization:      cc,
		BucketName:       e.BucketName,
		Period:           e.Period,
		IgnoreRecordType: e.IgnoreRecordType,
		Region:           e.Region,
	}
	values, err := m.MetricValues()
	if err != nil {
		return fmt.Errorf("metric values: %v", err)
	}

	if err := internal.PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
		return fmt.Errorf("post service metric values: %v", err)
	}

	return nil
}
