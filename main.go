package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes-lambda/pkg/monitoring"
	"github.com/itsubaki/hermes-lambda/pkg/storage"
	"github.com/mackerelio/mackerel-client-go"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(handle)
	log.Println("finished")
}

func fetch(e *Environ) error {
	s3, err := storage.New()
	if err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	p := &storage.Pricing{Storage: s3}
	if err := p.Fetch(e.BucketName, e.Region); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	uc := &storage.AccountCost{Storage: s3}
	for _, p := range e.Period {
		if err := uc.Fetch(p, e.BucketName); err != nil {
			return fmt.Errorf("fetch unlbended cost: %v", err)
		}
	}

	cc := &storage.CoveringCost{Storage: s3, SuppressWarning: true}
	for _, p := range e.Period {
		if err := cc.Fetch(p, e.BucketName); err != nil {
			return fmt.Errorf("fetch covering cost: %v", err)
		}
	}

	return nil
}

func getMetricValues(e *Environ) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	s3, err := storage.New()
	if err != nil {
		return values, fmt.Errorf("new storage: %v", err)
	}

	uc := &storage.AccountCost{Storage: s3}
	cc := &storage.CoveringCost{Storage: s3, SuppressWarning: true}

	for _, p := range e.Period {
		a, err := uc.Aggregate(p, e.BucketName, e.IgnoreRecordType, e.Region)
		if err != nil {
			return values, fmt.Errorf("aggregate unblended cost: %v", err)
		}

		for k, v := range a {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.unblended_cost.%s", p, strings.Replace(k, " ", "", -1)),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}
	}

	for _, p := range e.Period {
		a, err := cc.Aggregate(p, e.BucketName, e.Region)
		if err != nil {
			return values, fmt.Errorf("aggregate covering cost: %v", err)
		}

		for k, v := range a {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.ri_covering_cost.%s", p, strings.Replace(k, " ", "", -1)),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}
	}

	for _, p := range e.Period {
		unblended, err := uc.Aggregate(p, e.BucketName, e.IgnoreRecordType, e.Region)
		if err != nil {
			return values, fmt.Errorf("aggregate unblended cost: %v", err)
		}

		covering, err := cc.Aggregate(p, e.BucketName, e.Region)
		if err != nil {
			return values, fmt.Errorf("aggregate covering cost: %v", err)
		}

		total := func(u, c map[string]float64) map[string]float64 {
			total := make(map[string]float64)

			for k, v := range u {
				vv, ok := c[k]
				if !ok {
					continue
				}

				total[k] = v + vv
			}

			return total
		}(unblended, covering)

		for k, v := range total {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.rebate_cost.%s", p, strings.Replace(k, " ", "", -1)),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}
	}

	return values, nil
}

func createTable(e *Environ) error {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", e.Credential)

	ctx := context.Background()
	creds, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		return fmt.Errorf("find default credentials: %v", err)
	}

	bq, err := bigquery.NewClient(ctx, creds.ProjectID)
	if err != nil {
		return fmt.Errorf("new bigqurey client: %v", err)
	}
	defer bq.Close()

	md := make([]bigquery.TableMetadata, 0)
	for _, p := range e.Period {
		md = append(md, bigquery.TableMetadata{
			Name: fmt.Sprintf("%s_account_cost", p),
			Schema: bigquery.Schema{
				{Name: "account_id", Type: bigquery.StringFieldType},
				{Name: "description", Type: bigquery.StringFieldType},
				{Name: "date", Type: bigquery.StringFieldType},
				{Name: "service", Type: bigquery.StringFieldType},
				{Name: "record_type", Type: bigquery.StringFieldType},
				{Name: "unblended_cost", Type: bigquery.FloatFieldType},
				{Name: "blended_cost", Type: bigquery.FloatFieldType},
				{Name: "amortized_cost", Type: bigquery.FloatFieldType},
				{Name: "net_amortized_cost", Type: bigquery.FloatFieldType},
				{Name: "net_unblended_cost", Type: bigquery.FloatFieldType},
			},
		})
		md = append(md, bigquery.TableMetadata{
			Name: fmt.Sprintf("%s_utilization", p),
			Schema: bigquery.Schema{
				{Name: "account_id", Type: bigquery.StringFieldType},
				{Name: "description", Type: bigquery.StringFieldType},
				{Name: "region", Type: bigquery.StringFieldType},
				{Name: "instance_type", Type: bigquery.StringFieldType},
				{Name: "platform", Type: bigquery.StringFieldType},
				{Name: "cache_engine", Type: bigquery.StringFieldType},
				{Name: "database_engine", Type: bigquery.StringFieldType},
				{Name: "deployment_option", Type: bigquery.StringFieldType},
				{Name: "date", Type: bigquery.StringFieldType},
				{Name: "hours", Type: bigquery.FloatFieldType},
				{Name: "num", Type: bigquery.FloatFieldType},
				{Name: "percentage", Type: bigquery.FloatFieldType},
				{Name: "covering_cost", Type: bigquery.FloatFieldType},
			},
		})
	}

	for _, m := range md {
		ref := bq.Dataset(e.DataSetName).Table(m.Name)
		if err := ref.Create(ctx, &m); err != nil {
			return fmt.Errorf("create table: %v", err)
		}
	}

	return nil
}

func handle(ctx context.Context) error {
	e := NewEnv()
	log.Printf("env=%#v", e)

	if err := fetch(e); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	values, err := getMetricValues(e)
	if err != nil {
		return fmt.Errorf("get metric values: %v", err)
	}

	if err := monitoring.PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
		return fmt.Errorf("post service metric values: %v", err)
	}

	if err := createTable(e); err != nil {
		log.Printf("create bigquery table: %v", err)
	}

	return nil
}
