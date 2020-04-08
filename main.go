package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

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

	if err := l.Fetch(); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	values, err := l.MetricValues()
	if err != nil {
		return fmt.Errorf("metric values: %v", err)
	}

	if err := internal.PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
		return fmt.Errorf("post service metric values: %v", err)
	}

	ds, err := internal.NewDataSet(e.DataSetName, e.Credential)
	if err != nil {
		return fmt.Errorf("new dataset: %v", err)
	}
	defer ds.Close()

	if err := ds.CreateIfNotExists(e.Period); err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	for _, p := range e.Period {
		c, err := l.AccountCost.Read(p, e.BucketName)
		if err != nil {
			return fmt.Errorf("read: %v", err)
		}

		type AccountCostRow struct {
			Timestamp        time.Time `bigquery:"timestamp"`
			AccountID        string    `bigquery:"account_id"`
			Description      string    `bigquery:"description"`
			Date             string    `bigquery:"date"`
			Service          string    `bigquery:"service"`
			RecordType       string    `bigquery:"record_type"`
			UnblendedCost    float64   `bigquery:"unblended_cost"`     // volume discount for a single account
			BlendedCost      float64   `bigquery:"blended_cost"`       // volume discount across linked account
			AmortizedCost    float64   `bigquery:"amortized_cost"`     // unblended + ReservedInstances/12
			NetAmortizedCost float64   `bigquery:"net_amortized_cost"` // before discount
			NetUnblendedCost float64   `bigquery:"net_unblended_cost"` // before discount
		}

		items := make([]*AccountCostRow, 0)
		for _, cc := range c {
			u, _ := strconv.ParseFloat(cc.UnblendedCost.Amount, 64)
			b, _ := strconv.ParseFloat(cc.BlendedCost.Amount, 64)
			a, _ := strconv.ParseFloat(cc.AmortizedCost.Amount, 64)
			na, _ := strconv.ParseFloat(cc.NetAmortizedCost.Amount, 64)
			nu, _ := strconv.ParseFloat(cc.NetUnblendedCost.Amount, 64)

			items = append(items, &AccountCostRow{
				Timestamp:        time.Now(),
				AccountID:        cc.AccountID,
				Description:      cc.Description,
				Date:             cc.Date,
				Service:          cc.Service,
				RecordType:       cc.RecordType,
				UnblendedCost:    u,
				BlendedCost:      b,
				AmortizedCost:    a,
				NetAmortizedCost: na,
				NetUnblendedCost: nu,
			})
		}

		table := fmt.Sprintf("%s_account_cost", p)
		if err := ds.Put(table, items); err != nil {
			return fmt.Errorf("put=%s: %v", table, err)
		}
	}

	return nil
}
