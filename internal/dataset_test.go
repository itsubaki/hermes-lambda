package internal

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDataSet(t *testing.T) {
	os.Setenv("AWS_PROFILE", "hermes-lambda")
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("BUCKET_NAME", "hermes-lambda-j96qd0m3kh1")
	os.Setenv("PERIOD", "1d")

	ds, err := NewDataSet("hermes_lambda", "../credential.json")
	if err != nil {
		t.Errorf("new dataset: %v", err)
	}
	defer ds.Close()

	if err := ds.CreateIfNotExists([]string{"1d"}); err != nil {
		t.Errorf("create table: %v", err)
	}

	e := Environ()
	log.Printf("env=%#v", e)

	l, err := New(e)
	if err != nil {
		t.Errorf("new hermes-lambda: %v", err)
	}

	if err := l.Fetch(); err != nil {
		t.Errorf("fetch: %v", err)
	}

	c, err := l.AccountCost.Read("1d", e.BucketName)
	if err != nil {
		t.Errorf("read: %v", err)
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

	ins := ds.Client.Dataset(ds.Name).Table("1d_account_cost").Inserter()
	if err := ins.Put(ds.Context, items); err != nil {
		t.Errorf("put: %v", err)
	}
}
