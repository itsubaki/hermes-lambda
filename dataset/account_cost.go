package dataset

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type AccountCost struct {
	AccountID        string     `bigquery:"account_id"`
	Description      string     `bigquery:"description"`
	Date             civil.Date `bigquery:"date"`
	Service          string     `bigquery:"service"`
	RecordType       string     `bigquery:"record_type"`
	UnblendedCost    float64    `bigquery:"unblended_cost"`     // volume discount for a single account
	BlendedCost      float64    `bigquery:"blended_cost"`       // volume discount across linked account
	AmortizedCost    float64    `bigquery:"amortized_cost"`     // unblended + ReservedInstances/12
	NetAmortizedCost float64    `bigquery:"net_amortized_cost"` // before discount
	NetUnblendedCost float64    `bigquery:"net_unblended_cost"` // before discount
	InsertedAt       time.Time  `bigquery:"inserted_at"`
}

var AccountCostSchema = bigquery.Schema{
	{Name: "account_id", Type: bigquery.StringFieldType},
	{Name: "description", Type: bigquery.StringFieldType},
	{Name: "date", Type: bigquery.DateFieldType},
	{Name: "service", Type: bigquery.StringFieldType},
	{Name: "record_type", Type: bigquery.StringFieldType},
	{Name: "unblended_cost", Type: bigquery.FloatFieldType},
	{Name: "blended_cost", Type: bigquery.FloatFieldType},
	{Name: "amortized_cost", Type: bigquery.FloatFieldType},
	{Name: "net_amortized_cost", Type: bigquery.FloatFieldType},
	{Name: "net_unblended_cost", Type: bigquery.FloatFieldType},
	{Name: "inserted_at", Type: bigquery.TimestampFieldType},
}

func AccountCostMeta(period string) bigquery.TableMetadata {
	return bigquery.TableMetadata{
		Name:   fmt.Sprintf("%s_account_cost", period),
		Schema: AccountCostSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "date",
		},
	}
}
