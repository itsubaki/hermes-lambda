package dataset

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type Reservation struct {
	AccountID        string     `bigquery:"account_id"`
	Description      string     `bigquery:"description"`
	Region           string     `bigquery:"region"`
	InstanceType     string     `bigquery:"instance_type"`
	Platform         string     `bigquery:"platform"`
	CacheEngine      string     `bigquery:"cache_engine"`
	DatabaseEngine   string     `bigquery:"database_engine"`
	DeploymentOption string     `bigquery:"deployment_option"`
	Date             civil.Date `bigquery:"date"`
	Hours            float64    `bigquery:"hours"`
	Num              float64    `bigquery:"num"`
	Percentage       float64    `bigquery:"percentage"`
	InsertedAt       time.Time  `bigquery:"inserted_at"`
}

var ReservationSchema = bigquery.Schema{
	{Name: "account_id", Type: bigquery.StringFieldType},
	{Name: "description", Type: bigquery.StringFieldType},
	{Name: "region", Type: bigquery.StringFieldType},
	{Name: "instance_type", Type: bigquery.StringFieldType},
	{Name: "platform", Type: bigquery.StringFieldType},
	{Name: "cache_engine", Type: bigquery.StringFieldType},
	{Name: "database_engine", Type: bigquery.StringFieldType},
	{Name: "deployment_option", Type: bigquery.StringFieldType},
	{Name: "date", Type: bigquery.DateFieldType},
	{Name: "hours", Type: bigquery.FloatFieldType},
	{Name: "num", Type: bigquery.FloatFieldType},
	{Name: "percentage", Type: bigquery.FloatFieldType},
	{Name: "inserted_at", Type: bigquery.TimestampFieldType},
}

func ReservationMeta(period string) bigquery.TableMetadata {
	return bigquery.TableMetadata{
		Name:   fmt.Sprintf("%s_reservation", period),
		Schema: ReservationSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "date",
		},
	}
}
