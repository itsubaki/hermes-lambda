package dataset

import "cloud.google.com/go/bigquery"

type Items struct {
	TableName   string
	TableSchema bigquery.Schema
	Items       interface{}
}
