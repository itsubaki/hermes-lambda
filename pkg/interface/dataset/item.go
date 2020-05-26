package dataset

import "cloud.google.com/go/bigquery"

type Items struct {
	TableMetadata bigquery.TableMetadata
	Items         interface{}
}
