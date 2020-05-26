package dataset

import "cloud.google.com/go/bigquery"

type DataSet interface {
	CreateIfNotExists(m bigquery.TableMetadata) error
	Put(table string, items interface{}) error
	Close() error
}
