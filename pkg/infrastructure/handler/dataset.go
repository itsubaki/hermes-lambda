package handler

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
)

type DataSet struct {
	Context context.Context
	Client  *bigquery.Client
	Name    string
}

func NewDataSet(name, credential string) (*DataSet, error) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credential)

	ctx := context.Background()
	creds, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		return nil, fmt.Errorf("find default credentials: %v", err)
	}

	bq, err := bigquery.NewClient(ctx, creds.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("new bigqurey client: %v", err)
	}

	return &DataSet{
		Context: ctx,
		Client:  bq,
		Name:    name,
	}, nil
}

func (d *DataSet) Close() error {
	return d.Client.Close()
}

func (d *DataSet) Put(table string, items interface{}) error {
	return d.Client.Dataset(d.Name).Table(table).Inserter().Put(d.Context, items)
}

func (d *DataSet) CreateIfNotExists(m bigquery.TableMetadata) error {
	ref := d.Client.Dataset(d.Name).Table(m.Name)

	found := true
	if _, err := ref.Metadata(d.Context); err != nil {
		found = false
	}

	if found {
		return nil
	}

	if err := ref.Create(d.Context, &m); err != nil {
		return fmt.Errorf("create table=%s: %v", m.Name, err)
	}

	return nil
}