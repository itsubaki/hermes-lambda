package internal

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

func (d *DataSet) CreateIfNotExists(period []string) error {
	for _, m := range TableMetadata(period) {
		ref := d.Client.Dataset(d.Name).Table(m.Name)

		found := true
		if _, err := ref.Metadata(d.Context); err != nil {
			found = false
		}

		if found {
			continue
		}

		if err := ref.Create(d.Context, &m); err != nil {
			return fmt.Errorf("create table=%s: %v", m.Name, err)
		}
	}

	return nil
}

func TableMetadata(period []string) []bigquery.TableMetadata {
	md := make([]bigquery.TableMetadata, 0)
	for _, p := range period {
		md = append(md, bigquery.TableMetadata{
			Name: fmt.Sprintf("%s_account_cost", p),
			Schema: bigquery.Schema{
				{Name: "timestamp", Type: bigquery.TimestampFieldType},
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
				{Name: "timestamp", Type: bigquery.TimestampFieldType},
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

	return md
}
