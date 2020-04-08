package dataset

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

func New(name, credential string) (*DataSet, error) {
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
			Name:   fmt.Sprintf("%s_account_cost", p),
			Schema: accountCost,
		})
		md = append(md, bigquery.TableMetadata{
			Name:   fmt.Sprintf("%s_utilization", p),
			Schema: utilization,
		})
	}

	return md
}
