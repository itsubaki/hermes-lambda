package dataset

import (
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/itsubaki/hermes-lambda/pkg/interface/dataset"
)

func TestDataSet(t *testing.T) {
	os.Setenv("AWS_PROFILE", "hermes-lambda")
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("BUCKET_NAME", "hermes-lambda-j96qd0m3kh1")
	os.Setenv("PERIOD", "1d")

	ds, err := New("hermes_lambda", "../../../credential.json")
	if err != nil {
		t.Errorf("new dataset: %v", err)
	}
	defer ds.Close()

	if err := ds.CreateIfNotExists(bigquery.TableMetadata{
		Name:   "1d_account_cost",
		Schema: dataset.AccountCostSchema,
	}); err != nil {
		t.Errorf("create table: %v", err)
	}
}
