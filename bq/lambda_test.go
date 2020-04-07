package bq

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
)

func TestFetch(t *testing.T) {
	os.Setenv("AWS_PROFILE", "hermes-lambda")
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("PERIOD", "1d")

	Handle(context.Background())
}

func TestBigQuery(t *testing.T) {
	path := fmt.Sprintf("%s/../credential.json", os.Getenv("PWD"))
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)

	ctx := context.Background()
	creds, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		log.Fatalf("find default credentials: %v", err)
	}
	log.Printf("ProjectID=%v", creds.ProjectID)

	// bigquery client
	bq, err := bigquery.NewClient(ctx, creds.ProjectID)
	if err != nil {
		log.Fatalf("new bigqurey client: %v", err)
	}

	q := bq.Query("SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` WHERE state = \"TX\" LIMIT 10")
	q.Location = "US"

	job, err := q.Run(ctx)
	if err != nil {
		log.Fatalf("bigquery run: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("bigquery job wait: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("bigquery job status: %v", err)
	}

	it, err := job.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("bigquery read: %v", err)
		}

		log.Printf("%v", row)
	}
}
