package bq

import (
	"os"
	"strings"
)

type Environ struct {
	Period          []string
	Region          []string
	BucketName      string
	SuppressWarning bool
	Credential      string
	DataSetName     string
}

func DefaultEnv() *Environ {
	return &Environ{
		Period: []string{
			"1m",
			"1d",
		},
		Region: []string{
			"ap-northeast-1",
			"ap-southeast-1",
			"us-west-1",
			"us-west-2",
		},
		BucketName:      "hermes-lambda",
		SuppressWarning: true,
		Credential:      "./credential.json",
		DataSetName:     "hermes_lambda",
	}
}

func NewEnv() *Environ {
	e := DefaultEnv()

	period := os.Getenv("PERIOD")
	if len(period) > 0 {
		e.Period = strings.Split(period, ",")
	}

	region := os.Getenv("REGION")
	if len(region) > 0 {
		e.Region = strings.Split(region, ",")
	}

	bucketName := os.Getenv("BUCKET_NAME")
	if len(bucketName) > 0 {
		e.BucketName = bucketName
	}

	warning := os.Getenv("SUPPRESS_WARNING")
	if warning == "FALSE" || warning == "false" {
		e.SuppressWarning = false
	}

	credential := os.Getenv("CREDENTIAL")
	if len(credential) > 0 {
		e.Credential = credential
	}

	datasetName := os.Getenv("DATASET_NAME")
	if len(datasetName) > 0 {
		e.DataSetName = datasetName
	}

	return e
}
