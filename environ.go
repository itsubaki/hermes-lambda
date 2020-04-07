package main

import (
	"os"
	"strings"
)

type Environ struct {
	Period              []string
	Region              []string
	BucketName          string
	IgnoreRecordType    []string
	SuppressWarning     bool
	Credential          string
	DataSetName         string
	MackerelAPIKey      string
	MackerelServiceName string
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
		IgnoreRecordType: []string{
			"Tax",
			"Enterprise Discount Program Discount",
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

	mackerelapikey := os.Getenv("MACKEREL_APIKEY")
	if len(mackerelapikey) > 0 {
		e.MackerelAPIKey = mackerelapikey
	}

	mackerelservicename := os.Getenv("MACKEREL_SERVICE_NAME")
	if len(mackerelservicename) > 0 {
		e.MackerelServiceName = mackerelservicename
	}

	ignoreRecordType := os.Getenv("IGNORE_RECORD_TYPE")
	if len(ignoreRecordType) > 0 {
		e.IgnoreRecordType = strings.Split(ignoreRecordType, ",")
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
