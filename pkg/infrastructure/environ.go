package infrastructure

import (
	"os"
	"strings"
)

type Env struct {
	Dir              string
	Period           []string
	Region           []string
	SuppressWarning  bool
	IgnoreRecordType []string
	Driver           string // database
	DataSource       string // database
	Database         string // database
	BucketName       string // aws s3
	DataSetName      string // gcp bigquery
	Credential       string // gcp bigquery
	MkrAPIKey        string // mackerel
	MkrServiceName   string // mackerel
}

func Default() *Env {
	return &Env{
		Dir: "/tmp",
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
		SuppressWarning: true,
		IgnoreRecordType: []string{
			"Tax",
			"Enterprise Discount Program Discount",
		},
		Driver:      "mysql",
		DataSource:  "root:secret@tcp(127.0.0.1:3306)/",
		Database:    "hermes",
		BucketName:  "hermes-lambda",
		DataSetName: "hermes_lambda",
		Credential:  "./credential.json",
	}
}

func Environ() *Env {
	e := Default()

	dir := os.Getenv("DIR")
	if len(dir) > 0 {
		e.Dir = dir
	}

	period := os.Getenv("PERIOD")
	if len(period) > 0 {
		e.Period = strings.Split(period, ",")
	}

	region := os.Getenv("REGION")
	if len(region) > 0 {
		e.Region = strings.Split(region, ",")
	}

	warning := os.Getenv("SUPPRESS_WARNING")
	if warning == "FALSE" || warning == "false" {
		e.SuppressWarning = false
	}

	ignoreRecordType := os.Getenv("IGNORE_RECORD_TYPE")
	if len(ignoreRecordType) > 0 {
		e.IgnoreRecordType = strings.Split(ignoreRecordType, ",")
	}

	driver := os.Getenv("DRIVER")
	if len(driver) > 0 {
		e.Driver = driver
	}

	source := os.Getenv("DATASOURCE")
	if len(source) > 0 {
		e.DataSource = source
	}

	database := os.Getenv("DATABASE")
	if len(database) > 0 {
		e.Database = database
	}

	bucketName := os.Getenv("BUCKET_NAME")
	if len(bucketName) > 0 {
		e.BucketName = bucketName
	}

	datasetName := os.Getenv("DATASET_NAME")
	if len(datasetName) > 0 {
		e.DataSetName = datasetName
	}

	credential := os.Getenv("CREDENTIAL")
	if len(credential) > 0 {
		e.Credential = credential
	}

	apikey := os.Getenv("MACKEREL_APIKEY")
	if len(apikey) > 0 {
		e.MkrAPIKey = apikey
	}

	service := os.Getenv("MACKEREL_SERVICE_NAME")
	if len(service) > 0 {
		e.MkrServiceName = service
	}

	return e
}
