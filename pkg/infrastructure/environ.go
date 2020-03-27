package infrastructure

import (
	"os"
	"strings"
)

type Environ struct {
	Dir                 string
	Period              []string
	Region              []string
	Driver              string
	DataSource          string
	Database            string
	BucketName          string
	MackerelAPIKey      string
	MackerelServiceName string
	IgnoreRecordType    []string
	SuppressWarning     bool
}

func DefaultEnv() *Environ {
	return &Environ{
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
		IgnoreRecordType: []string{
			"Tax",
			"Enterprise Discount Program Discount",
		},
		Driver:          "mysql",
		DataSource:      "root:secret@tcp(127.0.0.1:3306)/",
		Database:        "hermes",
		BucketName:      "hermes-lambda",
		SuppressWarning: true,
	}
}

func NewEnv() *Environ {
	e := DefaultEnv()

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
		e.Region = strings.Split(ignoreRecordType, ",")
	}

	return e
}
