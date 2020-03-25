package infrastructure

import (
	"os"
	"strings"
)

type Environ struct {
	Dir             string
	Period          string
	Region          []string
	Driver          string
	DataSource      string
	Database        string
	BucketName      string
	SuppressWarning bool
}

func DefaultEnv() *Environ {
	return &Environ{
		Dir:    "/tmp",
		Period: "1m",
		Region: []string{
			"ap-northeast-1",
			"ap-southeast-1",
			"us-west-1",
			"us-west-2",
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
		e.Period = period
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

	return e
}
