package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/itsubaki/hermes/pkg/pricing"

	"github.com/itsubaki/hermes/pkg/reservation"

	"github.com/itsubaki/hermes/pkg/usage"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
)

func handle(ctx context.Context) error {
	e := infrastructure.NewEnv()
	log.Printf("env=%#v", e)

	date, err := calendar.Last(e.Period)
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", e.Period, err)
	}

	if err := fetch(e.BucketName, e.Region, date); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	return nil
}

func fetch(bucketName string, region []string, date []calendar.Date) error {
	s3, err := infrastructure.NewStorage()
	if err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(bucketName); err != nil {
		return fmt.Errorf("create bucket=%s if not exists: %v", bucketName, err)
	}

	for _, r := range region {
		file := fmt.Sprintf("pricing/%s.json", r)

		exists, err := s3.Exists(bucketName, file)
		if err != nil {
			return fmt.Errorf("exists: %v", err)
		}

		if exists {
			continue
		}

		price := make([]pricing.Price, 0)
		for _, url := range pricing.URL {
			p, err := pricing.Fetch(url, r)
			if err != nil {
				return fmt.Errorf("fetch pricing (%s, %s): %v\n", url, r, err)
			}

			list := make([]pricing.Price, 0)
			for k := range p {
				list = append(list, p[k])
			}

			price = append(price, list...)
		}

		b, err := json.Marshal(price)
		if err != nil {
			return fmt.Errorf("marshal: %v", err)
		}

		if err := s3.Write(bucketName, file, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, file, err)
		}
	}

	for i := range date {
		file := fmt.Sprintf("cost/%s.json", date[i].String())

		exists, err := s3.Exists(bucketName, file)
		if err != nil {
			return fmt.Errorf("exists: %v", err)
		}

		if exists {
			continue
		}

		ac, err := cost.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch cost (%s, %s): %v\n", date[i].Start, date[i].End, err)
		}

		b, err := json.Marshal(ac)
		if err != nil {
			return fmt.Errorf("marshal: %v\n", err)
		}

		if err := s3.Write(bucketName, file, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, file, err)
		}
	}

	for i := range date {
		file := fmt.Sprintf("usage/%s.json", date[i].String())

		exists, err := s3.Exists(bucketName, file)
		if err != nil {
			return fmt.Errorf("exists: %v", err)
		}

		if exists {
			continue
		}

		ac, err := usage.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch usage (%s, %s): %v\n", date[i].Start, date[i].End, err)
		}

		b, err := json.Marshal(ac)
		if err != nil {
			return fmt.Errorf("marshal: %v\n", err)
		}

		if err := s3.Write(bucketName, file, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, file, err)
		}
	}

	for i := range date {
		file := fmt.Sprintf("resevation/%s.json", date[i].String())

		exists, err := s3.Exists(bucketName, file)
		if err != nil {
			return fmt.Errorf("exists: %v", err)
		}

		if exists {
			continue
		}

		ac, err := reservation.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch resevation (%s, %s): %v\n", date[i].Start, date[i].End, err)
		}

		b, err := json.Marshal(ac)
		if err != nil {
			return fmt.Errorf("marshal: %v\n", err)
		}

		if err := s3.Write(bucketName, file, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, file, err)
		}
	}

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(handle)
	log.Println("finished")
}
