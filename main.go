package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
	"github.com/itsubaki/hermes/pkg/usage"
	mackerel "github.com/mackerelio/mackerel-client-go"
)

func handle(ctx context.Context) error {
	e := infrastructure.NewEnv()
	log.Printf("env=%#v", e)

	period := strings.Split(e.Period, ", ")
	for _, p := range period {
		date, err := calendar.Last(p)
		if err != nil {
			return fmt.Errorf("calendar.Last period=%s: %v", p, err)
		}

		if err := Download(e.BucketName, e.Region, date); err != nil {
			return fmt.Errorf("write: %v", err)
		}
	}

	// unblended cost
	for _, p := range []string{"1d", "1m"} {
		unblended, err := CreateUnblendedCost(p, e.BucketName, e.IgnoreRecordType)
		if err != nil {
			return fmt.Errorf("create unbelnded cost metrc values: %v", err)
		}

		values := make([]*mackerel.MetricValue, 0)
		for k, v := range unblended {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.unblended_cost.%s.%s", period, k),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}

		if err := PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
			return fmt.Errorf("post service metric values: %v", err)
		}
	}

	// ri covering cost
	for _, p := range []string{"1d", "1m"} {
		covering, err := CreateRICoveringCost(p, e.BucketName, e.Region)
		if err != nil {
			return fmt.Errorf("create ri covering ondemand cost metrc values: %v", err)
		}

		values := make([]*mackerel.MetricValue, 0)
		for k, v := range covering {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.ri_covering_cost.%s.%s", period, k),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}

		if err := PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
			return fmt.Errorf("post service metric values: %v", err)
		}
	}

	// unblended cost + ri covering cost
	for _, p := range []string{"1d", "1m"} {
		unblended, err := CreateUnblendedCost(p, e.BucketName, e.IgnoreRecordType)
		if err != nil {
			return fmt.Errorf("create unbelnded cost metrc values: %v", err)
		}

		covering, err := CreateRICoveringCost(p, e.BucketName, e.Region)
		if err != nil {
			return fmt.Errorf("create ri covering ondemand cost metrc values: %v", err)
		}

		total := make(map[string]float64)
		for k, v := range unblended {
			for kk, vv := range covering {
				if k == kk {
					total[k] = v + vv
					break
				}
			}
		}

		values := make([]*mackerel.MetricValue, 0)
		for k, v := range total {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.rebate_cost.%s.%s", period, k),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}

		if err := PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
			return fmt.Errorf("post service metric values: %v", err)
		}
	}

	return nil
}

func CreateRICoveringCost(period, bucketName string, region []string) (map[string]float64, error) {
	out := make(map[string]float64)

	date, err := calendar.Last(period)
	if err != nil {
		return out, fmt.Errorf("get last period=%s: %v", period, err)
	}

	s3, err := infrastructure.NewStorage()
	if err != nil {
		return out, fmt.Errorf("new storage: %v", err)
	}

	price := make([]pricing.Price, 0)
	for _, r := range region {
		b, err := s3.Read(bucketName, fmt.Sprintf("pricing/%s.json", r))
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}

		var p []pricing.Price
		if err := json.Unmarshal(b, &p); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		price = append(price, p...)
	}

	for _, d := range date {
		b, err := s3.Read(bucketName, fmt.Sprintf("reservation/%s.json", d.String()))
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}

		var util []reservation.Utilization
		if err := json.Unmarshal(b, &util); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		for _, e := range reservation.AddCoveringCost(price, util) {
			fmt.Printf("[WARN] %s\n", e)
		}

		for _, u := range util {
			v, ok := out[u.Description]
			if !ok {
				out[u.Description] = u.CoveringCost
				continue
			}

			out[u.Description] = v + u.CoveringCost
		}
	}

	return out, nil
}

func CreateUnblendedCost(period, bucketName string, ignoreRecordType []string) (map[string]float64, error) {
	out := make(map[string]float64)

	date, err := calendar.Last(period)
	if err != nil {
		return out, fmt.Errorf("calendar.Last period=%s: %v", "1d", err)
	}

	s3, err := infrastructure.NewStorage()
	if err != nil {
		return out, fmt.Errorf("new storage: %v", err)
	}

	for _, d := range date {
		b, err := s3.Read(bucketName, fmt.Sprintf("cost/%s.json", d.String()))
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}

		var cost []cost.AccountCost
		if err := json.Unmarshal(b, &cost); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		for _, c := range cost {
			ignore := false
			for _, i := range ignoreRecordType {
				if c.RecordType == i {
					ignore = true
					break
				}
			}

			if ignore {
				continue
			}

			a, err := strconv.ParseFloat(c.UnblendedCost.Amount, 64)
			if err != nil {
				return out, fmt.Errorf("parse float: %v", err)
			}

			v, ok := out[c.Description]
			if !ok {
				out[c.Description] = a
				continue
			}
			out[c.Description] = v + a
		}
	}

	return out, nil
}

func PostServiceMetricValues(apikey, service string, values []*mackerel.MetricValue) error {
	client := mackerel.NewClient(apikey)
	if err := client.PostServiceMetricValues(service, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}

func Download(bucketName string, region []string, date []calendar.Date) error {
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
		file := fmt.Sprintf("reservation/%s.json", date[i].String())

		exists, err := s3.Exists(bucketName, file)
		if err != nil {
			return fmt.Errorf("exists: %v", err)
		}

		if exists {
			continue
		}

		ac, err := reservation.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch reservation (%s, %s): %v\n", date[i].Start, date[i].End, err)
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
