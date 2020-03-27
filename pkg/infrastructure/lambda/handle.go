package lambda

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	mackerel "github.com/mackerelio/mackerel-client-go"
)

func Handle(ctx context.Context) error {
	e := infrastructure.NewEnv()
	log.Printf("env=%#v", e)

	s3, err := infrastructure.NewStorage()
	if err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	p := &Pricing{Storage: s3}
	if err := p.Fetch(e.BucketName, e.Region); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	uc := &UnblendedCost{Storage: s3}
	for _, p := range e.Period {
		if err := uc.Fetch(p, e.BucketName); err != nil {
			return fmt.Errorf("fetch unlbended cost: %v", err)
		}

		a, err := uc.Aggregate(p, e.BucketName, e.IgnoreRecordType, e.Region)
		if err != nil {
			return fmt.Errorf("aggregate unblended cost: %v", err)
		}

		values := make([]*mackerel.MetricValue, 0)
		for k, v := range a {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.unblended_cost.%s", p, strings.Replace(k, " ", "", -1)),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}

		if err := PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
			return fmt.Errorf("post service metric values: %v", err)
		}
	}

	cc := &CoveringCost{Storage: s3, SuppressWarning: true}
	for _, p := range e.Period {
		if err := cc.Fetch(p, e.BucketName); err != nil {
			return fmt.Errorf("fetch covering cost: %v", err)
		}

		a, err := cc.Aggregate(p, e.BucketName, e.Region)
		if err != nil {
			return fmt.Errorf("aggregate covering cost: %v", err)
		}

		values := make([]*mackerel.MetricValue, 0)
		for k, v := range a {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.ri_covering_cost.%s", p, strings.Replace(k, " ", "", -1)),
				Time:  time.Now().Unix(),
				Value: v,
			})
		}

		if err := PostServiceMetricValues(e.MackerelAPIKey, e.MackerelServiceName, values); err != nil {
			return fmt.Errorf("post service metric values: %v", err)
		}
	}

	for _, p := range e.Period {
		unblended, err := uc.Aggregate(p, e.BucketName, e.IgnoreRecordType, e.Region)
		if err != nil {
			return fmt.Errorf("aggregate unblended cost: %v", err)
		}

		covering, err := cc.Aggregate(p, e.BucketName, e.Region)
		if err != nil {
			return fmt.Errorf("aggregate covering cost: %v", err)
		}

		total := Add(unblended, covering)

		values := make([]*mackerel.MetricValue, 0)
		for k, v := range total {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.rebate_cost.%s", p, strings.Replace(k, " ", "", -1)),
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

func Add(u, c map[string]float64) map[string]float64 {
	total := make(map[string]float64)

	for k, v := range u {
		vv, ok := c[k]
		if !ok {
			continue
		}

		total[k] = v + vv
	}

	return total
}

func PostServiceMetricValues(apikey, service string, values []*mackerel.MetricValue) error {
	client := mackerel.NewClient(apikey)
	if err := client.PostServiceMetricValues(service, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}
