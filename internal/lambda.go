package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/mackerelio/mackerel-client-go"
)

type HermesLambda struct {
	Pricing          *Pricing
	AccountCost      *AccountCost
	Utilization      *Utilization
	BucketName       string
	Period           []string
	IgnoreRecordType []string
	Region           []string
}

func New(e *Env) (*HermesLambda, error) {
	s3, err := NewStorage()
	if err != nil {
		return nil, fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return nil, fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	return &HermesLambda{
		Pricing:          &Pricing{Storage: s3},
		AccountCost:      &AccountCost{Storage: s3},
		Utilization:      &Utilization{Storage: s3},
		BucketName:       e.BucketName,
		Period:           e.Period,
		IgnoreRecordType: e.IgnoreRecordType,
		Region:           e.Region,
	}, nil
}

func (h *HermesLambda) Fetch() ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	if err := h.Pricing.Fetch(h.BucketName, h.Region); err != nil {
		return values, fmt.Errorf("fetch pricing: %v", err)
	}

	if err := h.AccountCost.Fetch(h.Period, h.BucketName); err != nil {
		return values, fmt.Errorf("fetch account cost: %v", err)
	}

	if err := h.Utilization.Fetch(h.Period, h.BucketName); err != nil {
		return values, fmt.Errorf("fetch utilization: %v", err)
	}

	for _, p := range h.Period {
		v, err := h.MetricValues(p)
		if err != nil {
			return values, err
		}

		values = append(values, v...)
	}

	return values, nil
}

func (h *HermesLambda) MetricValues(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	u, err := h.AccountCost.UnblendedCost(period, h.BucketName, h.IgnoreRecordType, h.Region)
	if err != nil {
		return values, fmt.Errorf("unblended cost: %v", err)
	}

	c, err := h.Utilization.CoveringCost(period, h.BucketName, h.Region)
	if err != nil {
		return values, fmt.Errorf("covering cost: %v", err)
	}

	total := make(map[string]float64)
	for k, v := range u {
		vv, ok := c[k]
		if !ok {
			continue
		}

		total[k] = v + vv
	}

	for k, v := range u {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.unblended_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  time.Now().Unix(),
			Value: v,
		})
	}

	for k, v := range c {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.ri_covering_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  time.Now().Unix(),
			Value: v,
		})
	}

	for k, v := range total {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.rebate_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  time.Now().Unix(),
			Value: v,
		})

	}

	return values, nil
}

func PostServiceMetricValues(apiKey, service string, values []*mackerel.MetricValue) error {
	client := mackerel.NewClient(apiKey)
	if err := client.PostServiceMetricValues(service, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}
