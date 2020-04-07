package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/mackerelio/mackerel-client-go"
)

type Metric struct {
	AccountCost      *AccountCost
	Utilization      *Utilization
	BucketName       string
	Period           []string
	IgnoreRecordType []string
	Region           []string
}

func (m *Metric) MetricValues() ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)
	for _, p := range m.Period {
		v, err := m.metricValues(p)
		if err != nil {
			return values, err
		}

		values = append(values, v...)
	}

	return values, nil
}

func (m *Metric) metricValues(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	u, err := m.AccountCost.UnblendedCost(period, m.BucketName, m.IgnoreRecordType, m.Region)
	if err != nil {
		return values, fmt.Errorf("unblended cost: %v", err)
	}

	c, err := m.Utilization.CoveringCost(period, m.BucketName, m.Region)
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
