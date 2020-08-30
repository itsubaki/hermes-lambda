package lambda

import (
	"fmt"
	"strings"

	mackerel "github.com/mackerelio/mackerel-client-go"
)

func (l *HermesLambda) PostServiceMetricValues(values []*mackerel.MetricValue) error {
	c := mackerel.NewClient(l.Env.MkrAPIKey)
	if err := c.PostServiceMetricValues(l.Env.MkrServiceName, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}

func (l *HermesLambda) MetricValues() ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	for _, p := range l.Env.Period {
		v, err := l.MetricValuesWith(p)
		if err != nil {
			return values, err
		}

		values = append(values, v...)
	}

	return values, nil
}

func (l *HermesLambda) MetricValuesWith(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	u, err := l.AccountCost.UnblendedCost(period, l.Env.BucketName, l.Env.IgnoreRecordType, l.Env.Region)
	if err != nil {
		return values, fmt.Errorf("unblended cost: %v", err)
	}

	c, err := l.Utilization.OnDemandConversionCost(period, l.Env.BucketName, l.Env.Region)
	if err != nil {
		return values, fmt.Errorf("ondemand conversion cost: %v", err)
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
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range c {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.ri_ondemand_conversion_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range total {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.total_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  l.Time.Unix(),
			Value: v,
		})

	}

	return values, nil
}
