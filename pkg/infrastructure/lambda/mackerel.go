package lambda

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	mackerel "github.com/mackerelio/mackerel-client-go"
)

func (l *HermesLambda) PostServiceMetricValues() error {
	if err := l.NewStorage(); err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := l.Fetch(); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	values, err := l.MetricValues()
	if err != nil {
		return fmt.Errorf("metric values: %v", err)
	}

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

	u, err := l.AccountCost.Unblended(period, l.Env.BucketName, l.Env.IgnoreRecordType, l.Env.Region)
	if err != nil {
		return values, fmt.Errorf("unblended cost: %v", err)
	}

	c, err := l.Utilization.OnDemandConversionCost(period, l.Env.BucketName, l.Env.Region)
	if err != nil {
		return values, fmt.Errorf("ondemand conversion cost: %v", err)
	}

	total := make(map[string]float64)
	for k, v := range u {
		if vv, ok := c[k]; ok {
			total[k] = v + vv
			continue
		}

		total[k] = v
	}

	for k, v := range u {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.unblended_cost.%s", period, strings.ReplaceAll(k, " ", "")),
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range c {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.ri_ondemand_conversion_cost.%s", period, strings.ReplaceAll(k, " ", "")),
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range total {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.total_cost.%s", period, strings.ReplaceAll(k, " ", "")),
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	return values, nil
}

func (l *HermesLambda) MetricValuesGroupByServices(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	read, err := l.AccountCost.Read(period, l.Env.BucketName)
	if err != nil {
		return values, fmt.Errorf("read: %v", err)
	}

	services := make(map[string]map[string]float64)
	for _, c := range read {
		a, err := strconv.ParseFloat(c.UnblendedCost.Amount, 64)
		if err != nil {
			return values, fmt.Errorf("parse float: %v", err)
		}

		if v, ok := services[c.Description]; ok {
			if vv, ok2 := v[c.Service]; ok2 {
				services[c.Description][c.Service] = vv + a
				continue
			}

			v[c.Service] = a
			continue
		}

		services[c.Description] = make(map[string]float64)
		services[c.Description][c.Service] = a
	}

	for d, s := range services {
		desc := strings.ReplaceAll(d, " ", "")

		for n, v := range s {
			name := strings.ReplaceAll(strings.ReplaceAll(n, " ", "_"), "-", "")
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.unblended_cost_%s.%s", period, desc, name),
				Time:  l.Time.Unix(),
				Value: v,
			})

			log.Printf("%s %s", desc, name)
		}
	}

	return values, nil
}
