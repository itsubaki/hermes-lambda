package mackerel

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/itsubaki/hermes-lambda/infrastructure/config"
	"github.com/itsubaki/hermes-lambda/infrastructure/handler"
	"github.com/itsubaki/hermes-lambda/interface/storage"
	"github.com/mackerelio/mackerel-client-go"
)

type MackerelClient struct {
	Time             time.Time
	BucketName       string
	Region           []string
	Period           []string
	IgnoreRecordType []string
	MkrAPIKey        string
	MkrServiceName   string
	SuppressWarning  bool
	Pricing          *storage.Pricing
	AccountCost      *storage.AccountCost
	Utilization      *storage.Utilization
}

func New(c *config.Config) *MackerelClient {
	return &MackerelClient{
		Time:             c.Time,
		BucketName:       c.BucketName,
		Region:           c.Region,
		Period:           c.Period,
		IgnoreRecordType: c.IgnoreRecordType,
		MkrAPIKey:        c.MkrAPIKey,
		MkrServiceName:   c.MkrServiceName,
		SuppressWarning:  c.SuppressWarning,
	}
}

func (c *MackerelClient) NewStorage() error {
	s3, err := handler.NewStorage()
	if err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(c.BucketName); err != nil {
		return fmt.Errorf("create bucket=%s if not exists: %v", c.BucketName, err)
	}

	c.Pricing = &storage.Pricing{Storage: s3}
	c.AccountCost = &storage.AccountCost{Storage: s3}
	c.Utilization = &storage.Utilization{Storage: s3, SuppressWarning: c.SuppressWarning}

	return nil
}

func (c *MackerelClient) Fetch() error {
	if err := c.Pricing.Fetch(c.BucketName, c.Region); err != nil {
		return fmt.Errorf("fetch pricing: %v", err)
	}

	if err := c.AccountCost.Fetch(c.Period, c.BucketName); err != nil {
		return fmt.Errorf("fetch account cost: %v", err)
	}

	if err := c.Utilization.Fetch(c.Period, c.BucketName); err != nil {
		return fmt.Errorf("fetch utilization: %v", err)
	}

	return nil
}

func (c *MackerelClient) MetricValues() ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)
	for _, p := range c.Period {
		v, err := c.MetricValuesWith(p)
		if err != nil {
			return values, err
		}

		values = append(values, v...)
	}

	return values, nil
}

func (c *MackerelClient) MetricValuesWith(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	u, err := c.AccountCost.Unblended(period, c.BucketName, c.IgnoreRecordType, c.Region)
	if err != nil {
		return values, fmt.Errorf("unblended cost: %v", err)
	}

	cc, err := c.Utilization.OnDemandConversionCost(period, c.BucketName, c.Region)
	if err != nil {
		return values, fmt.Errorf("ondemand conversion cost: %v", err)
	}

	total := make(map[string]float64)
	for k, v := range u {
		if vv, ok := cc[k]; ok {
			total[k] = v + vv
			continue
		}

		total[k] = v
	}

	for k, v := range u {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.unblended_cost.%s", period, strings.ReplaceAll(k, " ", "")),
			Time:  c.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range cc {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.ri_ondemand_conversion_cost.%s", period, strings.ReplaceAll(k, " ", "")),
			Time:  c.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range total {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.total_cost.%s", period, strings.ReplaceAll(k, " ", "")),
			Time:  c.Time.Unix(),
			Value: v,
		})
	}

	v, err := c.MetricValuesGroupByServices(period)
	if err != nil {
		return values, fmt.Errorf("metric values group by servies: %v", err)
	}
	values = append(values, v...)

	return values, nil
}

func (c *MackerelClient) MetricValuesGroupByServices(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	read, err := c.AccountCost.Read(period, c.BucketName)
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

	replacer := strings.NewReplacer(" ", "", "-", "", "AWS", "", "Amazon", "", "(", "", ")", "")
	for d, s := range services {
		for n, v := range s {
			values = append(values, &mackerel.MetricValue{
				Name:  fmt.Sprintf("aws.%s.unblended_cost_%s.%s", period, strings.ReplaceAll(d, " ", ""), replacer.Replace(n)),
				Time:  c.Time.Unix(),
				Value: v,
			})
		}
	}

	return values, nil
}

func (c *MackerelClient) Write() error {
	if err := c.NewStorage(); err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := c.Fetch(); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	values, err := c.MetricValues()
	if err != nil {
		return fmt.Errorf("metric values: %v", err)
	}

	mkr := mackerel.NewClient(c.MkrAPIKey)
	if err := mkr.PostServiceMetricValues(c.MkrServiceName, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}
