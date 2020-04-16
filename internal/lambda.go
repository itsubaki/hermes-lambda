package internal

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"

	"cloud.google.com/go/bigquery"

	"github.com/itsubaki/hermes-lambda/internal/dataset"

	"github.com/itsubaki/hermes-lambda/internal/storage"

	"github.com/mackerelio/mackerel-client-go"
)

type HermesLambda struct {
	Time        time.Time
	Pricing     *storage.Pricing
	AccountCost *storage.AccountCost
	Utilization *storage.Utilization
	DataSet     *dataset.DataSet
	Env         *Env
}

func New(e *Env) (*HermesLambda, error) {
	s3, err := storage.New()
	if err != nil {
		return nil, fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return nil, fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	ds, err := dataset.New(e.DataSetName, e.Credential)
	if err != nil {
		return nil, fmt.Errorf("new dataset: %v", err)
	}

	return &HermesLambda{
		Time:        time.Now(),
		Pricing:     &storage.Pricing{Storage: s3},
		AccountCost: &storage.AccountCost{Storage: s3},
		Utilization: &storage.Utilization{Storage: s3, SuppressWarning: e.SuppressWarning},
		DataSet:     ds,
		Env:         e,
	}, nil
}

func (h *HermesLambda) Close() error {
	return h.DataSet.Close()
}

func (h *HermesLambda) Put(items []dataset.Items) error {
	for _, i := range items {
		if err := h.DataSet.CreateIfNotExists(i.TableMetadata); err != nil {
			return fmt.Errorf("create table=%#v: %v", i.TableMetadata, err)
		}

		if err := h.put(i.TableMetadata.Name, i.Items); err != nil {
			return fmt.Errorf("put=%#v: %v", i.TableMetadata, err)
		}
	}

	return nil
}

func (h *HermesLambda) put(table string, items interface{}) error {
	return h.DataSet.Put(table, items)
}

func (h *HermesLambda) Items() ([]dataset.Items, error) {
	out := make([]dataset.Items, 0)

	for _, p := range h.Env.Period {
		items, err := h.AccountCostItems(p)
		if err != nil {
			return out, fmt.Errorf("account cost items: %v", err)
		}
		out = append(out, items)
	}

	for _, p := range h.Env.Period {
		items, err := h.UtilizationItems(p)
		if err != nil {
			return out, fmt.Errorf("utilization items: %v", err)
		}
		out = append(out, items)
	}

	return out, nil
}

func (h *HermesLambda) AccountCostItems(p string) (dataset.Items, error) {
	c, err := h.AccountCost.Read(p, h.Env.BucketName)
	if err != nil {
		return dataset.Items{}, fmt.Errorf("read: %v", err)
	}

	items := make([]*dataset.AccountCostRow, 0)
	for _, cc := range c {
		u, err := strconv.ParseFloat(cc.UnblendedCost.Amount, 64)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse float: %v", err)
		}
		b, err := strconv.ParseFloat(cc.BlendedCost.Amount, 64)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse float: %v", err)
		}
		a, err := strconv.ParseFloat(cc.AmortizedCost.Amount, 64)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse float: %v", err)
		}
		na, err := strconv.ParseFloat(cc.NetAmortizedCost.Amount, 64)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse float: %v", err)
		}
		nu, err := strconv.ParseFloat(cc.NetUnblendedCost.Amount, 64)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse float: %v", err)
		}
		date, err := civil.ParseDate(cc.Date)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse date: %v", err)
		}

		items = append(items, &dataset.AccountCostRow{
			Timestamp:        h.Time,
			AccountID:        cc.AccountID,
			Description:      cc.Description,
			Date:             date,
			Service:          cc.Service,
			RecordType:       cc.RecordType,
			UnblendedCost:    u,
			BlendedCost:      b,
			AmortizedCost:    a,
			NetAmortizedCost: na,
			NetUnblendedCost: nu,
		})
	}

	out := dataset.Items{
		TableMetadata: bigquery.TableMetadata{
			Name:   fmt.Sprintf("%s_account_cost", p),
			Schema: dataset.AccountCostSchema,
			TimePartitioning: &bigquery.TimePartitioning{
				Field: "date",
			},
		},
		Items: items,
	}

	return out, nil
}

func (h *HermesLambda) UtilizationItems(p string) (dataset.Items, error) {
	u, err := h.Utilization.Read(p, h.Env.BucketName, h.Env.Region)
	if err != nil {
		return dataset.Items{}, fmt.Errorf("read: %v", err)
	}

	total := make(map[string]float64)
	for _, uu := range u {
		v, ok := total[uu.Region]
		if !ok {
			total[uu.Region] = uu.CoveringCost
			continue
		}

		total[uu.Region] = v + uu.CoveringCost
	}

	items := make([]*dataset.UtilizationRow, 0)
	for _, uu := range u {
		date, err := civil.ParseDate(uu.Date)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse date: %v", err)
		}

		items = append(items, &dataset.UtilizationRow{
			Timestamp:              h.Time,
			AccountID:              uu.AccountID,
			Description:            uu.Description,
			Region:                 uu.Region,
			InstanceType:           uu.InstanceType,
			Platform:               uu.Platform,
			CacheEngine:            uu.CacheEngine,
			DatabaseEngine:         uu.DatabaseEngine,
			DeploymentOption:       uu.DeploymentOption,
			Date:                   date,
			Hours:                  uu.Hours,
			Num:                    uu.Num,
			Percentage:             uu.Percentage,
			CoveringCost:           uu.CoveringCost,
			CoveringCostPercentage: uu.CoveringCost / total[uu.Region] * 100,
		})
	}

	out := dataset.Items{
		TableMetadata: bigquery.TableMetadata{
			Name:   fmt.Sprintf("%s_utilization", p),
			Schema: dataset.UtilizationSchema,
			TimePartitioning: &bigquery.TimePartitioning{
				Field: "date",
			},
		},
		Items: items,
	}

	return out, nil
}

func (h *HermesLambda) Fetch() error {
	if err := h.Pricing.Fetch(h.Env.BucketName, h.Env.Region); err != nil {
		return fmt.Errorf("fetch pricing: %v", err)
	}

	if err := h.AccountCost.Fetch(h.Env.Period, h.Env.BucketName); err != nil {
		return fmt.Errorf("fetch account cost: %v", err)
	}

	if err := h.Utilization.Fetch(h.Env.Period, h.Env.BucketName); err != nil {
		return fmt.Errorf("fetch utilization: %v", err)
	}

	return nil
}

func (h *HermesLambda) MetricValues() ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	for _, p := range h.Env.Period {
		v, err := h.metricValues(p)
		if err != nil {
			return values, err
		}

		values = append(values, v...)
	}

	return values, nil
}

func (h *HermesLambda) metricValues(period string) ([]*mackerel.MetricValue, error) {
	values := make([]*mackerel.MetricValue, 0)

	u, err := h.AccountCost.UnblendedCost(period, h.Env.BucketName, h.Env.IgnoreRecordType, h.Env.Region)
	if err != nil {
		return values, fmt.Errorf("unblended cost: %v", err)
	}

	c, err := h.Utilization.CoveringCost(period, h.Env.BucketName, h.Env.Region)
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
			Time:  h.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range c {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.ri_covering_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  h.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range total {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.rebate_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  h.Time.Unix(),
			Value: v,
		})

	}

	return values, nil
}

func (h *HermesLambda) PostServiceMetricValues(values []*mackerel.MetricValue) error {
	client := mackerel.NewClient(h.Env.MackerelAPIKey)
	if err := client.PostServiceMetricValues(h.Env.MackerelServiceName, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}
