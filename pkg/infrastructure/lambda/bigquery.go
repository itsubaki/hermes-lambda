package lambda

import (
	"fmt"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/handler"
	"github.com/itsubaki/hermes-lambda/pkg/interface/dataset"
)

func (l *HermesLambda) PutItems() error {
	if err := l.NewStorage(); err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := l.Fetch(); err != nil {
		return fmt.Errorf("fetch: %v", err)
	}

	items, err := l.Items()
	if err != nil {
		return fmt.Errorf("items: %v", err)
	}

	if err := l.Put(items); err != nil {
		return fmt.Errorf("put: %v", err)
	}

	return nil
}

func (l *HermesLambda) Items() ([]dataset.Items, error) {
	out := make([]dataset.Items, 0)

	for _, p := range l.Env.Period {
		items, err := l.AccountCostItems(p)
		if err != nil {
			return out, fmt.Errorf("account cost items: %v", err)
		}
		out = append(out, items)
	}

	for _, p := range l.Env.Period {
		items, err := l.UtilizationItems(p)
		if err != nil {
			return out, fmt.Errorf("utilization items: %v", err)
		}
		out = append(out, items)
	}

	return out, nil
}

func (l *HermesLambda) Put(items []dataset.Items) error {
	ds, err := handler.NewDataSet(l.Env.DataSetName, l.Env.Credential)
	if err != nil {
		return fmt.Errorf("new dataset: %v", err)
	}
	defer ds.Close()

	for _, i := range items {
		if err := ds.CreateIfNotExists(i.TableMetadata); err != nil {
			return fmt.Errorf("create table=%#v: %v", i.TableMetadata, err)
		}

		if err := ds.Put(i.TableMetadata.Name, i.Items); err != nil {
			return fmt.Errorf("put=%#v: %v", i.TableMetadata, err)
		}
	}

	return nil
}

func (l *HermesLambda) AccountCostItems(p string) (dataset.Items, error) {
	c, err := l.AccountCost.Read(p, l.Env.BucketName)
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
			Timestamp:        l.Time,
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

func (l *HermesLambda) UtilizationItems(p string) (dataset.Items, error) {
	u, err := l.Utilization.Read(p, l.Env.BucketName, l.Env.Region)
	if err != nil {
		return dataset.Items{}, fmt.Errorf("read: %v", err)
	}

	total := make(map[string]float64)
	for _, uu := range u {
		key := fmt.Sprintf("%s_%s", uu.Region, uu.Date)
		v, ok := total[key]
		if !ok {
			total[key] = uu.OnDemandConversionCost
			continue
		}

		total[key] = v + uu.OnDemandConversionCost
	}

	items := make([]*dataset.UtilizationRow, 0)
	for _, uu := range u {
		date, err := civil.ParseDate(uu.Date)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse date: %v", err)
		}

		items = append(items, &dataset.UtilizationRow{
			Timestamp:                        l.Time,
			AccountID:                        uu.AccountID,
			Description:                      uu.Description,
			Region:                           uu.Region,
			InstanceType:                     uu.InstanceType,
			Platform:                         uu.Platform,
			CacheEngine:                      uu.CacheEngine,
			DatabaseEngine:                   uu.DatabaseEngine,
			DeploymentOption:                 uu.DeploymentOption,
			Date:                             date,
			Hours:                            uu.Hours,
			Num:                              uu.Num,
			Percentage:                       uu.Percentage,
			OnDemandConversionCost:           uu.OnDemandConversionCost,
			OnDemandConversionCostPercentage: uu.OnDemandConversionCost / total[uu.Region] * 100,
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
