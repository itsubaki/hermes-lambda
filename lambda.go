package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/mackerelio/mackerel-client-go"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
	dhandler "github.com/itsubaki/hermes-lambda/pkg/infrastructure/dataset"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/environ"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/handler"
	shandler "github.com/itsubaki/hermes-lambda/pkg/infrastructure/storage"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
	"github.com/itsubaki/hermes-lambda/pkg/interface/dataset"
	"github.com/itsubaki/hermes-lambda/pkg/interface/storage"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
	"github.com/itsubaki/hermes/pkg/usage"
)

type HermesLambda struct {
	Time        time.Time
	Env         *environ.Env
	Pricing     *storage.Pricing
	AccountCost *storage.AccountCost
	Utilization *storage.Utilization
}

func Default(e *environ.Env) *HermesLambda {
	return &HermesLambda{
		Time: time.Now(),
		Env:  e,
	}
}

func New(e *environ.Env) (*HermesLambda, error) {
	s3, err := shandler.New()
	if err != nil {
		return nil, fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return nil, fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	return &HermesLambda{
		Time:        time.Now(),
		Env:         e,
		Pricing:     &storage.Pricing{Storage: s3},
		AccountCost: &storage.AccountCost{Storage: s3},
		Utilization: &storage.Utilization{Storage: s3, SuppressWarning: e.SuppressWarning},
	}, nil
}

func (l *HermesLambda) Run() error {
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

func (l *HermesLambda) Fetch() error {
	if err := l.Pricing.Fetch(l.Env.BucketName, l.Env.Region); err != nil {
		return fmt.Errorf("fetch pricing: %v", err)
	}

	if err := l.AccountCost.Fetch(l.Env.Period, l.Env.BucketName); err != nil {
		return fmt.Errorf("fetch account cost: %v", err)
	}

	if err := l.Utilization.Fetch(l.Env.Period, l.Env.BucketName); err != nil {
		return fmt.Errorf("fetch utilization: %v", err)
	}

	return nil
}

func (l *HermesLambda) Put(items []dataset.Items) error {
	ds, err := dhandler.New(l.Env.DataSetName, l.Env.Credential)
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
			total[key] = uu.CoveringCost
			continue
		}

		total[key] = v + uu.CoveringCost
	}

	items := make([]*dataset.UtilizationRow, 0)
	for _, uu := range u {
		date, err := civil.ParseDate(uu.Date)
		if err != nil {
			return dataset.Items{}, fmt.Errorf("parse date: %v", err)
		}

		items = append(items, &dataset.UtilizationRow{
			Timestamp:              l.Time,
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

	c, err := l.Utilization.CoveringCost(period, l.Env.BucketName, l.Env.Region)
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
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range c {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.ri_covering_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  l.Time.Unix(),
			Value: v,
		})
	}

	for k, v := range total {
		values = append(values, &mackerel.MetricValue{
			Name:  fmt.Sprintf("aws.%s.rebate_cost.%s", period, strings.Replace(k, " ", "", -1)),
			Time:  l.Time.Unix(),
			Value: v,
		})

	}

	return values, nil
}

func (l *HermesLambda) PostServiceMetricValues(values []*mackerel.MetricValue) error {
	c := mackerel.NewClient(l.Env.MkrAPIKey)
	if err := c.PostServiceMetricValues(l.Env.MkrServiceName, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}

func (l *HermesLambda) Store() error {
	date, err := calendar.Last(l.Env.Period[0])
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", l.Env.Period, err)
	}

	h, err := handler.New(l.Env.Driver, l.Env.DataSource, l.Env.Database)
	if err != nil {
		return fmt.Errorf("new handler: %v", err)
	}
	defer h.Close()

	// pricing
	{
		log.Println("serialize pricing")
		if err := pricing.Serialize(l.Env.Dir, l.Env.Region); err != nil {
			return fmt.Errorf("serialize pricing: %v", err)
		}

		log.Println("deserialize pricing")
		price, err := pricing.Deserialize(l.Env.Dir, l.Env.Region)
		if err != nil {
			return fmt.Errorf("deserialize pricing: %v\n", err)
		}

		log.Println("export pricing to database")
		r := database.NewPricingRepository(h)
		for _, p := range price {
			o := &domain.Pricing{
				Version:                 p.Version,
				SKU:                     p.SKU,
				OfferTermCode:           p.OfferTermCode,
				Region:                  p.Region,
				InstanceType:            p.InstanceType,
				UsageType:               p.UsageType,
				LeaseContractLength:     p.LeaseContractLength,
				PurchaseOption:          p.PurchaseOption,
				OnDemand:                p.OnDemand,
				ReservedQuantity:        p.ReservedQuantity,
				ReservedHrs:             p.ReservedHrs,
				Tenancy:                 p.Tenancy,
				PreInstalled:            p.PreInstalled,
				Operation:               p.Operation,
				OperatingSystem:         p.OperatingSystem,
				CacheEngine:             p.CacheEngine,
				DatabaseEngine:          p.DatabaseEngine,
				OfferingClass:           p.OfferingClass,
				NormalizationSizeFactor: p.NormalizationSizeFactor,
			}

			if err := o.GenID(); err != nil {
				return fmt.Errorf("generate id: %v", err)
			}

			if r.Exists(o.ID) {
				if l.Env.SuppressWarning {
					continue
				}

				log.Printf("[WARN] pricing already exists: %#v", o)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save pricing: %v", err)
			}
		}
	}

	// account cost
	{
		log.Println("serialize account cost")
		if err := cost.Serialize(l.Env.Dir, date); err != nil {
			return fmt.Errorf("serialize cost: %v", err)
		}

		log.Println("deserialize account cost")
		ac, err := cost.Deserialize(l.Env.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize cost: %v", err)
		}

		log.Println("export account cost to database")
		r := database.NewAccountCostRepository(h)
		for _, c := range ac {
			o := &domain.AccountCost{
				AccountID:              c.AccountID,
				Description:            c.Description,
				Date:                   c.Date,
				Service:                c.Service,
				RecordType:             c.RecordType,
				UnblendedCostAmount:    c.UnblendedCost.Amount,
				UnblendedCostUnit:      c.UnblendedCost.Unit,
				BlendedCostAmount:      c.BlendedCost.Amount,
				BlendedCostUnit:        c.BlendedCost.Unit,
				AmortizedCostAmount:    c.AmortizedCost.Amount,
				AmortizedCostUnit:      c.AmortizedCost.Unit,
				NetAmortizedCostAmount: c.NetAmortizedCost.Amount,
				NetAmortizedCostUnit:   c.NetAmortizedCost.Unit,
				NetUnblendedCostAmount: c.NetUnblendedCost.Amount,
				NetUnblendedCostUnit:   c.NetUnblendedCost.Unit,
			}

			if err := o.GenID(); err != nil {
				return fmt.Errorf("generate id: %v", err)
			}

			if r.Exists(o.ID) {
				if l.Env.SuppressWarning {
					continue
				}

				log.Printf("[WARN] account cost already exists: %#v", o)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save account cost: %v", err)
			}
		}
	}

	// usage quantity
	{
		log.Println("serialize usage quantity")
		if err := usage.Serialize(l.Env.Dir, date); err != nil {
			return fmt.Errorf("serialize usage quantity: %v", err)
		}

		log.Println("deserialize usage quantity")
		u, err := usage.Deserialize(l.Env.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize usage quantity: %v", err)
		}

		log.Println("export usage quantity to database")
		r := database.NewUsageQuantityRepository(h)
		for _, q := range u {
			o := &domain.UsageQuantity{
				AccountID:      q.AccountID,
				Description:    q.Description,
				Region:         q.Region,
				UsageType:      q.UsageType,
				Platform:       q.Platform,
				CacheEngine:    q.CacheEngine,
				DatabaseEngine: q.DatabaseEngine,
				Date:           q.Date,
				InstanceHour:   q.InstanceHour,
				InstanceNum:    q.InstanceNum,
				GByte:          q.GByte,
				Requests:       q.Requests,
				Unit:           q.Unit,
			}

			if err := o.GenID(); err != nil {
				return fmt.Errorf("generate id: %v", err)
			}

			if r.Exists(o.ID) {
				if l.Env.SuppressWarning {
					continue
				}

				log.Printf("[WARN] usage quantity already exists: %#v", o)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save usgae quantity: %v", err)
			}
		}
	}

	// reservation utilization
	{
		log.Println("serialize reservation utilization")
		if err := reservation.Serialize(l.Env.Dir, date); err != nil {
			return fmt.Errorf("serialize reservation utilization: %v", err)
		}

		log.Println("deserialize reservation utilization")
		res, err := reservation.Deserialize(l.Env.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize reservation utilization: %v", err)
		}

		log.Println("deserialize pricing")
		plist, err := pricing.Deserialize(l.Env.Dir, l.Env.Region)
		if err != nil {
			return fmt.Errorf("desirialize pricing: %v\n", err)
		}

		log.Println("add covering cost")
		w := reservation.AddCoveringCost(plist, res)
		if !l.Env.SuppressWarning {
			for _, ww := range w {
				log.Printf("[WARN] %s", ww)
			}
		}

		log.Println("export reservation utilization to database")
		r := database.NewUtilizationRepository(h)
		for _, u := range res {
			o := &domain.Utilization{
				AccountID:        u.AccountID,
				Description:      u.Description,
				Region:           u.Region,
				InstanceType:     u.InstanceType,
				Platform:         u.Platform,
				CacheEngine:      u.CacheEngine,
				DatabaseEngine:   u.DatabaseEngine,
				DeploymentOption: u.DeploymentOption,
				Date:             u.Date,
				Hours:            u.Hours,
				Num:              u.Num,
				Percentage:       u.Percentage,
				CoveringCost:     u.CoveringCost,
			}

			if err := o.GenID(); err != nil {
				return fmt.Errorf("generate id: %v", err)
			}

			if r.Exists(o.ID) {
				if l.Env.SuppressWarning {
					continue
				}

				log.Printf("[WARN] reservation utilization already exists: %#v", o)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save reseravtion utilization: %v", err)
			}
		}
	}

	return nil
}
