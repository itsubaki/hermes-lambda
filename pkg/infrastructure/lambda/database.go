package lambda

import (
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/handler"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
	"github.com/itsubaki/hermes/pkg/usage"
)

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

		log.Println("add ondemand conversion cost")
		w := reservation.AddOnDemandConversionCost(plist, res)
		if !l.Env.SuppressWarning {
			for _, ww := range w {
				log.Printf("[WARN] %s", ww)
			}
		}

		log.Println("export reservation utilization to database")
		r := database.NewUtilizationRepository(h)
		for _, u := range res {
			o := &domain.Utilization{
				AccountID:              u.AccountID,
				Description:            u.Description,
				Region:                 u.Region,
				InstanceType:           u.InstanceType,
				Platform:               u.Platform,
				CacheEngine:            u.CacheEngine,
				DatabaseEngine:         u.DatabaseEngine,
				DeploymentOption:       u.DeploymentOption,
				Date:                   u.Date,
				Hours:                  u.Hours,
				Num:                    u.Num,
				Percentage:             u.Percentage,
				OnDemandConversionCost: u.OnDemandConversionCost,
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
