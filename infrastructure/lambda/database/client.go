package database

import (
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/domain"
	"github.com/itsubaki/hermes-lambda/infrastructure/config"
	"github.com/itsubaki/hermes-lambda/infrastructure/handler"
	"github.com/itsubaki/hermes-lambda/interface/database"
	"github.com/itsubaki/hermes/calendar"
	"github.com/itsubaki/hermes/cost"
	"github.com/itsubaki/hermes/pricing"
	"github.com/itsubaki/hermes/reservation"
	"github.com/itsubaki/hermes/usage"
)

type DBClient struct {
	Period          []string
	Driver          string
	DataSource      string
	Database        string
	Dir             string
	Region          []string
	SuppressWarning bool
}

func New(c *config.Config) *DBClient {
	return &DBClient{
		Period:          c.Period,
		Driver:          c.Driver,
		DataSource:      c.DataSource,
		Database:        c.Database,
		Dir:             c.Dir,
		Region:          c.Region,
		SuppressWarning: c.SuppressWarning,
	}
}

func (c *DBClient) Write() error {
	date, err := calendar.Last(c.Period[0])
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", c.Period, err)
	}

	h, err := handler.New(c.Driver, c.DataSource, c.Database)
	if err != nil {
		return fmt.Errorf("new handler: %v", err)
	}
	defer h.Close()

	// pricing
	{
		log.Println("serialize pricing")
		if err := pricing.Serialize(c.Dir, c.Region); err != nil {
			return fmt.Errorf("serialize pricing: %v", err)
		}

		log.Println("deserialize pricing")
		price, err := pricing.Deserialize(c.Dir, c.Region)
		if err != nil {
			return fmt.Errorf("deserialize pricing: %v", err)
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
				if c.SuppressWarning {
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
		if err := cost.Serialize(c.Dir, date, []string{"NetAmortizedCost", "NetUnblendedCost", "UnblendedCost", "AmortizedCost", "BlendedCost"}); err != nil {
			return fmt.Errorf("serialize cost: %v", err)
		}

		log.Println("deserialize account cost")
		ac, err := cost.Deserialize(c.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize cost: %v", err)
		}

		log.Println("export account cost to database")
		r := database.NewAccountCostRepository(h)
		for _, cc := range ac {
			o := &domain.AccountCost{
				AccountID:              cc.AccountID,
				Description:            cc.Description,
				Date:                   cc.Date,
				Service:                cc.Service,
				RecordType:             cc.RecordType,
				UnblendedCostAmount:    cc.UnblendedCost.Amount,
				UnblendedCostUnit:      cc.UnblendedCost.Unit,
				BlendedCostAmount:      cc.BlendedCost.Amount,
				BlendedCostUnit:        cc.BlendedCost.Unit,
				AmortizedCostAmount:    cc.AmortizedCost.Amount,
				AmortizedCostUnit:      cc.AmortizedCost.Unit,
				NetAmortizedCostAmount: cc.NetAmortizedCost.Amount,
				NetAmortizedCostUnit:   cc.NetAmortizedCost.Unit,
				NetUnblendedCostAmount: cc.NetUnblendedCost.Amount,
				NetUnblendedCostUnit:   cc.NetUnblendedCost.Unit,
			}

			if err := o.GenID(); err != nil {
				return fmt.Errorf("generate id: %v", err)
			}

			if r.Exists(o.ID) {
				if c.SuppressWarning {
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
		if err := usage.Serialize(c.Dir, date); err != nil {
			return fmt.Errorf("serialize usage quantity: %v", err)
		}

		log.Println("deserialize usage quantity")
		u, err := usage.Deserialize(c.Dir, date)
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
				if c.SuppressWarning {
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
		if err := reservation.Serialize(c.Dir, date); err != nil {
			return fmt.Errorf("serialize reservation utilization: %v", err)
		}

		log.Println("deserialize reservation utilization")
		res, err := reservation.Deserialize(c.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize reservation utilization: %v", err)
		}

		log.Println("deserialize pricing")
		plist, err := pricing.Deserialize(c.Dir, c.Region)
		if err != nil {
			return fmt.Errorf("desirialize pricing: %v", err)
		}

		log.Println("add ondemand conversion cost")
		w := reservation.AddOnDemandConversionCost(plist, res)
		if !c.SuppressWarning {
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
				if c.SuppressWarning {
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
