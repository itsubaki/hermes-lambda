package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
	"github.com/itsubaki/hermes/pkg/usage"
)

func handle(ctx context.Context) error {
	e := infrastructure.NewEnviron()

	date, err := calendar.Last(e.Period)
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", e.Period, err)
	}

	h := infrastructure.NewHandler(e.Driver, e.DataSource, e.Database)
	defer h.Close()

	// pricing
	{
		log.Println("start: serialize pricing")
		if err := pricing.Serialize(e.Dir, e.Region); err != nil {
			return fmt.Errorf("serialize pricing: %v", err)
		}

		log.Println("start: deserialize pricing")
		price, err := pricing.Deserialize(e.Dir, e.Region)
		if err != nil {
			return fmt.Errorf("deserialize pricing: %v\n", err)
		}

		log.Println("start: export pricing to database")
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

			sha := sha256.Sum256([]byte(o.JSON()))
			o.ID = hex.EncodeToString(sha[:])

			if r.Exists(o.ID) {
				log.Printf("pricing already exists: %v", o.ID)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save pricing: %v", err)
			}
		}
	}

	// account cost
	{
		log.Println("start: serialize account cost")
		if err := cost.Serialize(e.Dir, date); err != nil {
			return fmt.Errorf("serialize cost: %v", err)
		}

		log.Println("start: deserialize account cost")
		ac, err := cost.Deserialize(e.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize cost: %v", err)
		}

		log.Println("start: export account cost to database")
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

			sha := sha256.Sum256([]byte(o.JSON()))
			o.ID = hex.EncodeToString(sha[:])

			if r.Exists(o.ID) {
				log.Printf("account cost already exists: %v", o.ID)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save account cost: %v", err)
			}
		}
	}

	// usage
	{
		log.Println("start: serialize usage")
		if err := usage.Serialize(e.Dir, date); err != nil {
			return fmt.Errorf("serialize cost: %v", err)
		}

		log.Println("start: deserialize usage")
		u, err := usage.Deserialize(e.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize cost: %v", err)
		}

		log.Println("start: export usage to database")
		r := database.NewUsageRepository(h)
		for _, uu := range u {
			o := &domain.UsageQuantity{
				AccountID:      uu.AccountID,
				Description:    uu.Description,
				Region:         uu.Region,
				UsageType:      uu.UsageType,
				Platform:       uu.Platform,
				CacheEngine:    uu.CacheEngine,
				DatabaseEngine: uu.DatabaseEngine,
				Date:           uu.Date,
				InstanceHour:   uu.InstanceHour,
				InstanceNum:    uu.InstanceNum,
				GByte:          uu.GByte,
				Requests:       uu.Requests,
				Unit:           uu.Unit,
			}

			sha := sha256.Sum256([]byte(o.JSON()))
			o.ID = hex.EncodeToString(sha[:])

			if r.Exists(o.ID) {
				log.Printf("usage already exists: %v", o.ID)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save usgae: %v", err)
			}
		}
	}

	// reservation utilization
	{
		log.Println("start: serialize reservation utilization")
		if err := reservation.Serialize(e.Dir, date); err != nil {
			return fmt.Errorf("serialize reservation utilization: %v", err)
		}

		log.Println("start: deserialize reservation utilization")
		res, err := reservation.Deserialize(e.Dir, date)
		if err != nil {
			return fmt.Errorf("deserialize reservation utilization: %v", err)
		}

		log.Println("start: deserialize pricing")
		plist, err := pricing.Deserialize(e.Dir, e.Region)
		if err != nil {
			return fmt.Errorf("desirialize pricing: %v\n", err)
		}

		log.Println("start: add covering cost")
		for _, w := range reservation.AddCoveringCost(plist, res) {
			log.Println(w)
		}

		log.Println("start: export reservation utilization to database")
		r := database.NewUtilizationRepository(h)
		for _, rr := range res {
			o := &domain.Utilization{
				AccountID:        rr.AccountID,
				Description:      rr.Description,
				Region:           rr.Region,
				InstanceType:     rr.InstanceType,
				Platform:         rr.Platform,
				CacheEngine:      rr.CacheEngine,
				DatabaseEngine:   rr.DatabaseEngine,
				DeploymentOption: rr.DeploymentOption,
				Date:             rr.Date,
				Hours:            rr.Hours,
				Num:              rr.Num,
				Percentage:       rr.Percentage,
				CoveringCost:     rr.CoveringCost,
			}

			sha := sha256.Sum256([]byte(o.JSON()))
			o.ID = hex.EncodeToString(sha[:])

			if r.Exists(o.ID) {
				log.Printf("reseravtion utilization already exists: %v", o.ID)
				continue
			}

			if _, err := r.Save(o); err != nil {
				return fmt.Errorf("save reseravtion utilization: %v", err)
			}
		}
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(handle)
	log.Println("finished")
}
