package database

import (
	"fmt"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
)

type PricingRepository struct {
	Handler
}

func NewPricingRepository(h Handler) *PricingRepository {
	if err := h.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`
			create table if not exists pricing (
				id							varchar(64) not null primary key,
				version						varchar(64) not null,
				sku							varchar(64) not null,
				offer_term_code				varchar(64)	not null,
				region						varchar(64) not null,
				instance_type				varchar(64) not null,
				usage_type					varchar(64) not null,
				lease_contract_length		varchar(3)	not null,
				purchase_option				varchar(64)	not null,
				ondemand					double,
				reserved_quantity			double,
				reserved_hours				double,
				tenancy						varchar(64),
				pre_installed				varchar(64),
				operation					varchar(64),
				operating_system			varchar(64),
				cache_engine				varchar(64),
				database_engine				varchar(64),
				offering_class				varchar(64),
				normalization_size_factor	varchar(64),
				index(lease_contract_length),
				index(purchase_option)
			)
			`,
		); err != nil {
			return fmt.Errorf("create table pricing: %v", err)
		}

		return nil
	}); err != nil {
		panic(fmt.Errorf("transaction: %v", err))
	}

	return &PricingRepository{
		Handler: h,
	}
}

func (r *PricingRepository) List() ([]domain.Pricing, error) {
	price := make([]domain.Pricing, 0)
	if err := r.Transact(func(tx Tx) error {
		rows, err := tx.Query("select * from pricing")
		if err != nil {
			return fmt.Errorf("select * from pricnig: %v", err)
		}
		defer rows.Close()

		var p domain.Pricing
		for rows.Next() {
			if err := rows.Scan(
				&p.ID,
				&p.Version,
				&p.SKU,
				&p.OfferTermCode,
				&p.Region,
				&p.InstanceType,
				&p.UsageType,
				&p.LeaseContractLength,
				&p.PurchaseOption,
				&p.OnDemand,
				&p.ReservedQuantity,
				&p.ReservedHrs,
				&p.Tenancy,
				&p.PreInstalled,
				&p.Operation,
				&p.OperatingSystem,
				&p.CacheEngine,
				&p.DatabaseEngine,
				&p.OfferingClass,
				&p.NormalizationSizeFactor,
			); err != nil {
				return fmt.Errorf("scan: %v", err)
			}
		}

		price = append(price, p)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}

	return price, nil
}

func (r *PricingRepository) Exists(id string) bool {
	rows, err := r.Query("select 1 from pricing where id=?", id)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if rows.Next() {
		return true
	}

	return false
}

func (r *PricingRepository) Save(p *domain.Pricing) (*domain.Pricing, error) {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`insert into pricing values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			p.ID,
			p.Version,
			p.SKU,
			p.OfferTermCode,
			p.Region,
			p.InstanceType,
			p.UsageType,
			p.LeaseContractLength,
			p.PurchaseOption,
			p.OnDemand,
			p.ReservedQuantity,
			p.ReservedHrs,
			p.Tenancy,
			p.PreInstalled,
			p.Operation,
			p.OperatingSystem,
			p.CacheEngine,
			p.DatabaseEngine,
			p.OfferingClass,
			p.NormalizationSizeFactor,
		); err != nil {
			return fmt.Errorf("insert into pricing: %v", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}

	return p, nil
}

func (r *PricingRepository) Delete(id string) error {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec("delete from pricing where id=?", id); err != nil {
			return fmt.Errorf("delete from pricing: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("transaction: %v", err)
	}

	return nil
}
