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
				normalization_size_factor	double,
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
	return make([]domain.Pricing, 0), nil
}

func (r *PricingRepository) Exists(id string) bool {
	return false
}

func (r *PricingRepository) Save(p *domain.Pricing) (domain.Pricing, error) {
	return p, nil
}

func (r *PricingRepository) Delete(id string) error {
	return nil
}
