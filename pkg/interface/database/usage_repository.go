package database

import (
	"fmt"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
)

type UsageRepository struct {
	Handler
}

func NewUsageRepository(h Handler) *UsageRepository {
	if err := h.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`
			create table if not exists service_usage (
				id				varchar(64) not null primary key,
				account_id		varchar(12) not null,
				description		varchar(64)	not null,
				region			varchar(64)	not null,
				usage_type		varchar(64)	not null,
				platform		varchar(64)	,
				cache_engine	varchar(64)	,
				database_engine	varchar(64)	,
				date			varchar(10)	not null,
				instance_hour	double		,
				instance_num	double		,
				giga_byte		double		,
				requests		int			,
				unit			varchar(64)	not null,
				index(account_id),
				index(date)
			)
			`,
		); err != nil {
			return fmt.Errorf("create table service_usage: %v", err)
		}

		return nil
	}); err != nil {
		panic(fmt.Errorf("transaction: %v", err))
	}

	return &UsageRepository{
		Handler: h,
	}
}

func (r *UsageRepository) List() ([]domain.UsageQuantity, error) {
	return make([]domain.UsageQuantity, 0), nil
}

func (r *UsageRepository) Exists(id string) bool {
	return false
}

func (r *UsageRepository) Save(p *domain.UsageQuantity) (*domain.UsageQuantity, error) {
	return p, nil
}

func (r *UsageRepository) Delete(id string) error {
	return nil
}
