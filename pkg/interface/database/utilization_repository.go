package database

import (
	"fmt"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
)

type UtilizationRepository struct {
	Handler
}

func NewUtilizationRepository(h Handler) *UtilizationRepository {
	if err := h.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`
			create table if not exists ri_utilization (
				id					varchar(64) not null primary key,
				account_id			varchar(12) not null,
				description			varchar(64)	not null,
				region				varchar(64)	not null,
				instance_type		varchar(64)	not null,
				platform			varchar(64),
				cache_engine		varchar(64),
				database_engine		varchar(64),
				deployment_option	varchar(64),
				date				varchar(10)	not null,
				hours				double,
				num					double,
				percentage			double,
				covering_cost		double,
				index(account_id),
				index(date)
			)
			`,
		); err != nil {
			return fmt.Errorf("create table ri_utlization: %v", err)
		}

		return nil
	}); err != nil {
		panic(fmt.Errorf("transaction: %v", err))
	}

	return &UtilizationRepository{
		Handler: h,
	}
}

func (r *UtilizationRepository) List() ([]domain.Utilization, error) {
	return make([]domain.Utilization, 0), nil
}

func (r *UtilizationRepository) Exists(id string) bool {
	return false
}

func (r *UtilizationRepository) Save(p *domain.Utilization) (*domain.Utilization, error) {
	return p, nil
}

func (r *UtilizationRepository) Delete(id string) error {
	return nil
}
