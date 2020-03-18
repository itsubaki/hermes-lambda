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
	utilization := make([]domain.Utilization, 0)
	if err := r.Transact(func(tx Tx) error {
		rows, err := tx.Query("select * from ri_utilization")
		if err != nil {
			return fmt.Errorf("select * from ri_utilization: %v", err)
		}
		defer rows.Close()

		var u domain.Utilization
		for rows.Next() {
			if err := rows.Scan(
				&u.ID,
				&u.AccountID,
				&u.Description,
				&u.Region,
				&u.InstanceType,
				&u.Platform,
				&u.CacheEngine,
				&u.DatabaseEngine,
				&u.DeploymentOption,
				&u.Date,
				&u.Hours,
				&u.Num,
				&u.Percentage,
				&u.CoveringCost,
			); err != nil {
				return fmt.Errorf("scan: %v", err)
			}
		}

		utilization = append(utilization, u)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}

	return utilization, nil
}

func (r *UtilizationRepository) Exists(id string) bool {
	rows, err := r.Query("select 1 from ri_utilization where id=?", id)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if rows.Next() {
		return true
	}

	return false
}

func (r *UtilizationRepository) Save(u *domain.Utilization) (*domain.Utilization, error) {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`insert into ri_utilization values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			u.ID,
			u.AccountID,
			u.Description,
			u.Region,
			u.InstanceType,
			u.Platform,
			u.CacheEngine,
			u.DatabaseEngine,
			u.DeploymentOption,
			u.Date,
			u.Hours,
			u.Num,
			u.Percentage,
			u.CoveringCost,
		); err != nil {
			return fmt.Errorf("insert into ri_utilization: %v", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}
	return u, nil
}

func (r *UtilizationRepository) Delete(id string) error {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec("delete from ri_utilization where id=?", id); err != nil {
			return fmt.Errorf("delete from ri_utilization: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("transaction: %v", err)
	}

	return nil
}
