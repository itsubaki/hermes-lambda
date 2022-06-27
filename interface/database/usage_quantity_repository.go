package database

import (
	"fmt"

	"github.com/itsubaki/hermes-lambda/domain"
)

type UsageQuantityRepository struct {
	Handler
}

func NewUsageQuantityRepository(h Handler) *UsageQuantityRepository {
	if err := h.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`
			create table if not exists usage_quantity (
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
			return fmt.Errorf("create table usage_quantity: %v", err)
		}

		return nil
	}); err != nil {
		panic(fmt.Errorf("transaction: %v", err))
	}

	return &UsageQuantityRepository{
		Handler: h,
	}
}

func (r *UsageQuantityRepository) List() ([]domain.UsageQuantity, error) {
	quantity := make([]domain.UsageQuantity, 0)
	if err := r.Transact(func(tx Tx) error {
		rows, err := tx.Query("select * from usage_quantity")
		if err != nil {
			return fmt.Errorf("select * from usage_quantity: %v", err)
		}
		defer rows.Close()

		var q domain.UsageQuantity
		for rows.Next() {
			if err := rows.Scan(
				&q.ID,
				&q.AccountID,
				&q.Description,
				&q.Region,
				&q.UsageType,
				&q.Platform,
				&q.CacheEngine,
				&q.DatabaseEngine,
				&q.Date,
				&q.InstanceHour,
				&q.InstanceNum,
				&q.GByte,
				&q.Requests,
				&q.Unit,
			); err != nil {
				return fmt.Errorf("scan: %v", err)
			}
		}

		quantity = append(quantity, q)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}

	return quantity, nil
}

func (r *UsageQuantityRepository) Exists(id string) bool {
	rows, err := r.Query("select 1 from usage_quantity where id=?", id)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if rows.Next() {
		return true
	}

	return false
}

func (r *UsageQuantityRepository) Save(q *domain.UsageQuantity) (*domain.UsageQuantity, error) {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`insert into usage_quantity values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			q.ID,
			q.AccountID,
			q.Description,
			q.Region,
			q.UsageType,
			q.Platform,
			q.CacheEngine,
			q.DatabaseEngine,
			q.Date,
			q.InstanceHour,
			q.InstanceNum,
			q.GByte,
			q.Requests,
			q.Unit,
		); err != nil {
			return fmt.Errorf("insert into usage_quantity: %v", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}

	return q, nil
}

func (r *UsageQuantityRepository) Delete(id string) error {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec("delete from usage_quantity where id=?", id); err != nil {
			return fmt.Errorf("delete from usage_quantity: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("transaction: %v", err)
	}

	return nil
}
