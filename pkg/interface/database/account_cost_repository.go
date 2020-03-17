package database

import (
	"fmt"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
)

type AccountCostRepository struct {
	Handler
}

func NewAccountCostRepository(h Handler) *AccountCostRepository {
	if err := h.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`
			create table if not exists account_cost (
				id							varchar(64)	not null primary key,
				account_id					varchar(12)	not null,
				description					varchar(64)	not null,
				date						varchar(10)	not null,
				service						text		not null,
				record_type					text		not null,
				unblended_cost_amount		text		not null,
				unblended_cost_unit			varchar(16)	not null,
				blended_cost_amount			text		not null,
				blended_cost_unit			varchar(16)	not null,
				amortized_cost_amount		text		not null,
				amortized_cost_unit			varchar(16)	not null,
				net_amortized_cost_amount	text		not null,
				net_amortized_cost_unit		varchar(16)	not null,
				net_unblended_cost_amount	text		not null,
				net_unblended_cost_unit		varchar(16)	not null,
				index(account_id),
				index(date)
			)
			`,
		); err != nil {
			return fmt.Errorf("create table account_cost: %v", err)
		}

		return nil
	}); err != nil {
		panic(fmt.Errorf("transaction: %v", err))
	}

	return &AccountCostRepository{
		Handler: h,
	}
}

func (r *AccountCostRepository) List() ([]domain.AccountCost, error) {
	cost := make([]domain.AccountCost, 0)
	if err := r.Transact(func(tx Tx) error {
		rows, err := tx.Query("select * from account_cost")
		if err != nil {
			return fmt.Errorf("select * from account_cost: %v", err)
		}
		defer rows.Close()

		var c domain.AccountCost
		for rows.Next() {
			if err := rows.Scan(
				&c.ID,
				&c.AccountID,
				&c.Description,
				&c.Date,
				&c.Service,
				&c.RecordType,
				&c.UnblendedCostAmount,
				&c.UnblendedCostUnit,
				&c.BlendedCostAmount,
				&c.BlendedCostUnit,
				&c.AmortizedCostAmount,
				&c.AmortizedCostUnit,
				&c.NetAmortizedCostAmount,
				&c.NetAmortizedCostUnit,
				&c.NetUnblendedCostAmount,
				&c.NetUnblendedCostUnit,
			); err != nil {
				return fmt.Errorf("scan: %v", err)
			}
		}

		cost = append(cost, c)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transction: %v", err)
	}

	return cost, nil
}

func (r *AccountCostRepository) Exists(id string) bool {
	rows, err := r.Query("select 1 from account_cost where id=?", id)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if rows.Next() {
		return true
	}

	return false
}

func (r *AccountCostRepository) Save(c *domain.AccountCost) (*domain.AccountCost, error) {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec(
			`insert into account_cost values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			c.ID,
			c.AccountID,
			c.Description,
			c.Date,
			c.Service,
			c.RecordType,
			c.UnblendedCostAmount,
			c.UnblendedCostUnit,
			c.BlendedCostAmount,
			c.BlendedCostUnit,
			c.AmortizedCostAmount,
			c.AmortizedCostUnit,
			c.NetAmortizedCostAmount,
			c.NetAmortizedCostUnit,
			c.NetUnblendedCostAmount,
			c.NetUnblendedCostUnit,
		); err != nil {
			return fmt.Errorf("insert into account_cost: %v", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("transaction: %v", err)
	}

	return c, nil
}

func (r *AccountCostRepository) Delete(id string) error {
	if err := r.Transact(func(tx Tx) error {
		if _, err := tx.Exec("delete from account_cost where id=?", id); err != nil {
			return fmt.Errorf("delete from account_cost: %v", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("transaction: %v", err)
	}

	return nil
}
