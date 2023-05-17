package cost

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/itsubaki/hermes-lambda/dataset"
	"github.com/itsubaki/hermes/calendar"
	"github.com/itsubaki/hermes/cost"
	"github.com/urfave/cli/v2"
)

func Fetch(c *cli.Context) error {
	projectID := c.String("project")
	dsn := c.String("dataset")
	loc := c.String("location")
	period := c.String("period")

	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("get last date: %v", err)
	}

	table := dataset.AccountCostMeta(period)
	if err := dataset.Create(c.Context, projectID, loc, dsn, []bigquery.TableMetadata{table}); err != nil {
		return fmt.Errorf("create dataset: %v", err)
	}

	now := time.Now()
	for i := range date {
		list, err := cost.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch cost (%s, %s): %v", date[i].Start, date[i].End, err)
		}

		items := make([]interface{}, 0)
		for _, ac := range list {
			item, err := Item(now, ac)
			if err != nil {
				return fmt.Errorf("item: %v", err)
			}

			items = append(items, item)
		}

		if err := dataset.Insert(c.Context, projectID, loc, dsn, table.Name, items); err != nil {
			return fmt.Errorf("insert: %v", err)
		}

		for _, item := range items {
			fmt.Println(item)
		}
	}

	return nil
}

func Item(now time.Time, c cost.AccountCost) (dataset.AccountCost, error) {
	u, err := strconv.ParseFloat(c.UnblendedCost.Amount, 64)
	if err != nil {
		return dataset.AccountCost{}, fmt.Errorf("parse float: %v", err)
	}

	b, err := strconv.ParseFloat(c.BlendedCost.Amount, 64)
	if err != nil {
		return dataset.AccountCost{}, fmt.Errorf("parse float: %v", err)
	}

	a, err := strconv.ParseFloat(c.AmortizedCost.Amount, 64)
	if err != nil {
		return dataset.AccountCost{}, fmt.Errorf("parse float: %v", err)
	}

	na, err := strconv.ParseFloat(c.NetAmortizedCost.Amount, 64)
	if err != nil {
		return dataset.AccountCost{}, fmt.Errorf("parse float: %v", err)
	}

	nu, err := strconv.ParseFloat(c.NetUnblendedCost.Amount, 64)
	if err != nil {
		return dataset.AccountCost{}, fmt.Errorf("parse float: %v", err)
	}

	date, err := civil.ParseDate(c.Date)
	if err != nil {
		return dataset.AccountCost{}, fmt.Errorf("parse date: %v", err)
	}

	return dataset.AccountCost{
		AccountID:        c.AccountID,
		Description:      c.Description,
		Date:             date,
		Service:          c.Service,
		RecordType:       c.RecordType,
		UnblendedCost:    u,
		BlendedCost:      b,
		AmortizedCost:    a,
		NetAmortizedCost: na,
		NetUnblendedCost: nu,
		InsertedAt:       now,
	}, nil
}
