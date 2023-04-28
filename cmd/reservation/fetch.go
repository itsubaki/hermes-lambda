package reservation

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/itsubaki/hermes-lambda/dataset"
	"github.com/itsubaki/hermes/calendar"
	"github.com/itsubaki/hermes/reservation"
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

	table := dataset.ReservationMeta(period)
	if err := dataset.Create(c.Context, projectID, loc, dsn, []bigquery.TableMetadata{table}); err != nil {
		return fmt.Errorf("create dataset: %v", err)
	}

	now := time.Now()
	for i := range date {
		list, err := reservation.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch reservation (%s, %s): %v", date[i].Start, date[i].End, err)
		}

		items := make([]interface{}, 0)
		for _, rsv := range list {
			item, err := Item(now, rsv)
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

func Item(now time.Time, u reservation.Utilization) (dataset.Reservation, error) {
	date, err := civil.ParseDate(u.Date)
	if err != nil {
		return dataset.Reservation{}, fmt.Errorf("parse date: %v", err)
	}

	return dataset.Reservation{
		Timestamp:        now,
		AccountID:        u.AccountID,
		Description:      u.Description,
		Date:             date,
		Region:           u.Region,
		InstanceType:     u.InstanceType,
		Platform:         u.Platform,
		CacheEngine:      u.CacheEngine,
		DatabaseEngine:   u.DatabaseEngine,
		DeploymentOption: u.DeploymentOption,
		Hours:            u.Hours,
		Num:              u.Num,
		Percentage:       u.Percentage,
	}, nil
}
