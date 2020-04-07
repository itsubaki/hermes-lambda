package bq

import (
	"context"
	"fmt"
	"log"

	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
)

func Handle(ctx context.Context) error {
	e := NewEnv()
	log.Printf("env=%#v", e)

	for _, p := range e.Period {
		date, err := calendar.Last(p)
		if err != nil {
			return fmt.Errorf("get last period=%s: %v", p, err)
		}

		for i := range date {
			c, err := cost.Fetch(date[i].Start, date[i].End)
			if err != nil {
				return fmt.Errorf("fetch cost (%s, %s): %v\n", date[i].Start, date[i].End, err)
			}
			log.Printf("fetched %v\n", date[i])

			fmt.Printf("%s %v\n", p, c)
		}
	}

	return nil
}
