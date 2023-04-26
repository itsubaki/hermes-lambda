package reservation

import (
	"fmt"

	"github.com/itsubaki/hermes/calendar"
	"github.com/itsubaki/hermes/reservation"
	"github.com/urfave/cli/v2"
)

func Fetch(c *cli.Context) error {
	period := c.String("period")

	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("get last date: %v", err)
	}

	for i := range date {
		rsv, err := reservation.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch reservation (%s, %s): %v", date[i].Start, date[i].End, err)
		}

		for _, r := range rsv {
			fmt.Println(r)
		}
	}

	return nil
}
