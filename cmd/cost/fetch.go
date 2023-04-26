package cost

import (
	"fmt"

	"github.com/itsubaki/hermes/calendar"
	"github.com/itsubaki/hermes/cost"
	"github.com/urfave/cli/v2"
)

func Fetch(c *cli.Context) error {
	period := c.String("period")

	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("get last date: %v", err)
	}

	for i := range date {
		cst, err := cost.Fetch(
			date[i].Start,
			date[i].End,
			[]string{"NetAmortizedCost", "NetUnblendedCost", "UnblendedCost", "AmortizedCost", "BlendedCost"},
		)
		if err != nil {
			return fmt.Errorf("fetch cost (%s, %s): %v", date[i].Start, date[i].End, err)
		}

		for _, c := range cst {
			fmt.Println(c)
		}
	}

	return nil
}
