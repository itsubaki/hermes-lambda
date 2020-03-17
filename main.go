package main

import (
	"context"
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
	"github.com/itsubaki/hermes/pkg/usage"
)

func handle(ctx context.Context) error {
	e := infrastructure.NewEnviron()

	date, err := calendar.Last(e.Period)
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", e.Period, err)
	}

	// serialize
	log.Println("start: serialize cost")
	if err := cost.Serialize(e.Dir, date); err != nil {
		return fmt.Errorf("serialize cost: %v", err)
	}
	log.Println("finished: serialize cost")

	log.Println("start: serialize reservation")
	if err := reservation.Serialize(e.Dir, date); err != nil {
		return fmt.Errorf("serialize reservation: %v", err)
	}
	log.Println("finished: serialize reservation")

	log.Println("start: serialize usage")
	if err := usage.Serialize(e.Dir, date); err != nil {
		return fmt.Errorf("serialize usage: %v", err)
	}
	log.Println("finished: serialize usage")

	log.Println("start: serialize pricing")
	if err := pricing.Serialize(e.Dir, e.Region); err != nil {
		return fmt.Errorf("serialize pricing: %v", err)
	}
	log.Println("finished: serialize pricing")

	// export to database
	// TODO
	// h := infrastructure.NewHandler(e.Driver, e.DataSource, e.Database)

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("start")
	lambda.Start(handle)
	log.Println("finished")
}
