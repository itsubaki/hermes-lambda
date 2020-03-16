package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
	"github.com/itsubaki/hermes/pkg/usage"
)

func handle(ctx context.Context) error {
	dir := "/tmp"
	period := "1d"
	region := []string{
		"ap-northeast-1",
		"ap-southeast-1",
		"us-west-1",
		"us-west-2",
	}

	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", period, err)
	}

	fmt.Println("serialize cost")
	if err := cost.Serialize(dir, date); err != nil {
		return fmt.Errorf("serialize cost: %v", err)
	}

	fmt.Println("serialize reservation")
	if err := reservation.Serialize(dir, date); err != nil {
		return fmt.Errorf("serialize reservation: %v", err)
	}

	fmt.Println("serialize usage")
	if err := usage.Serialize(dir, date); err != nil {
		return fmt.Errorf("serialize usage: %v", err)
	}

	fmt.Println("serialize pricing")
	if err := pricing.Serialize(dir, region); err != nil {
		return fmt.Errorf("serialize pricing: %v", err)
	}

	return nil
}

func main() {
	fmt.Println("start")
	lambda.Start(handle)
	fmt.Println("finished")
}
