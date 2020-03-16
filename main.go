package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
)

func handle(ctx context.Context) error {
	dir := "/tmp"
	period := "1d"
	//region := []string{
	//	"ap-northeast-1",
	//	"ap-southeast-1",
	//	"us-west-1",
	//	"us-west-2",
	//}

	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("calendar.Last period=%s: %v", period, err)
	}

	if err := cost.Serialize(dir, date); err != nil {
		return fmt.Errorf("serialize cost: %v", err)
	}

	ac, err := cost.Deserialize(dir, date)
	if err != nil {
		fmt.Printf("deserialize cost: %v\n", err)
		os.Exit(1)
	}

	for _, a := range ac {
		fmt.Println(a)
	}

	//if err := reservation.Serialize(dir, date); err != nil {
	//	return fmt.Errorf("serialize reservation: %v", err)
	//}
	//
	//if err := usage.Serialize(dir, date); err != nil {
	//	return fmt.Errorf("serialize usage: %v", err)
	//}
	//
	//if err := pricing.Serialize(dir, region); err != nil {
	//	return fmt.Errorf("serialize pricing: %v", err)
	//}

	return nil
}

func main() {
	lambda.Start(handle)
}
