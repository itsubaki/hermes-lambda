package lambda

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
)

type CoveringCost struct {
	Storage         *infrastructure.Storage
	SuppressWarning bool
}

func (c *CoveringCost) Fetch(period, bucketName string) error {
	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("get last period=%s: %v", period, err)
	}

	for i := range date {
		key := fmt.Sprintf("reservation/%s.json", date[i].String())
		exists, err := c.Storage.Exists(bucketName, key)
		if err != nil {
			return fmt.Errorf("s3 exists: %v", err)
		}

		if exists {
			if err := c.Storage.Delete(bucketName, key); err != nil {
				return fmt.Errorf("s3 delte s3://%s/%s: %v", bucketName, key, err)
			}
			log.Printf("deleted s3://%s/%s\n", bucketName, key)
		}

		ac, err := reservation.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch reservation (%s, %s): %v\n", date[i].Start, date[i].End, err)
		}
		log.Printf("fetched s3://%s/%s\n", bucketName, key)

		b, err := json.Marshal(ac)
		if err != nil {
			return fmt.Errorf("marshal: %v\n", err)
		}

		if err := c.Storage.Write(bucketName, key, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, key, err)
		}
		log.Printf("wrote s3://%s/%s\n", bucketName, key)
	}

	return nil
}

func (c *CoveringCost) Aggregate(period, bucketName string, region []string) (map[string]float64, error) {
	out := make(map[string]float64, 0)

	date, err := calendar.Last(period)
	if err != nil {
		return out, fmt.Errorf("get last period=%s: %v", period, err)
	}

	price := make([]pricing.Price, 0)
	for _, r := range region {
		key := fmt.Sprintf("pricing/%s.json", r)
		b, err := c.Storage.Read(bucketName, key)
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}
		log.Printf("read s3://%s/%s\n", bucketName, key)

		var p []pricing.Price
		if err := json.Unmarshal(b, &p); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		price = append(price, p...)
	}

	for _, d := range date {
		key := fmt.Sprintf("reservation/%s.json", d.String())
		b, err := c.Storage.Read(bucketName, key)
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}
		log.Printf("read s3://%s/%s\n", bucketName, key)

		var util []reservation.Utilization
		if err := json.Unmarshal(b, &util); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		for _, e := range reservation.AddCoveringCost(price, util) {
			if c.SuppressWarning {
				continue
			}

			fmt.Printf("[WARN] %s\n", e)
		}

		for _, u := range util {
			v, ok := out[u.Description]
			if !ok {
				out[u.Description] = u.CoveringCost
				continue
			}

			out[u.Description] = v + u.CoveringCost
		}
	}

	return out, nil
}
