package storage

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/pricing"
	"github.com/itsubaki/hermes/pkg/reservation"
)

type Utilization struct {
	Storage         Storage
	SuppressWarning bool
}

func (u *Utilization) OnDemandConversionCost(period, bucketName string, region []string) (map[string]float64, error) {
	out := make(map[string]float64, 0)

	util, err := u.Read(period, bucketName, region)
	if err != nil {
		return out, fmt.Errorf("read: %v", err)
	}

	for _, u := range util {
		if v, ok := out[u.Description]; ok {
			out[u.Description] = v + u.OnDemandConversionCost
			continue
		}

		out[u.Description] = u.OnDemandConversionCost
	}

	return out, nil
}

func (u *Utilization) Read(period, bucketName string, region []string) ([]reservation.Utilization, error) {
	out := make([]reservation.Utilization, 0)

	price := make([]pricing.Price, 0)
	for _, r := range region {
		key := fmt.Sprintf("pricing/%s.json", r)
		b, err := u.Storage.Read(bucketName, key)
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}
		log.Printf("read s3://%s/%s\n", bucketName, key)

		var p []pricing.Price
		if err := json.Unmarshal(b, &p); err != nil {
			return out, fmt.Errorf("unmarshal pricing: %v", err)
		}

		price = append(price, p...)
	}

	date, err := calendar.Last(period)
	if err != nil {
		return out, fmt.Errorf("get last period=%s: %v", period, err)
	}

	for i := range date {
		key := fmt.Sprintf("reservation/%s.json", date[i].String())
		read, err := u.Storage.Read(bucketName, key)
		if err != nil {
			return out, fmt.Errorf("read storage key=%s: %v", key, err)
		}

		var list []reservation.Utilization
		if err := json.Unmarshal(read, &list); err != nil {
			return out, fmt.Errorf("unmarshal reservation.Utilization: %v", err)
		}

		for _, e := range reservation.AddOnDemandConversionCost(price, list) {
			if u.SuppressWarning {
				continue
			}

			fmt.Printf("[WARN] %s\n", e)
		}

		out = append(out, list...)
	}

	return out, nil
}

func (u *Utilization) Fetch(period []string, bucketName string) error {
	for _, p := range period {
		if err := u.fetch(p, bucketName); err != nil {
			return err
		}
	}

	return nil
}

func (u *Utilization) fetch(period, bucketName string) error {
	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("get last period=%s: %v", period, err)
	}

	for i := range date {
		key := fmt.Sprintf("reservation/%s.json", date[i].String())
		exists, err := u.Storage.Exists(bucketName, key)
		if err != nil {
			return fmt.Errorf("s3 exists: %v", err)
		}

		if exists {
			if err := u.Storage.Delete(bucketName, key); err != nil {
				return fmt.Errorf("s3 delete s3://%s/%s: %v", bucketName, key, err)
			}
			log.Printf("deleted s3://%s/%s\n", bucketName, key)
		}

		ac, err := reservation.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch reservation (%s, %s): %v\n", date[i].Start, date[i].End, err)
		}
		log.Printf("fetched %s %s", date[i].Start, date[i].End)

		b, err := json.Marshal(ac)
		if err != nil {
			return fmt.Errorf("marshal: %v\n", err)
		}

		if err := u.Storage.Write(bucketName, key, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, key, err)
		}
		log.Printf("wrote s3://%s/%s\n", bucketName, key)
	}

	return nil
}
