package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
)

type AccountCost struct {
	Storage Storage
}

func IsIgnoreRecordType(c cost.AccountCost, ignoreRecordType []string) bool {
	for _, i := range ignoreRecordType {
		if c.RecordType == i {
			return true
		}
	}

	return false
}

func (c *AccountCost) Unblended(period, bucketName string, ignoreRecordType, region []string) (map[string]float64, error) {
	out := make(map[string]float64, 0)

	cost, err := c.Read(period, bucketName)
	if err != nil {
		return out, fmt.Errorf("read: %v", err)
	}

	for _, c := range cost {
		if ignore := IsIgnoreRecordType(c, ignoreRecordType); ignore {
			continue
		}

		a, err := strconv.ParseFloat(c.UnblendedCost.Amount, 64)
		if err != nil {
			return out, fmt.Errorf("parse float: %v", err)
		}

		if v, ok := out[c.Description]; ok {
			out[c.Description] = v + a
			continue
		}

		out[c.Description] = a
	}

	return out, nil
}

func (c *AccountCost) Read(period, bucketName string) ([]cost.AccountCost, error) {
	out := make([]cost.AccountCost, 0)

	date, err := calendar.Last(period)
	if err != nil {
		return out, fmt.Errorf("get last period=%s: %v", period, err)
	}

	for i := range date {
		key := fmt.Sprintf("cost/%s.json", date[i].String())
		read, err := c.Storage.Read(bucketName, key)
		if err != nil {
			return out, fmt.Errorf("read storage: %v", err)
		}

		var u []cost.AccountCost
		if err := json.Unmarshal(read, &u); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		out = append(out, u...)
	}

	return out, nil
}

func (c *AccountCost) Fetch(period []string, bucketName string) error {
	for _, p := range period {
		if err := c.fetch(p, bucketName); err != nil {
			return err
		}
	}

	return nil
}

func (c *AccountCost) fetch(period, bucketName string) error {
	date, err := calendar.Last(period)
	if err != nil {
		return fmt.Errorf("get last period=%s: %v", period, err)
	}

	for i := range date {
		key := fmt.Sprintf("cost/%s.json", date[i].String())
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

		ac, err := cost.Fetch(date[i].Start, date[i].End)
		if err != nil {
			return fmt.Errorf("fetch cost (%s, %s): %v\n", date[i].Start, date[i].End, err)
		}
		log.Printf("fetched %s %s", date[i].Start, date[i].End)

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
