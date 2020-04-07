package mackerel

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/itsubaki/hermes/pkg/calendar"
	"github.com/itsubaki/hermes/pkg/cost"
)

type UnblendedCost struct {
	Storage *Storage
}

func (c *UnblendedCost) Fetch(period, bucketName string) error {
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

func (c *UnblendedCost) Aggregate(period, bucketName string, ignoreRecordType, region []string) (map[string]float64, error) {
	out := make(map[string]float64, 0)

	date, err := calendar.Last(period)
	if err != nil {
		return out, fmt.Errorf("get last period=%s: %v", period, err)
	}

	for _, d := range date {
		key := fmt.Sprintf("cost/%s.json", d.String())
		b, err := c.Storage.Read(bucketName, key)
		if err != nil {
			return out, fmt.Errorf("s3 read: %v", err)
		}
		log.Printf("read s3://%s/%s\n", bucketName, key)

		var cost []cost.AccountCost
		if err := json.Unmarshal(b, &cost); err != nil {
			return out, fmt.Errorf("unmarshal: %v", err)
		}

		for _, c := range cost {
			ignore := false
			for _, i := range ignoreRecordType {
				if c.RecordType == i {
					ignore = true
					break
				}
			}

			if ignore {
				continue
			}

			a, err := strconv.ParseFloat(c.UnblendedCost.Amount, 64)
			if err != nil {
				return out, fmt.Errorf("parse float: %v", err)
			}

			v, ok := out[c.Description]
			if !ok {
				out[c.Description] = a
				continue
			}
			out[c.Description] = v + a
		}
	}

	return out, nil
}
