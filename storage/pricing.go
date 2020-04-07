package storage

import (
	"encoding/json"
	"fmt"

	"github.com/itsubaki/hermes/pkg/pricing"
)

type Pricing struct {
	Storage *Storage
}

func (p *Pricing) Fetch(bucketName string, region []string) error {
	for _, r := range region {
		key := fmt.Sprintf("pricing/%s.json", r)
		exists, err := p.Storage.Exists(bucketName, key)
		if err != nil {
			return fmt.Errorf("exists: %v", err)
		}

		if exists {
			continue
		}

		price := make([]pricing.Price, 0)
		for _, url := range pricing.URL {
			p, err := pricing.Fetch(url, r)
			if err != nil {
				return fmt.Errorf("fetch pricing (%s, %s): %v\n", url, r, err)
			}

			list := make([]pricing.Price, 0)
			for k := range p {
				list = append(list, p[k])
			}

			price = append(price, list...)
		}

		b, err := json.Marshal(price)
		if err != nil {
			return fmt.Errorf("marshal: %v", err)
		}

		if err := p.Storage.Write(bucketName, key, b); err != nil {
			return fmt.Errorf("write s3://%s/%s: %v", bucketName, key, err)
		}
	}

	return nil
}
