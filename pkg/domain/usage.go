package domain

import "encoding/json"

type UsageQuantity struct {
	ID             string
	AccountID      string  `json:"account_id,omitempty"`
	Description    string  `json:"description,omitempty"`
	Region         string  `json:"region,omitempty"`
	UsageType      string  `json:"usage_type,omitempty"`
	Platform       string  `json:"platform,omitempty"`
	CacheEngine    string  `json:"cache_engine,omitempty"`
	DatabaseEngine string  `json:"database_engine,omitempty"`
	Date           string  `json:"date,omitempty"`
	InstanceHour   float64 `json:"instance_hour,omitempty"`
	InstanceNum    float64 `json:"instance_num,omitempty"`
	GByte          float64 `json:"giga_byte,omitempty"`
	Requests       int64   `json:"requests,omitempty"`
	Unit           string  `json:"unit"`
}

func (u UsageQuantity) JSON() string {
	b, err := json.Marshal(u)
	if err != nil {
		panic(err)
	}

	return string(b)
}
