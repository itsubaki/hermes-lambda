package domain

import (
	"encoding/json"
	"fmt"
)

type Utilization struct {
	ID               string
	AccountID        string  `json:"account_id"`
	Description      string  `json:"description"`
	Region           string  `json:"region"`
	InstanceType     string  `json:"instance_type"`
	Platform         string  `json:"platform,omitempty"`
	CacheEngine      string  `json:"cache_engine,omitempty"`
	DatabaseEngine   string  `json:"database_engine,omitempty"`
	DeploymentOption string  `json:"deployment_option,omitempty"`
	Date             string  `json:"date"`
	Hours            float64 `json:"hours"`
	Num              float64 `json:"num"`
	Percentage       float64 `json:"percentage"`
	CoveringCost     float64 `json:"covering_cost"` // ondemand cost
}

func (u *Utilization) GenID() error {
	id, err := NewID(u.JSON())
	if err != nil {
		return fmt.Errorf("new id: %v", err)
	}

	u.ID = id
	return nil
}

func (u Utilization) JSON() string {
	b, err := json.Marshal(u)
	if err != nil {
		panic(err)
	}

	return string(b)
}
