package domain

import (
	"encoding/json"
	"fmt"
)

type Pricing struct {
	ID                      string
	Version                 string  `json:"version,omitempty"`                   // common
	SKU                     string  `json:"sku,omitempty"`                       // common
	OfferTermCode           string  `json:"offer_term_code,omitempty"`           // common
	Region                  string  `json:"region,omitempty"`                    // common
	InstanceType            string  `json:"instance_type,omitempty"`             // common
	UsageType               string  `json:"usage_type,omitempty"`                // common
	LeaseContractLength     string  `json:"lease_contract_length,omitempty"`     // common
	PurchaseOption          string  `json:"purchase_option,omitempty"`           // common
	OnDemand                float64 `json:"ondemand,omitempty"`                  // common
	ReservedQuantity        float64 `json:"reserved_quantity,omitempty"`         // common
	ReservedHrs             float64 `json:"reserved_hours,omitempty"`            // common
	Tenancy                 string  `json:"tenancy,omitempty"`                   // compute: Shared, Host, Dedicated
	PreInstalled            string  `json:"pre_installed,omitempty"`             // compute: SQL Web, SQL Ent, SQL Std, NA
	Operation               string  `json:"operation,omitempty"`                 // compute
	OperatingSystem         string  `json:"operating_system,omitempty"`          // compute: Windows, Linux, SUSE, RHEL
	CacheEngine             string  `json:"cache_engine,omitempty"`              // cache
	DatabaseEngine          string  `json:"database_engine,omitempty"`           // database
	OfferingClass           string  `json:"offering_class,omitempty"`            // compute, database
	NormalizationSizeFactor string  `json:"normalization_size_factor,omitempty"` // compute, database
}

func (p *Pricing) GenID() error {
	id, err := NewID(p.JSON())
	if err != nil {
		return fmt.Errorf("new id: %v", err)
	}

	p.ID = id
	return nil
}

func (p Pricing) JSON() string {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}

	return string(b)
}
