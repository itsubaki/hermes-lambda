package domain

import "encoding/json"

type AccountCost struct {
	ID                     string
	AccountID              string  `json:"account_id"`
	Description            string  `json:"description"`
	Date                   string  `json:"date"`
	Service                string  `json:"service"`
	RecordType             string  `json:"record_type"`
	UnblendedCostAmount    float64 `json:"unblended_cost_amount"`
	UnblendedCostUnit      string  `json:"unblended_cost_unit"`
	BlendedCostAmount      float64 `json:"blended_cost_amount"`
	BlendedCostUnit        string  `json:"blended_cost_unit"`
	AmortizedCostAmount    float64 `json:"amortized_cost_amount"`
	AmortizedCostUnit      string  `json:"amortized_cost_unit"`
	NetAmortizedCostAmount float64 `json:"net_amortized_cost_amount"`
	NetAmortizedCostUnit   string  `json:"net_amortized_cost_unit"`
	NetUnblendedCostAmount float64 `json:"net_unblended_cost_amount"`
	NetUnblendedCostUnit   string  `json:"net_unblended_cost_unit"`
}

func (a AccountCost) JSON() string {
	b, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(b)
}
