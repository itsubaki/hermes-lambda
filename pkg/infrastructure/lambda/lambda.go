package lambda

import (
	"fmt"
	"strings"
	"time"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/environ"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/handler"
	"github.com/itsubaki/hermes-lambda/pkg/interface/storage"
)

type HermesLambda struct {
	Time        time.Time
	Env         *environ.Env
	Pricing     *storage.Pricing
	AccountCost *storage.AccountCost
	Utilization *storage.Utilization
}

func Default(e *environ.Env) *HermesLambda {
	return &HermesLambda{
		Time: time.Now(),
		Env:  e,
	}
}

func New(e *environ.Env) (*HermesLambda, error) {
	s3, err := handler.NewStorage()
	if err != nil {
		return nil, fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(e.BucketName); err != nil {
		return nil, fmt.Errorf("create bucket=%s if not exists: %v", e.BucketName, err)
	}

	return &HermesLambda{
		Time:        time.Now(),
		Env:         e,
		Pricing:     &storage.Pricing{Storage: s3},
		AccountCost: &storage.AccountCost{Storage: s3},
		Utilization: &storage.Utilization{Storage: s3, SuppressWarning: e.SuppressWarning},
	}, nil
}

func (h *HermesLambda) Run() error {
	for _, o := range h.Env.Output {
		if strings.ToLower(o) == "bigquery" {
			if err := h.PutItems(); err != nil {
				return fmt.Errorf("output to bigquery: %v", err)
			}
		}

		if strings.ToLower(o) == "mackerel" {
			v, err := h.MetricValues()
			if err != nil {
				return fmt.Errorf("metric values of mackerel: %v", err)
			}
			if err := h.PostServiceMetricValues(v); err != nil {
				return fmt.Errorf("output to mackerel: %v", err)
			}
		}

		if strings.ToLower(o) == "database" {
			if err := h.Store(); err != nil {
				return fmt.Errorf("output to database: %v", err)
			}
		}
	}

	return nil
}
