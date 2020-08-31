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

func (l *HermesLambda) NewStorage() error {
	s3, err := handler.NewStorage()
	if err != nil {
		return fmt.Errorf("new storage: %v", err)
	}

	if err := s3.CreateIfNotExists(l.Env.BucketName); err != nil {
		return fmt.Errorf("create bucket=%s if not exists: %v", l.Env.BucketName, err)
	}

	l.Pricing = &storage.Pricing{Storage: s3}
	l.AccountCost = &storage.AccountCost{Storage: s3}
	l.Utilization = &storage.Utilization{Storage: s3, SuppressWarning: l.Env.SuppressWarning}

	return nil
}

func (l *HermesLambda) Run() error {
	for _, o := range l.Env.Output {
		if strings.ToLower(o) == "bigquery" {
			if err := l.PutItems(); err != nil {
				return fmt.Errorf("output to bigquery: %v", err)
			}
		}

		if strings.ToLower(o) == "mackerel" {
			if err := l.PostServiceMetricValues(); err != nil {
				return fmt.Errorf("output to mackerel: %v", err)
			}
		}

		if strings.ToLower(o) == "database" {
			if err := l.Store(); err != nil {
				return fmt.Errorf("output to database: %v", err)
			}
		}
	}

	return nil
}

func (l *HermesLambda) Fetch() error {
	if err := l.Pricing.Fetch(l.Env.BucketName, l.Env.Region); err != nil {
		return fmt.Errorf("fetch pricing: %v", err)
	}

	if err := l.AccountCost.Fetch(l.Env.Period, l.Env.BucketName); err != nil {
		return fmt.Errorf("fetch account cost: %v", err)
	}

	if err := l.Utilization.Fetch(l.Env.Period, l.Env.BucketName); err != nil {
		return fmt.Errorf("fetch utilization: %v", err)
	}

	return nil
}
