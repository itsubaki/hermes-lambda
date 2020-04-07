package database

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/itsubaki/hermes-lambda/pkg/domain"
	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
)

func TestAccountCostRepository(t *testing.T) {
	e := infrastructure.Env()
	h, _ := infrastructure.New(e.Driver, e.DataSource, e.Database)
	defer h.Close()
	r := database.NewAccountCostRepository(h)

	c := &domain.AccountCost{
		AccountID:              "123412341234",
		Description:            "web service",
		Date:                   "2020-02-02",
		Service:                "Amazon Elastic Compute Cloud - Compute",
		RecordType:             "Usage",
		UnblendedCostAmount:    "1.234",
		UnblendedCostUnit:      "USD",
		BlendedCostAmount:      "2.345",
		BlendedCostUnit:        "USD",
		AmortizedCostAmount:    "3.456",
		AmortizedCostUnit:      "USD",
		NetAmortizedCostAmount: "4.567",
		NetAmortizedCostUnit:   "USD",
		NetUnblendedCostAmount: "5.678",
		NetUnblendedCostUnit:   "USD",
	}

	sha := sha256.Sum256([]byte(c.JSON()))
	c.ID = hex.EncodeToString(sha[:11])

	if r.Exists(c.ID) {
		t.Fatalf("account cost already exists: %#v", c)
	}

	if _, err := r.Save(c); err != nil {
		t.Fatalf("save account cost: %v", err)
	}

	list, err := r.List()
	if err != nil {
		t.Fatalf("list account cost: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("invalid list: %v", err)
	}

	if list[0].ID != c.ID {
		t.Fatalf("invalid account cost id: %v", err)
	}

	if err := r.Delete(c.ID); err != nil {
		t.Fatalf("delete account cost: %v", err)
	}
}
