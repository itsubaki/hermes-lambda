package database

import (
	"fmt"
	"testing"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
)

func TestPricingRepository(t *testing.T) {
	e := infrastructure.NewEnviron()
	h := infrastructure.NewHandler(e.Driver, e.DataSource, e.Database)
	defer h.Close()
	r := database.NewPricingRepository(h)

	fmt.Println(r)
}