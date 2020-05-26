package database

import (
	"fmt"
	"testing"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure/handler"

	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
)

func TestPricingRepository(t *testing.T) {
	e := main.Env()
	h, _ := handler.New(e.Driver, e.DataSource, e.Database)
	defer h.Close()
	r := database.NewPricingRepository(h)

	fmt.Println(r)
}
