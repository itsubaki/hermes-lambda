package database

import (
	"fmt"
	"testing"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
)

func TestUsageQuantityRepository(t *testing.T) {
	e := infrastructure.NewEnv()
	h, _ := infrastructure.NewHandler(e.Driver, e.DataSource, e.Database)
	defer h.Close()
	r := database.NewUsageQuantityRepository(h)

	fmt.Println(r)
}
