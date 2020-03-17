package database

import (
	"fmt"
	"testing"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
)

func TestUsageRepository(t *testing.T) {
	e := infrastructure.NewEnviron()
	h := infrastructure.NewHandler(e.Driver, e.DataSource, e.Database)
	defer h.Close()
	r := database.NewUsageRepository(h)

	fmt.Println(r)
}
