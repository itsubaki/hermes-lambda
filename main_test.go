package main

import (
	"context"
	"os"
	"testing"

	"github.com/itsubaki/hermes-lambda/pkg/infrastructure"
)

func TestDatabaseHandler(t *testing.T) {
	os.Setenv("AWS_PROFILE", "example")
	os.Setenv("PERIOD", "1d")
	os.Setenv("DATABASE", "hermes_daily")

	if err := infrastructure.handle(context.Background()); err != nil {
		t.Errorf("handle: %v", err)
	}
}
