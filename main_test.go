package main

import (
	"context"
	"os"
	"testing"
)

func TestDatabaseHandler(t *testing.T) {
	os.Setenv("AWS_PROFILE", "example")
	os.Setenv("PERIOD", "1d")
	os.Setenv("DATABASE", "hermes_daily")

	if err := handle(context.Background()); err != nil {
		t.Errorf("handle: %v", err)
	}
}
