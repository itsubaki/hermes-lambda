package main

import (
	"context"
	"os"
	"testing"
)

func TestHandler(t *testing.T) {
	os.Setenv("AWS_PROFILE", "example")
	os.Setenv("PERIOD", "1d")
	os.Setenv("DATABASE", "hermes_daily")

	if err := handle2(context.Background()); err != nil {
		t.Errorf("handle: %v", err)
	}
}
