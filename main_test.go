package main

import (
	"context"
	"os"
	"testing"
)

func TestHandler(t *testing.T) {
	os.Setenv("AWS_PROFILE", "example")
	os.Setenv("PERIOD", "1m")

	if err := handle(context.Background()); err != nil {
		t.Errorf("handle: %v", err)
	}
}
