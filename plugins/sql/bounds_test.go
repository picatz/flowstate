package main

import (
	"strings"
	"testing"
)

func TestClampMaxRowsRefusesZeroAndNegative(t *testing.T) {
	if _, err := clampMaxRows(0); err == nil {
		t.Error("clampMaxRows(0): got no error, want a refusal (no default)")
	}
	if _, err := clampMaxRows(-1); err == nil {
		t.Error("clampMaxRows(-1): got no error, want a refusal")
	}
}

func TestClampMaxRowsRefusesOverTheCeilingRatherThanReducing(t *testing.T) {
	requested := int32(maxMaxRows + 1)
	got, err := clampMaxRows(requested)
	if err == nil {
		t.Fatalf("clampMaxRows(%d): got (%d, nil), want a refusal - a request over the ceiling "+
			"must be refused, never silently reduced to the ceiling", requested, got)
	}
	if !strings.Contains(err.Error(), "max_rows") {
		t.Errorf("error does not name max_rows: %v", err)
	}
}

func TestClampMaxRowsAcceptsTheCeilingItself(t *testing.T) {
	got, err := clampMaxRows(int32(maxMaxRows))
	if err != nil {
		t.Fatalf("clampMaxRows(maxMaxRows): unexpected error: %v", err)
	}
	if got != maxMaxRows {
		t.Errorf("clampMaxRows(maxMaxRows) = %d, want %d", got, maxMaxRows)
	}
}

func TestClampMaxRowsAcceptsAnOrdinaryValue(t *testing.T) {
	got, err := clampMaxRows(50)
	if err != nil {
		t.Fatalf("clampMaxRows(50): unexpected error: %v", err)
	}
	if got != 50 {
		t.Errorf("clampMaxRows(50) = %d, want 50", got)
	}
}
