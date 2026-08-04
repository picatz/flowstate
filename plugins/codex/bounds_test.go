package main

import "testing"

func TestClampMaxOutputBytesDefaultsAndRefusesOverCeiling(t *testing.T) {
	got, err := clampMaxOutputBytes(0)
	if err != nil || got != defaultMaxOutputBytes {
		t.Fatalf("clampMaxOutputBytes(0) = (%d, %v), want (%d, nil)", got, err, defaultMaxOutputBytes)
	}

	if _, err := clampMaxOutputBytes(-1); err == nil {
		t.Error("clampMaxOutputBytes(-1): got no error, want one")
	}

	if _, err := clampMaxOutputBytes(maxMaxOutputBytes + 1); err == nil {
		t.Error("clampMaxOutputBytes(ceiling+1): got no error, want one - a value over the ceiling " +
			"must be refused, not silently clamped")
	}

	got, err = clampMaxOutputBytes(1024)
	if err != nil || got != 1024 {
		t.Fatalf("clampMaxOutputBytes(1024) = (%d, %v), want (1024, nil)", got, err)
	}
}

func TestClampMaxEventsDefaultsAndRefusesOverCeiling(t *testing.T) {
	got, err := clampMaxEvents(0)
	if err != nil || got != defaultMaxEvents {
		t.Fatalf("clampMaxEvents(0) = (%d, %v), want (%d, nil)", got, err, defaultMaxEvents)
	}

	if _, err := clampMaxEvents(-1); err == nil {
		t.Error("clampMaxEvents(-1): got no error, want one")
	}

	if _, err := clampMaxEvents(maxMaxEvents + 1); err == nil {
		t.Error("clampMaxEvents(ceiling+1): got no error, want one")
	}
}
