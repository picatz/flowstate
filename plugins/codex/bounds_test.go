package main

import (
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

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

// TestMaxOutputBytesCeilingFitsATaskOutput pins #2168: a result whose text
// spends the whole max_output_bytes ceiling, split across the three fields
// that share it, is within flowstatev1.MaxTaskOutputBytes as the host
// measures a step output.
func TestMaxOutputBytesCeilingFitsATaskOutput(t *testing.T) {
	// Control characters are what JSON spells with the most bytes (\u0001),
	// the worst case for the encoding the host measures.
	text := func(n int) string { return strings.Repeat("\x01", n) }

	out, err := sdk.EncodeOutputs(&codexv1.ExecOutputs{
		FinalMessage: text(maxFinalMessageBytes),
		Patch:        text(maxMaxOutputBytes - maxFinalMessageBytes),
		ThreadId:     text(256),
	})
	if err != nil {
		t.Fatalf("EncodeOutputs: %v", err)
	}
	if err := flowstatev1.CheckTaskOutputSize(out); err != nil {
		t.Fatalf("a codex result at its own ceiling (%d bytes) is refused by the host: %v", maxMaxOutputBytes, err)
	}
}
