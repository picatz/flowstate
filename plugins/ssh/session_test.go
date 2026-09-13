package main

import (
	"errors"
	"testing"
)

// TestAStreamPastItsLimitStopsBeingRead is the work limit beside the reporting
// one.
//
// max_output_bytes says how much of a stream a result carries. Accepting and
// discarding everything after that bounds what is reported and nothing else: a
// command writing to a full stream would decide how long this call reads, for
// up to the grant's whole timeout. Past a margin the writer refuses, which ends
// the session.
func TestAStreamPastItsLimitStopsBeingRead(t *testing.T) {
	writer := &boundedWriter{limit: 16}

	block := make([]byte, 16)

	// The first block fills it; the next few are slack, so a command that ends
	// just over the limit is truncated rather than refused.
	for range 1 + discardRatio {
		if _, err := writer.Write(block); err != nil {
			t.Fatalf("a write within the discard margin was refused: %v", err)
		}
	}

	if _, err := writer.Write(block); err == nil {
		t.Fatal("a command past the margin was still being read; the limit bounds the report but not the work")
	} else if !errors.Is(err, errOutputExhausted) {
		t.Errorf("error is %v, want the exhausted-output sentinel", err)
	}

	if !writer.overflowed {
		t.Error("overflowed is false, so the result would be reported as complete")
	}
	if writer.buffer.String() != string(block) {
		t.Errorf("kept %q, want exactly the limit's worth", writer.buffer.String())
	}
}
