package main

import (
	"testing"
)

// TestAStreamPastItsCeilingSignalsRatherThanRefusing is the work limit beside
// the reporting one, and the shape that limit has to take.
//
// max_output_bytes says how much of a stream a result carries; reading past a
// full stream is ordinary, and produces the truncated result that limit
// describes. Only far past it - maxOutputReadBytes - has the command stopped
// producing output being cut short and started deciding how long this call
// reads.
//
// The writer signals rather than refusing the write. Refusing leaves the far
// side blocked on a flow-control window nobody extends again: the command never
// exits, the session's wait never returns, and a command that ran to completion
// is reported as an unknown outcome a whole timeout later. exec closes the
// session on this signal instead.
func TestAStreamPastItsCeilingSignalsRatherThanRefusing(t *testing.T) {
	writer := newBoundedWriter(16)

	block := make([]byte, 1<<20)

	// Ordinary noise: well past the reporting limit, nowhere near the ceiling.
	// Every write is accepted, and nothing is signalled.
	for range 8 {
		n, err := writer.Write(block)
		if err != nil {
			t.Fatalf("a write below the ceiling was refused: %v", err)
		}
		if n != len(block) {
			t.Fatalf("wrote %d of %d bytes; a short count blocks the far side", n, len(block))
		}
	}

	select {
	case <-writer.exhausted:
		t.Fatal("the ceiling fired for a command that is merely noisy; ordinary output would stop being truncated and start failing")
	default:
	}

	if !writer.overflowed {
		t.Error("overflowed is false, so the result would be reported as complete")
	}
	if writer.buffer.String() != string(block[:16]) {
		t.Errorf("kept %d bytes, want exactly the limit's 16", writer.buffer.Len())
	}

	// Past the ceiling the signal fires, and writes still succeed, so the
	// copying goroutine ends on the session closing rather than on an error.
	for writer.discarded <= maxOutputReadBytes {
		if _, err := writer.Write(block); err != nil {
			t.Fatalf("a write past the ceiling was refused rather than signalled: %v", err)
		}
	}

	select {
	case <-writer.exhausted:
	default:
		t.Fatal("the ceiling was passed and nothing signalled; the session would read until the grant's timeout")
	}

	// Idempotent: a second pass must not close an already-closed channel.
	if _, err := writer.Write(block); err != nil {
		t.Fatalf("a write after the signal was refused: %v", err)
	}
}
