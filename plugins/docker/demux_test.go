package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// frame renders one of the daemon's multiplexed frames.
func frame(stream byte, payload string) []byte {
	header := make([]byte, frameHeaderBytes)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	return append(header, payload...)
}

// endlessFrames is a log stream that never ends, counting what was read from
// it. The count is the thing a work limit is a limit on: a stream this plugin
// reads to its end is a stream whose length the container chose.
type endlessFrames struct {
	// streams are the stream bytes the frames carry, cycled.
	streams []byte

	next    int
	pending bytes.Buffer
	read    int64
}

func (e *endlessFrames) Read(p []byte) (int, error) {
	for e.pending.Len() < len(p) {
		stream := e.streams[e.next%len(e.streams)]
		e.next++
		e.pending.Write(frame(stream, strings.Repeat("x", 64)))
	}

	n, err := e.pending.Read(p)
	e.read += int64(n)
	return n, err
}

// TestReadingStopsOnceBothStreamsAreFull is the work limit, as opposed to the
// reporting limit beside it.
//
// A grant's max_output_bytes says how much of each stream a result carries.
// Once both streams hold that much, nothing further can be reported, and a loop
// that went on discarding frames until the container's own EOF would let the
// container decide how long this call runs - which is what invariant 5 calls a
// reporting limit standing in for a work limit.
func TestReadingStopsOnceBothStreamsAreFull(t *testing.T) {
	body := &endlessFrames{streams: []byte{streamStdout, streamStderr}}

	stdout, stderr, truncated, err := demultiplex(body, 16)
	if err != nil {
		t.Fatalf("demultiplex: %v", err)
	}
	if !truncated {
		t.Error("truncated is false for a stream that was cut off")
	}
	if len(stdout) != 16 || len(stderr) != 16 {
		t.Errorf("kept %d bytes of stdout and %d of stderr, want 16 of each", len(stdout), len(stderr))
	}

	// Four frames of 72 bytes is already generous for filling two 16-byte
	// limits; the point is that the number does not depend on the container.
	if body.read > 1<<10 {
		t.Errorf("read %d bytes from an endless stream, want it bounded by the limits rather than by the container", body.read)
	}
}

// TestReadingOneEndlessStreamIsStillBounded is the same limit where only one
// stream is full: reaching the other means reading past it, so the ceiling that
// ends the read is this plugin's own rather than the grant's.
func TestReadingOneEndlessStreamIsStillBounded(t *testing.T) {
	body := &endlessFrames{streams: []byte{streamStdout}}

	stdout, _, truncated, err := demultiplex(body, 16)
	if err != nil {
		t.Fatalf("demultiplex: %v", err)
	}
	if !truncated {
		t.Error("truncated is false for a stream that was cut off")
	}
	if len(stdout) != 16 {
		t.Errorf("kept %d bytes of stdout, want the grant's 16", len(stdout))
	}
	if body.read > maxLogReadBytes+maxFrameBytes {
		t.Errorf("read %d bytes, want at most one frame past the %d-byte ceiling", body.read, int64(maxLogReadBytes))
	}
}

// TestAnUnknownStreamCutMidFrameIsNotFatal covers the frame this plugin does
// not keep: an unknown stream contributes to neither result, and one whose body
// ends short of its declared length ends the read rather than anything worse.
func TestAnUnknownStreamCutMidFrameIsNotFatal(t *testing.T) {
	var body bytes.Buffer
	body.Write(frame(streamStdout, "kept\n"))

	// A stream this plugin does not know - stdin echoed back, or a future one -
	// declaring more than the rest of the body carries.
	unknown := make([]byte, frameHeaderBytes)
	unknown[0] = 9
	binary.BigEndian.PutUint32(unknown[4:8], 4096)
	body.Write(unknown)
	body.WriteString("short")

	stdout, stderr, truncated, err := demultiplex(&body, 1<<10)
	if err != nil {
		t.Fatalf("demultiplex: %v", err)
	}
	if stdout != "kept\n" {
		t.Errorf("stdout = %q, want what was read before the cut", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want an unknown stream mixed into neither", stderr)
	}
	if truncated {
		t.Error("truncated is true, but no stream this result reports lost anything")
	}
}
