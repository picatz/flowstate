package main

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// The daemon multiplexes a container's streams over one connection when no TTY
// was allocated, which is always here: every frame is an eight-byte header -
// the stream, three reserved bytes, and a big-endian length - followed by that
// many bytes. Reading the body as text would interleave stdout, stderr and the
// headers themselves into one stream nobody can parse afterwards.
//
// The alternative is asking for a TTY, which merges the streams at the source
// and hands back plain text. This plugin does not: a TTY is a terminal for a
// program that thinks a human is watching, it loses the distinction between
// what a container reported and what it complained about, and allocating one is
// a capability rather than a formatting choice.

const (
	// frameHeaderBytes is the header's size.
	frameHeaderBytes = 8

	// streamStdout and streamStderr are the stream identifiers the daemon puts
	// in the header's first byte.
	streamStdout = 1
	streamStderr = 2

	// maxFrameBytes bounds one frame's declared length before it is read. The
	// header comes from the daemon, and a length nobody checked is an
	// allocation another party chose.
	maxFrameBytes = 16 << 20
)

// demultiplex splits a log stream into its two streams, each bounded.
//
// Reading stops at the first frame that would take either stream past the
// limit, and truncated says so: a workflow reading a cut-off stream as a
// complete one is how a check passes on output nobody received.
func demultiplex(body io.Reader, limit int64) (stdout, stderr string, truncated bool, err error) {
	var out, errOut strings.Builder
	var outBytes, errBytes int64

	header := make([]byte, frameHeaderBytes)
	for {
		if _, readErr := io.ReadFull(body, header); readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
			return "", "", false, sdk.Unavailable("reading the container's output: %v", readErr)
		}

		length := int64(binary.BigEndian.Uint32(header[4:8]))
		if length < 0 || length > maxFrameBytes {
			return "", "", false, sdk.Failed("the container runtime declared a %d-byte output frame", length)
		}

		var target *strings.Builder
		var written *int64
		switch header[0] {
		case streamStdout:
			target, written = &out, &outBytes
		case streamStderr:
			target, written = &errOut, &errBytes
		default:
			// A stream this plugin does not know - stdin echoed back, or a
			// future one. Skipped rather than guessed at, and its bytes are not
			// mixed into either stream.
			if _, copyErr := io.CopyN(io.Discard, body, length); copyErr != nil {
				break
			}
			continue
		}

		remaining := limit - *written
		if remaining <= 0 {
			truncated = true
			if _, copyErr := io.CopyN(io.Discard, body, length); copyErr != nil {
				break
			}
			continue
		}

		toKeep := min(length, remaining)
		if _, copyErr := io.CopyN(target, body, toKeep); copyErr != nil {
			break
		}
		*written += toKeep
		if toKeep < length {
			truncated = true
			if _, copyErr := io.CopyN(io.Discard, body, length-toKeep); copyErr != nil {
				break
			}
		}
	}

	return text(out.String()), text(errOut.String()), truncated, nil
}

// text is a stream as a string, empty when it is not valid UTF-8: a step output
// is not where arbitrary binary belongs, and a cut at the byte limit can leave
// half a rune behind.
func text(value string) string {
	if !utf8.ValidString(value) {
		return ""
	}
	return value
}
