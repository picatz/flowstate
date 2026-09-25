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

	// maxLogReadBytes bounds the whole stream this call will read, headers and
	// skipped frames included.
	//
	// The grant's own limit bounds what each stream reports. It cannot bound
	// the reading, because reaching a stream still under its limit means
	// reading past one that is not - so without a second ceiling a container
	// writing only to its full stream decides how long this call reads, which
	// is a reporting limit standing in for a work limit.
	maxLogReadBytes = 64 << 20
)

// demultiplex splits a log stream into its two streams, each bounded.
//
// A frame that would take a stream past the limit is kept up to it and the rest
// skipped, and truncated says so: a workflow reading a cut-off stream as a
// complete one is how a check passes on output nobody received. Reading stops
// outright once both streams are full or [maxLogReadBytes] is spent, because
// closing the body is what stops the daemon sending.
func demultiplex(body io.Reader, limit int64) (stdout, stderr string, truncated bool, err error) {
	var out, errOut strings.Builder
	var outBytes, errBytes int64

	header := make([]byte, frameHeaderBytes)
	var read int64
	for {
		if _, readErr := io.ReadFull(body, header); readErr != nil {
			if errors.Is(readErr, io.EOF) {
				// The stream ended where a frame ended, which is what a
				// complete one looks like.
				break
			}
			if errors.Is(readErr, io.ErrUnexpectedEOF) {
				// It ended partway through a header. What was read is real and
				// incomplete, and returning it as a finished result is how a
				// check passes on evidence that was cut off in transit.
				return "", "", false, errIncompleteStream
			}
			return "", "", false, sdk.Unavailable("reading the container's output: %v", readErr)
		}
		read += frameHeaderBytes

		// There is another frame, and one of these says it will not be read:
		// both streams already hold everything they may report, or this call
		// has spent what it will spend reading past them. Either way the rest
		// of the container's output is content this result does not carry.
		if (outBytes >= limit && errBytes >= limit) || read >= maxLogReadBytes {
			truncated = true
			break
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
		}

		// A stream this plugin does not know - stdin echoed back, or a future
		// one - keeps nothing, so written is nil and the whole frame is
		// skipped below rather than guessed at or mixed into either stream.
		var toKeep int64
		if written != nil {
			toKeep = min(length, limit-*written)
		}

		if toKeep > 0 {
			kept, copyErr := io.CopyN(target, body, toKeep)
			read += kept
			*written += kept
			if copyErr != nil {
				return "", "", false, errIncompleteStream
			}
		}
		if toKeep < length {
			if written != nil {
				// A stream this result reports lost content, which is what
				// truncated is for. A skipped unknown stream is not that: no
				// stream reported here is missing anything because of it.
				truncated = true
			}
			skipped, copyErr := io.CopyN(io.Discard, body, length-toKeep)
			read += skipped
			if copyErr != nil {
				return "", "", false, errIncompleteStream
			}
		}
	}

	return text(out.String()), text(errOut.String()), truncated, nil
}

// errIncompleteStream is what a frame the daemon began and did not finish
// earns. It is not truncation: truncation is this plugin deciding it has read
// enough, and this is the other party's connection ending mid-frame, which
// leaves no way to say what the container actually wrote.
var errIncompleteStream = sdk.Unavailable("the container's output ended partway through a frame; what it wrote is not known")

// text is a stream as a string, empty when it is not valid UTF-8: a step output
// is not where arbitrary binary belongs, and a cut at the byte limit can leave
// half a rune behind.
func text(value string) string {
	if !utf8.ValidString(value) {
		return ""
	}
	return value
}
