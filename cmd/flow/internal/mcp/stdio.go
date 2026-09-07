package mcp

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Stdio is the transport `flow mcp` serves over: newline-delimited JSON-RPC
// on stdin and stdout, where one line the host got wrong fails that line and
// not the session (#1289).
//
// The SDK's own [mcp.StdioTransport] ends the connection on the first line it
// cannot decode: a line that is not JSON, a JSON-RPC batch on a protocol
// version that forbids one, an object that is not a JSON-RPC message. Its
// read loop treats any read failure as the peer going away, so a host's one
// serialization slip destroyed every in-flight tool call, a running
// `flowstate_run_local` included, with nothing on stdout to say why. JSON-RPC
// 2.0 §4.2 asks for a -32700 or -32600 error response and a session that
// continues.
//
// So the lines are read here first. Each one is decoded the way the SDK
// would decode it, and one the SDK would refuse is answered on stdout with
// the error the specification names and never forwarded; the rest reach the
// SDK unchanged through a pipe, so its session handling, batching for the
// versions that still allow it, and cancellation are exactly what they were.
// The fix belongs upstream — the read loop is the SDK's — and this is the
// wrapping this repository can do in the meantime, recorded on the issue.
func Stdio() mcp.Transport {
	return &lineTransport{in: os.Stdin, out: os.Stdout}
}

// maxStdioLineBytes bounds one inbound line. The SDK's decoder reads a
// message of any length into memory; a host is one trusted process, but a
// bound on what one line may cost is still the rule everywhere else a peer
// controls the size of what this program reads. Sized above the largest
// argument any tool takes — a whole workflow specification and a run's
// inputs — with room to spare.
const maxStdioLineBytes = 64 << 20

// protocolVersionBatchesRefused is the protocol version from which JSON-RPC
// batching is refused, the SDK's own rule: batches are legal on the versions
// before it and forwarded there.
const protocolVersionBatchesRefused = "2025-06-18"

// lineTransport is [Stdio] over any reader and writer, which is what the tests
// drive.
type lineTransport struct {
	in  io.Reader
	out io.Writer

	// maxLine bounds one inbound line; zero is [maxStdioLineBytes]. A field
	// so a test can reach the bound without writing sixty-four megabytes.
	maxLine int
}

// Connect implements [mcp.Transport].
func (t *lineTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	out := &sessionWriter{w: t.out}
	pr, pw := io.Pipe()
	go filterLines(t.in, pw, out, cmp.Or(t.maxLine, maxStdioLineBytes))

	return (&mcp.IOTransport{Reader: pr, Writer: out}).Connect(ctx)
}

// sessionWriter is the one writer to stdout, shared by the SDK's connection
// and the filter, so that a line from either is written whole. It also reads
// the initialize result on its way out to learn the negotiated protocol
// version, which decides whether a batch is forwarded or refused — the SDK
// tells its own connection through an interface this package cannot
// implement, so the version is taken from the wire instead.
type sessionWriter struct {
	mu              sync.Mutex
	w               io.Writer
	protocolVersion string
}

// Write implements [io.Writer]. Called by the SDK with one message per call,
// newline included, which is what makes the lock enough.
func (s *sessionWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.protocolVersion == "" {
		var initialized struct {
			Result struct {
				ProtocolVersion string `json:"protocolVersion"`
			} `json:"result"`
		}
		if json.Unmarshal(p, &initialized) == nil && initialized.Result.ProtocolVersion != "" {
			s.protocolVersion = initialized.Result.ProtocolVersion
		}
	}
	return s.w.Write(p)
}

// Close implements [io.Closer] for [mcp.IOTransport]; stdout is not this
// package's to close.
func (s *sessionWriter) Close() error { return nil }

// version is the negotiated protocol version, or empty before initialize
// has been answered.
func (s *sessionWriter) version() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.protocolVersion
}

// refuse writes a JSON-RPC error response for a line that will not be
// forwarded. id is the request's id when the line carried one, and null
// otherwise, which is what the specification says a parse error answers with.
func (s *sessionWriter) refuse(id json.RawMessage, code int, message string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	line, err := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{
		JSONRPC: "2.0",
		ID:      id,
		Error: struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}{Code: code, Message: message},
	})
	if err != nil {
		return
	}
	// Errors are not reported: a stdout the host has closed ends the session
	// through the SDK's own write on its next message, and there is nothing
	// this side could do with the failure first.
	_, _ = s.Write(append(line, '\n'))
}

// filterLines reads stdin line by line, forwards each line the SDK would
// accept to the pipe it reads, and answers each one it would not. It closes
// the pipe when stdin ends, which is how the SDK learns the host is gone.
func filterLines(in io.Reader, forward *io.PipeWriter, out *sessionWriter, maxLine int) {
	reader := bufio.NewReader(in)
	for {
		line, tooLong, err := readLine(reader, maxLine)
		if tooLong {
			out.refuse(nil, jsonrpc.CodeInvalidRequest,
				fmt.Sprintf("the message is longer than the %d bytes one line may carry; it was not read", maxLine))
		} else if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
			if reason, code, id := lineRefusal(trimmed, out.version()); reason != "" {
				out.refuse(id, code, reason)
			} else if _, werr := forward.Write(append(trimmed, '\n')); werr != nil {
				// The SDK closed its side: the session is over.
				return
			}
		}
		if err != nil {
			_ = forward.CloseWithError(err)
			return
		}
	}
}

// readLine reads up to and including the next newline. A line longer than
// max is consumed to its end and reported as too long rather than returned in
// part, so the bytes after the bound are never mistaken for the next message.
func readLine(reader *bufio.Reader, max int) (line []byte, tooLong bool, err error) {
	for {
		chunk, rerr := reader.ReadSlice('\n')
		if !tooLong {
			if len(line)+len(chunk) > max {
				tooLong = true
				line = nil
			} else {
				line = append(line, chunk...)
			}
		}
		switch {
		case rerr == nil:
			return line, tooLong, nil
		case errors.Is(rerr, bufio.ErrBufferFull):
			continue
		default:
			return line, tooLong, rerr
		}
	}
}

// lineRefusal decides whether one non-empty line is forwarded. It returns the
// refusal to answer with, the JSON-RPC code for it, and the request id the
// line carried when it carried one; an empty reason forwards the line.
//
// The checks are the SDK's own, run ahead of it: [jsonrpc.DecodeMessage] on
// each message is what its connection would call, and the batch rule is the
// one its connection applies. A line that passes here is one the SDK will
// accept, so its session cannot end on a decode.
func lineRefusal(line []byte, protocolVersion string) (reason string, code int, id json.RawMessage) {
	if !json.Valid(line) {
		return "parse error: the line is not valid JSON", jsonrpc.CodeParseError, nil
	}

	if line[0] == '[' {
		if protocolVersion >= protocolVersionBatchesRefused {
			return fmt.Sprintf("JSON-RPC batching is not supported in %s and later (negotiated version: %s)",
				protocolVersionBatchesRefused, protocolVersion), jsonrpc.CodeInvalidRequest, nil
		}
		var members []json.RawMessage
		if err := json.Unmarshal(line, &members); err != nil || len(members) == 0 {
			return "invalid request: an empty batch", jsonrpc.CodeInvalidRequest, nil
		}
		for _, member := range members {
			if _, err := jsonrpc.DecodeMessage(member); err != nil {
				return "invalid request: a batch member is not a JSON-RPC message: " + err.Error(),
					jsonrpc.CodeInvalidRequest, nil
			}
		}
		return "", 0, nil
	}

	if _, err := jsonrpc.DecodeMessage(line); err != nil {
		return "invalid request: " + err.Error(), jsonrpc.CodeInvalidRequest, requestID(line)
	}
	return "", 0, nil
}

// requestID is the id a JSON object carries when it is one JSON-RPC allows —
// a string or a number — so a refusal can name the request it answers.
func requestID(line []byte) json.RawMessage {
	var envelope struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(line, &envelope) != nil || len(envelope.ID) == 0 {
		return nil
	}
	switch envelope.ID[0] {
	case '"', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return envelope.ID
	}
	return nil
}
