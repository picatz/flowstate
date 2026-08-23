package lsp

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/require"
)

// TestAnUnterminatedHeaderIsRefused is the named regression for what
// [MaxFrameBytes] was added for.
//
// [jsonrpc2.VSCodeObjectCodec] parses the header block with
// `stream.ReadString('\r')`, which accumulates until it finds a carriage return
// or the stream ends. A peer that sends neither is not sending a malformed
// frame the decoder rejects; it is sending a buffer the decoder grows, by
// doubling, for as long as the bytes keep coming. Measured against v0.2.2
// before this bound existed, 256 MiB of such a line left roughly 512 MiB live.
//
// The endless reader here is what makes that a test rather than an anecdote: it
// yields header bytes forever, so a decode that returns at all is a decode
// something bounded. Without [NewBoundedStream] this test does not fail, it
// runs until the machine gives out — which is why it is written against a
// reader with no end rather than against a large fixed input.
func TestAnUnterminatedHeaderIsRefused(t *testing.T) {
	t.Parallel()

	endless := &endlessHeader{}
	stream := NewBoundedStream(readWriteCloser{Reader: endless, Writer: io.Discard})

	var req jsonrpc2.Request
	err := stream.ReadObject(&req)

	require.ErrorIs(t, err, ErrFrameTooLarge,
		"an unterminated header block was not refused; it was buffered")
	require.LessOrEqual(t, endless.n, MaxFrameBytes+bufioReadSlack,
		"the refusal came after reading %d bytes, past the %d byte bound", endless.n, MaxFrameBytes)
}

// TestABoundedFrameStillCarriesADocument is the other direction, and the one
// that decides whether the bound is set somewhere an honest editor can reach.
//
// A `didOpen` carrying a document at [maxDocumentBytes] — the largest one this
// package will analyze — has to fit, escaping and envelope included, or the
// bound has turned a diagnostic into a disconnection.
func TestABoundedFrameStillCarriesADocument(t *testing.T) {
	t.Parallel()

	// Worst-case escaping: every byte of the document is a control character
	// the JSON encoder writes as a six-byte \uXXXX escape.
	document := strings.Repeat("\x01", maxDocumentBytes)

	var wire bytes.Buffer
	out := NewBoundedStream(readWriteCloser{Reader: &wire, Writer: &wire})
	require.NoError(t, out.WriteObject(&jsonrpc2.Request{
		Method: "textDocument/didOpen",
		Notif:  true,
		Params: rawMessage(t, map[string]any{
			"textDocument": map[string]any{
				"uri":  "untitled:big.yaml",
				"text": document,
			},
		}),
	}))
	require.Greater(t, wire.Len(), maxDocumentBytes,
		"the test did not actually build an oversized frame")

	var back jsonrpc2.Request
	require.NoError(t, out.ReadObject(&back),
		"a frame carrying a maximal document was refused; the bound is below what an editor sends")
	require.Equal(t, "textDocument/didOpen", back.Method)
}

// TestTheFrameBudgetIsPerFrame pins that the bound is on one message rather
// than on a session: a connection that carries more than [MaxFrameBytes] in
// total, in frames that are each small, is an ordinary long editing session and
// must not be cut off.
func TestTheFrameBudgetIsPerFrame(t *testing.T) {
	t.Parallel()

	one := frame(`{"jsonrpc":"2.0","method":"textDocument/didSave","params":{}}`)
	stream := NewBoundedStream(readWriteCloser{
		Reader: strings.NewReader(strings.Repeat(one, 64)),
		Writer: io.Discard,
	})

	for i := range 64 {
		var req jsonrpc2.Request
		require.NoErrorf(t, stream.ReadObject(&req), "frame %d of a long session was refused", i)
		require.Equal(t, "textDocument/didSave", req.Method)
	}
}

// rawMessage marshals v into the raw params a request carries.
func rawMessage(t *testing.T, v any) *json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	raw := json.RawMessage(data)
	return &raw
}

// An endlessHeader is a header line that never terminates: no carriage return,
// no end of stream.
type endlessHeader struct{ n int }

func (e *endlessHeader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	e.n += len(p)
	return len(p), nil
}
