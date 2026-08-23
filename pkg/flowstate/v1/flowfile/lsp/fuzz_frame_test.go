package lsp

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/require"
)

// maxFuzzFrames bounds how many frames one execution decodes.
//
// The loop's progress is measured in frames, and how many frames a given number
// of bytes yields is the *writer's* choice — a two-byte `Content-Length: 1`
// body per frame means an input of a few kilobytes decodes into thousands of
// them. CLAUDE.md's rule for a loop whose units the far side decides is to
// count the round trips as well, and this is that count: the byte budget below
// bounds the memory, this bounds the work.
const maxFuzzFrames = 64

// FuzzLSPFrames fuzzes the language server's frame path: the bytes an editor
// writes to `flow lsp`'s standard input, as they are read by the stream the
// command actually builds ([NewBoundedStream]), and the request each decoded
// frame becomes as it reaches the one piece of this repository's own code that
// runs on the connection's read loop.
//
// This is picatz/flowstate#403's item 3, and it is the half of that item
// [FuzzLSPDocumentEdits] deliberately did not take. That target drives the
// analyzers directly and says why: [FlowfileServer.Handle] converts a panic
// into a JSON-RPC error, so a crash reached through the connection would be
// reported as an error and a fuzzer looking through it would be blind. That
// argument is exactly right about `Handle` and exactly wrong about everything
// upstream of it, which is what this target covers:
//
//   - The frame decode itself, where the bytes are the peer's and the bound is
//     the one this repository had to add — see [MaxFrameBytes] for the
//     unbounded header read it exists for, and [TestAnUnterminatedHeaderIsRefused]
//     for the measured case.
//   - [FlowfileServer.announceInbound], which runs on the read loop *before*
//     the `go` in [asyncHandler.Handle] and is therefore outside the recover
//     entirely. It JSON-decodes params the peer chose. A panic there does not
//     become a protocol error; it kills the read loop's goroutine and takes the
//     process with it.
//
// # The properties
//
// **Bounded.** No frame is allowed to consume more than [MaxFrameBytes], which
// is asserted directly by counting what the underlying reader was asked for.
// The whole input is capped at that bound too, for the same reason
// [FuzzWebhookEventBinding] caps a body at `MaxWebhookPayloadBytes`: bytes are
// the resource the peer controls, so bytes are what a target that means to stay
// inside GOMEMLIMIT has to bound where the real reader does.
//
// **A decoded frame round-trips.** Anything the codec accepts, re-encoded by
// the same codec and read back, is the same request — same method, same id,
// same notification-or-not, same params bytes. This is the property a frame
// decoder has: framing is supposed to be transparent, and a decoder that loses
// or invents a byte between those two points has broken the envelope rather
// than the payload. Note what is *not* claimed: that re-encoding reproduces the
// input bytes. It cannot — the header block a peer sends is not canonical
// (`Content-Type` is optional, whitespace varies), so the round trip is on the
// decoded message, not on the wire form.
//
// **Every announce retires.** `announceInbound` registers that a document build
// is in flight and returns the function that retires it. A registration that is
// never retired holds every `await` for that URI to its full build timeout, so
// the store having no entries left at the end of an execution is a real claim
// about a real failure mode, checkable with no oracle for what any of these
// bytes meant.
//
// **No panic, no hang.** The usual pair, and the reason the whole thing is
// driven synchronously off a [bytes.Reader] rather than over a live connection:
// a panic here is a test failure rather than something a recover somewhere
// converts into a reply nobody reads.
func FuzzLSPFrames(f *testing.F) {
	for _, seed := range lspFrameSeeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		// The bound the real reader applies per frame, applied here to the
		// whole input: one execution may not cost more than one frame's worth
		// of bytes, whatever the fuzzer arranged them into.
		if len(raw) > MaxFrameBytes {
			t.Skip("input past the frame bound this target holds itself to")
		}

		counted := &countingReader{r: bytes.NewReader(raw)}
		stream := NewBoundedStream(readWriteCloser{Reader: counted, Writer: io.Discard})
		defer func() { _ = stream.Close() }()

		server := &FlowfileServer{Logger: discardLogger()}

		for frames := 0; frames < maxFuzzFrames; frames++ {
			before := counted.n

			var req jsonrpc2.Request
			if err := stream.ReadObject(&req); err != nil {
				// Every refusal is an ordinary answer: malformed headers, a
				// body that is not a JSON-RPC object, or the end of the input.
				// There is no oracle here for which bytes should decode.
				break
			}

			require.LessOrEqualf(t, counted.n-before, MaxFrameBytes+bufioReadSlack,
				"one frame read %d bytes, past the %d byte bound", counted.n-before, MaxFrameBytes)

			requireFrameRoundTrips(t, &req)

			// The read-loop half of dispatch, which no recover covers.
			server.announceInbound(&req)()
		}

		server.docs.mu.Lock()
		building := len(server.docs.building)
		server.docs.mu.Unlock()
		require.Zerof(t, building,
			"%d document build registrations were left behind; an unretired registration "+
				"holds every await for that URI to its full build timeout", building)
	})
}

// bufioReadSlack is how far past [MaxFrameBytes] a single frame's reads may run.
//
// [frameLimitReader] counts bytes pulled from the connection, and the
// [bufio.Reader] above it fills in buffer-sized chunks, so the last fill before
// the limit trips can carry a buffer's worth the codec never consumes. One
// default bufio buffer is the whole of the overshoot; see the type's own doc
// comment.
const bufioReadSlack = 4096

// requireFrameRoundTrips fails unless re-encoding req and reading it back
// yields the same request.
func requireFrameRoundTrips(t *testing.T, req *jsonrpc2.Request) {
	t.Helper()

	var buf bytes.Buffer
	rt := NewBoundedStream(readWriteCloser{Reader: &buf, Writer: &buf})
	require.NoError(t, rt.WriteObject(req), "a decoded request would not re-encode")

	var back jsonrpc2.Request
	require.NoError(t, rt.ReadObject(&back), "a re-encoded request would not decode")

	require.Equal(t, req.Method, back.Method, "the method did not survive a frame round trip")
	require.Equal(t, req.Notif, back.Notif, "notification-or-not did not survive a frame round trip")
	require.Equal(t, req.ID, back.ID, "the id did not survive a frame round trip")
	require.Equal(t, rawParams(req), rawParams(&back), "the params did not survive a frame round trip")
}

// rawParams renders a request's params as bytes, with an absent params and a
// null params distinguished — the difference decides whether a handler sees a
// value at all.
func rawParams(req *jsonrpc2.Request) string {
	if req.Params == nil {
		return "<absent>"
	}
	return string(*req.Params)
}

// A readWriteCloser assembles a stream out of a reader and a writer, which is
// what the language server's own transport is: reads come from standard input
// and writes go to standard output (see the `stdio` type in cmd/flow).
type readWriteCloser struct {
	io.Reader
	io.Writer
}

func (readWriteCloser) Close() error { return nil }

// A countingReader records how many bytes were pulled through it, which is what
// makes the frame bound assertable rather than merely configured.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// frame renders a JSON body as the wire form an editor sends.
func frame(body string) string {
	return fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)
}

// lspFrameSeeds are the byte sequences the fuzzer explores outward from.
//
// Real frames, taken from what the harness in this package actually sends over
// a connection — an initialize handshake, a didOpen, a didChange, a hover — so
// that the fuzzer starts from inputs that decode *and* dispatch rather than
// from garbage that dies in the header block. Each is then joined by the
// deliberately malformed ones, because the interesting mutations are the ones
// that cross that boundary in either direction.
var lspFrameSeeds = [][]byte{
	// The handshake, then a document, then a request about it: the shortest
	// byte sequence that is a plausible editor session.
	[]byte(frame(`{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"rootUri":"untitled:/","capabilities":{}}}`) +
		frame(`{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"untitled:a.yaml","languageId":"yaml","version":1,"text":"edition: v2026.3\nname: n\nsteps:\n- id: a\n  log:\n    message: hi\n"}}}`) +
		frame(`{"jsonrpc":"2.0","id":1,"method":"textDocument/hover","params":{"textDocument":{"uri":"untitled:a.yaml"},"position":{"line":5,"character":6}}}`)),
	// An incremental edit, which is the notification #403 names and the one
	// announceInbound registers a build for.
	[]byte(frame(`{"jsonrpc":"2.0","method":"textDocument/didChange","params":{"textDocument":{"uri":"untitled:a.yaml","version":2},"contentChanges":[{"range":{"start":{"line":5,"character":13},"end":{"line":5,"character":15}},"text":"bye"}]}}`)),
	// A didChange whose params are structurally fine and semantically empty,
	// and one whose uri is missing: the two shapes announceInbound is
	// documented as registering nothing for.
	[]byte(frame(`{"jsonrpc":"2.0","method":"textDocument/didChange","params":{}}`)),
	[]byte(frame(`{"jsonrpc":"2.0","method":"textDocument/didChange","params":{"textDocument":{"version":9}}}`)),
	// Params that are not an object at all, and params cut off mid-value —
	// the malformed-params case handler_internal_test.go pins by hand.
	[]byte(frame(`{"jsonrpc":"2.0","method":"textDocument/didOpen","params":[1,2,3]}`)),
	[]byte(frame(`{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":`)),
	// Header-block shapes. A Content-Type alongside the length (which real
	// editors send), a length that disagrees with the body in both directions,
	// a zero length, a negative one, and one past what a uint32 holds.
	[]byte("Content-Length: 33\r\nContent-Type: application/vscode-jsonrpc; charset=utf-8\r\n\r\n" + `{"jsonrpc":"2.0","method":"x"}`),
	[]byte("Content-Length: 1000\r\n\r\n" + `{"jsonrpc":"2.0","method":"x"}`),
	[]byte("Content-Length: 4\r\n\r\n" + `{"jsonrpc":"2.0","method":"x"}`),
	[]byte("Content-Length: 0\r\n\r\n"),
	[]byte("Content-Length: -1\r\n\r\n{}"),
	[]byte("Content-Length: 99999999999999999999\r\n\r\n{}"),
	// No header at all, a header with the wrong line ending (the one case the
	// codec rejects with a message of its own), and a header block that never
	// terminates — the shape [MaxFrameBytes] exists for, here at a size that
	// costs nothing so the fuzzer can grow it.
	[]byte(`{"jsonrpc":"2.0","method":"x"}`),
	[]byte("Content-Length: 2\n\n{}"),
	[]byte("Content-Length: " + string(bytes.Repeat([]byte("9"), 4096))),
	// Two frames where the second is truncated mid-body, which is what a
	// disconnect looks like: the first must still decode and dispatch.
	[]byte(frame(`{"jsonrpc":"2.0","method":"initialized","params":{}}`) + "Content-Length: 500\r\n\r\n{\"jsonrpc\""),
	// Empty input, which must terminate immediately rather than spin.
	{},
}
