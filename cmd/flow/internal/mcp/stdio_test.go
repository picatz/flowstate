package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A host session over [lineTransport]: the test is the host, writing lines the
// way a client process does and reading what comes back, so what is asserted
// is the wire and not a Go call.
type hostSession struct {
	t      *testing.T
	toSrv  *io.PipeWriter
	fromSr *bufio.Reader

	done   chan error
	once   sync.Once
	runErr error
}

// wait returns what the server's Run returned, once it has.
func (h *hostSession) wait() error {
	h.once.Do(func() { h.runErr = <-h.done })
	return h.runErr
}

func startHostSession(t *testing.T, maxLine int) *hostSession {
	t.Helper()
	hostIn, srvOut := io.Pipe() // server writes, host reads
	srvIn, hostOut := io.Pipe() // host writes, server reads
	transport := &lineTransport{in: srvIn, out: srvOut, maxLine: maxLine}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- NewServer("test").Run(ctx, transport) }()
	h := &hostSession{t: t, toSrv: hostOut, fromSr: bufio.NewReader(hostIn), done: done}
	t.Cleanup(func() {
		cancel()
		_ = hostOut.Close()
		_ = h.wait()
	})
	return h
}

func (h *hostSession) send(line string) {
	h.t.Helper()
	_, err := io.WriteString(h.toSrv, line+"\n")
	require.NoError(h.t, err)
}

// recv reads one line and decodes it as a generic JSON value.
func (h *hostSession) recv() any {
	h.t.Helper()
	line, err := h.fromSr.ReadString('\n')
	require.NoError(h.t, err, "reading the server's next line")
	var v any
	require.NoError(h.t, json.Unmarshal([]byte(line), &v), "server wrote a line that is not JSON: %q", line)
	return v
}

func (h *hostSession) initialize(version string) {
	h.t.Helper()
	h.send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"` + version +
		`","capabilities":{},"clientInfo":{"name":"host","version":"0"}}}`)
	reply := h.recv().(map[string]any)
	require.Equal(h.t, version, reply["result"].(map[string]any)["protocolVersion"],
		"initialize must negotiate the version the host asked for: %v", reply)
	h.send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}

// errorOf returns a response's error code, message and id, failing when the
// line is not an error response.
func errorOf(t *testing.T, v any) (code float64, message string, id any) {
	t.Helper()
	reply, ok := v.(map[string]any)
	require.True(t, ok, "%v", v)
	e, ok := reply["error"].(map[string]any)
	require.True(t, ok, "not an error response: %v", v)
	return e["code"].(float64), e["message"].(string), reply["id"]
}

// TestOneBadLineFailsThatLineAndNotTheSession is the issue's own reproduction
// (#1289): after a good initialize, the two lines that ended the whole process
// each get the JSON-RPC error the specification names, and a ping on the same
// session is still answered. Against the SDK's own stdio transport the first
// bad line closes stdout and the ping is never read.
func TestOneBadLineFailsThatLineAndNotTheSession(t *testing.T) {
	t.Parallel()

	h := startHostSession(t, 0)
	h.initialize("2025-06-18")

	h.send(`{this is not json}`)
	code, message, id := errorOf(t, h.recv())
	assert.Equal(t, float64(-32700), code)
	assert.Contains(t, message, "parse error")
	assert.Nil(t, id, "a parse error answers with a null id")

	h.send(`[{"jsonrpc":"2.0","id":2,"method":"ping"}]`)
	code, message, id = errorOf(t, h.recv())
	assert.Equal(t, float64(-32600), code)
	assert.Contains(t, message, "batching is not supported in 2025-06-18")
	assert.Contains(t, message, "negotiated version: 2025-06-18")
	assert.Nil(t, id)

	// Valid JSON that is not a JSON-RPC message, with an id the refusal can
	// name.
	h.send(`{"jsonrpc":"1.0","id":7,"method":"ping"}`)
	code, message, id = errorOf(t, h.recv())
	assert.Equal(t, float64(-32600), code)
	assert.Contains(t, message, "invalid request")
	assert.Equal(t, float64(7), id, "the refusal names the request it answers")

	h.send(`{"jsonrpc":"2.0","id":3,"method":"ping"}`)
	reply := h.recv().(map[string]any)
	assert.Equal(t, float64(3), reply["id"], "the session must still answer: %v", reply)
	assert.NotContains(t, reply, "error")
}

// TestABatchIsForwardedOnAVersionThatAllowsIt: the refusal is the SDK's rule
// and not a blanket one, so a host on 2025-03-26 still gets its batch answered
// as an array, through the SDK's own batching.
func TestABatchIsForwardedOnAVersionThatAllowsIt(t *testing.T) {
	t.Parallel()

	h := startHostSession(t, 0)
	h.initialize("2025-03-26")

	h.send(`[{"jsonrpc":"2.0","id":2,"method":"ping"},{"jsonrpc":"2.0","id":3,"method":"ping"}]`)
	replies, ok := h.recv().([]any)
	require.True(t, ok, "a batch is answered with an array")
	require.Len(t, replies, 2)
	ids := []any{replies[0].(map[string]any)["id"], replies[1].(map[string]any)["id"]}
	assert.ElementsMatch(t, []any{float64(2), float64(3)}, ids)

	// A batch with a member that is not a message is refused whole, and the
	// session goes on.
	h.send(`[{"jsonrpc":"2.0","id":4,"method":"ping"},{"nope":true}]`)
	code, _, _ := errorOf(t, h.recv())
	assert.Equal(t, float64(-32600), code)

	h.send(`{"jsonrpc":"2.0","id":5,"method":"ping"}`)
	assert.Equal(t, float64(5), h.recv().(map[string]any)["id"])
}

// TestALineOverTheBoundIsRefusedAndTheRestOfItDiscarded: the bound is on the
// line, the bytes past it are consumed rather than read as the next message,
// and the session continues.
func TestALineOverTheBoundIsRefusedAndTheRestOfItDiscarded(t *testing.T) {
	t.Parallel()

	h := startHostSession(t, 512)
	h.initialize("2025-06-18")

	h.send(`{"jsonrpc":"2.0","id":2,"method":"ping","params":{"pad":"` + strings.Repeat("x", 600) + `"}}`)
	code, message, _ := errorOf(t, h.recv())
	assert.Equal(t, float64(-32600), code)
	assert.Contains(t, message, "longer than the 512 bytes")

	h.send(`{"jsonrpc":"2.0","id":3,"method":"ping"}`)
	assert.Equal(t, float64(3), h.recv().(map[string]any)["id"])
}

// TestStdinEndingEndsTheSession: the one way the session should end is the
// host going away, and it still does.
func TestStdinEndingEndsTheSession(t *testing.T) {
	t.Parallel()

	h := startHostSession(t, 0)
	h.initialize("2025-06-18")
	require.NoError(t, h.toSrv.Close())
	require.NoError(t, h.wait(), "the server returns cleanly when stdin ends")
}

// TestRequestIDNamesOnlyWhatJSONRPCAllows: an id that is an object or a bool
// is not one the refusal may echo, so it answers with null instead.
func TestRequestIDNamesOnlyWhatJSONRPCAllows(t *testing.T) {
	t.Parallel()

	assert.Equal(t, json.RawMessage(`7`), requestID([]byte(`{"id":7}`)))
	assert.Equal(t, json.RawMessage(`"a"`), requestID([]byte(`{"id":"a"}`)))
	assert.Nil(t, requestID([]byte(`{"id":{"x":1}}`)))
	assert.Nil(t, requestID([]byte(`{"id":true}`)))
	assert.Nil(t, requestID([]byte(`{}`)))
}
