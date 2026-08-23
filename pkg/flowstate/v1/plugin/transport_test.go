package plugin

// boundedTransport exists for one attack the surrounding comment names by hand:
// connect.WithReadMaxBytes bounds only a *successful* response, so a hostile
// plugin answering HTTP 500 with an arbitrarily large body would otherwise be
// buffered whole into the worker's memory. The transport wraps every response
// body — whatever its status — in io.LimitReader(body, max+1), so the read is
// capped one byte past the limit: a body at the limit still passes, and one over
// it is caught rather than silently truncated into something that might parse.
//
// Nothing tested this. These tests drive a non-200 response with an oversized
// body through boundedTransport and assert the cap is *reached* — the body the
// caller can read is bounded to max+1, not the full oversized payload.

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// bodyOfSize is a deterministic body of exactly n bytes.
func bodyOfSize(n int) string { return strings.Repeat("A", n) }

// roundTripThroughBound stands up a server that answers `status` with a body of
// `bodySize` bytes, drives one request through a boundedTransport capped at
// `max`, and returns what the caller can read from the response body.
//
// The base is a real *http.Transport (the field's concrete type) reaching a
// loopback httptest server, so the whole path a response takes on the way back —
// the exact place connect's own limit is not present on an error body — is
// exercised, not a stub in place of it.
func roundTripThroughBound(t *testing.T, status, bodySize int, max int64) (*http.Response, []byte) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, bodyOfSize(bodySize))
	}))
	t.Cleanup(srv.Close)

	base := &http.Transport{Proxy: nil} // loopback: no proxy, dial the server directly
	t.Cleanup(base.CloseIdleConnections)

	bt := &boundedTransport{base: base, max: max}

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := bt.RoundTrip(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, got
}

// TestBoundedTransportCapsNon200OversizedBody is the security-critical path: a
// hostile plugin answers HTTP 500 with a body far larger than the cap. The read
// the caller can perform is bounded to max+1 — the extra byte is exactly how
// "over the limit" is detectable — rather than the full oversized payload
// reaching memory.
func TestBoundedTransportCapsNon200OversizedBody(t *testing.T) {
	const max = 1024

	resp, got := roundTripThroughBound(t, http.StatusInternalServerError, 1<<20 /* 1 MiB */, max)

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"the test must exercise the non-200 path connect's own limit misses")
	assert.Len(t, got, max+1,
		"an oversized non-200 body must be capped at max+1, not read whole")
	assert.Less(t, len(got), 1<<20,
		"the full oversized payload must never reach the caller")
}

// TestBoundedTransportCapReachedAtBoundary pins the max+1 contract at its exact
// edge, on a non-200 response:
//
//   - a body of exactly max bytes passes through whole (the limit is inclusive);
//   - a body one byte over max is where the cap first bites, yielding max+1 —
//     the sentinel byte that lets a reader tell "at the limit" from "over it".
func TestBoundedTransportCapReachedAtBoundary(t *testing.T) {
	const max = 4096

	t.Run("body at the limit passes whole", func(t *testing.T) {
		_, got := roundTripThroughBound(t, http.StatusInternalServerError, max, max)
		assert.Len(t, got, max, "a body at exactly the limit must not be truncated")
	})

	t.Run("body one over the limit is capped at max+1", func(t *testing.T) {
		_, got := roundTripThroughBound(t, http.StatusInternalServerError, max+100, max)
		assert.Len(t, got, max+1,
			"a body over the limit is read to max+1, the sentinel that flags 'over'")
	})
}

// theLeakedToken is the per-launch bearer token standing in for a real one in
// TestTokenClientInterceptorNeverReflectsTheToken — distinctive enough that a
// substring match cannot be an accident.
const theLeakedToken = "s3cr3t-per-launch-bearer-token-must-never-print"

// TestTokenClientInterceptorNeverReflectsTheToken is CLAUDE.md's "secrets
// never enter workflow history" containment test, applied to
// [tokenClientInterceptor]: a round-2 fix (6c888eb) added streaming coverage
// by storing the per-launch token directly as a struct field, which reverses
// the guarantee [authInterceptor]'s own doc comment states two paragraphs
// above it — fmt reflects into an unexported field it cannot call a method
// on, and a credential in a field is a credential that prints. Rendering the
// interceptor itself, a struct wrapping one, and a slice of several — the
// three containment shapes CLAUDE.md's own testing standard names — must
// never surface the token under any of %v, %+v, %#v, or %s.
func TestTokenClientInterceptorNeverReflectsTheToken(t *testing.T) {
	interceptor := authInterceptor(theLeakedToken)

	type wrapper struct {
		one   connect.Interceptor
		batch []connect.Interceptor
	}
	w := wrapper{one: interceptor, batch: []connect.Interceptor{interceptor, authInterceptor(theLeakedToken + "-2")}}

	rendered := []string{
		fmt.Sprintf("%v", interceptor),
		fmt.Sprintf("%+v", interceptor),
		fmt.Sprintf("%#v", interceptor),
		fmt.Sprintf("%s", interceptor),
		fmt.Sprintf("%v", w),
		fmt.Sprintf("%+v", w),
		fmt.Sprintf("%#v", w),
		fmt.Sprintf("%v", w.batch),
		fmt.Sprintf("%+v", w.batch),
		fmt.Sprintf("%#v", w.batch),
	}

	for _, r := range rendered {
		assert.NotContains(t, r, theLeakedToken,
			"the per-launch token reached a formatted rendering of the client interceptor")
	}
}

// TestBoundedTransportCapsAllStatuses shows the cap is a property of the
// transport, not of a status: a 200 with an oversized body is bounded the same
// way, so the limit covers the successful path connect also guards and the error
// path it does not, uniformly.
func TestBoundedTransportCapsAllStatuses(t *testing.T) {
	const max = 512

	for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusInternalServerError, http.StatusBadGateway} {
		_, got := roundTripThroughBound(t, status, 64*1024, max)
		assert.Lenf(t, got, max+1, "status %d: oversized body must be capped at max+1", status)
	}
}

// TestProgressFrameWireSizeStaysWithinBudget is what
// maxProgressFrameWireBytes's own doc comment (issue #804) points at rather
// than trusting its arithmetic: it marshals a real ExecuteStreamResponse
// carrying each of TaskPhase's legitimate values, plus the terminal shape the
// "looping" fixture's own trivial response takes
// (progress_conformance_test.go), and asserts every one of them fits inside
// the constant with real margin.
//
// A change that grows TaskProgress or ExecuteStreamResponse enough to close
// that margin fails here first — loudly, in a unit test — rather than
// quietly under-reserving progressReserve below and reintroducing #804 for
// whichever plugin happens to report progress fastest.
func TestProgressFrameWireSizeStaysWithinBudget(t *testing.T) {
	t.Parallel()

	marshal := func(t *testing.T, msg *pluginv1.ExecuteStreamResponse) int {
		t.Helper()
		b, err := proto.Marshal(msg)
		require.NoError(t, err)
		return len(b) + 5 // Connect's own streaming envelope: 1 flag byte + 4 length bytes.
	}

	for _, phase := range []pluginv1.TaskPhase{
		pluginv1.TaskPhase_TASK_PHASE_REQUESTING,
		pluginv1.TaskPhase_TASK_PHASE_READING_RESPONSE,
		pluginv1.TaskPhase_TASK_PHASE_CALLING_PLUGIN,
	} {
		size := marshal(t, &pluginv1.ExecuteStreamResponse{
			Message: &pluginv1.ExecuteStreamResponse_Progress{
				Progress: &pluginv1.TaskProgress{Phase: phase},
			},
		})
		assert.LessOrEqualf(t, size, maxProgressFrameWireBytes,
			"a %s progress frame is %d bytes on the wire, want no more than maxProgressFrameWireBytes (%d)",
			phase, size, maxProgressFrameWireBytes)
	}

	terminalSize := marshal(t, &pluginv1.ExecuteStreamResponse{
		Message: &pluginv1.ExecuteStreamResponse_Response{
			Response: &pluginv1.ExecuteResponse{Outputs: &flowstatev1.Node_Outputs{}},
		},
	})
	assert.LessOrEqualf(t, terminalSize, maxProgressFrameWireBytes,
		"a trivial terminal response is %d bytes on the wire, want no more than maxProgressFrameWireBytes (%d) — "+
			"the tests that rely on it fitting inside a small MaxResponseBytes assume this",
		terminalSize, maxProgressFrameWireBytes)
}

// TestProgressReserveIsFailClosed checks progressReserve's edges directly:
// zero and negative inputs reserve nothing (fail-closed, matching
// [Plugin.executeTask]'s own frame counter — see progressReserve's doc
// comment for why the two have to agree), and a positive count reserves
// exactly that many frames' worth, no more.
func TestProgressReserveIsFailClosed(t *testing.T) {
	t.Parallel()

	assert.Zero(t, progressReserve(0), "a zero frame budget must reserve nothing")
	assert.Zero(t, progressReserve(-1), "a negative frame budget must reserve nothing, not be treated as unlimited")

	const n = 4096 // DefaultMaxProgressFrames.
	assert.Equal(t, int64(n*maxProgressFrameWireBytes), progressReserve(n),
		"the reserve must be exactly frames * one frame's wire budget, the additive amount task.go's own doc comment promises")
}

// TestCheckProgressFrameSizeRefusesAPaddedFrame is the regression for a Codex
// finding on #813: the progress reserve's arithmetic assumes every frame fits
// maxProgressFrameWireBytes, but a frame carrying protobuf unknown fields — a
// schema this build has never seen, or a hostile peer's padding — is bounded
// only by the transport ceiling. Enough of those spend the terminal
// response's own share, recreating the starvation the reserve exists to
// prevent, so the per-frame figure has to be enforced at decode rather than
// assumed from the closed vocabulary.
func TestCheckProgressFrameSizeRefusesAPaddedFrame(t *testing.T) {
	t.Parallel()

	// A legitimate frame — one enum, nothing else — passes with room to spare.
	legit := &pluginv1.ExecuteStreamResponse{
		Message: &pluginv1.ExecuteStreamResponse_Progress{
			Progress: &pluginv1.TaskProgress{Phase: pluginv1.TaskPhase_TASK_PHASE_CALLING_PLUGIN},
		},
	}
	require.NoError(t, checkProgressFrameSize(legit))

	// The same frame padded with an unknown field this build cannot name:
	// what a decode of a newer or hostile peer's frame leaves behind.
	// Field 1000, length-delimited, 4KiB of padding — proto.Size must count
	// it, because the wire did.
	padded := proto.Clone(legit).(*pluginv1.ExecuteStreamResponse)
	unknown := protowire.AppendTag(nil, 1000, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, make([]byte, 4096))
	padded.ProtoReflect().SetUnknown(protoreflect.RawFields(unknown))

	err := checkProgressFrameSize(padded)
	require.Error(t, err, "a frame padded past the per-frame bound must be refused, or the reserve under-reserves")
	require.Contains(t, err.Error(), "per-frame bound")
}
