package flowstatev1

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/stretchr/testify/require"
)

// Test_taskFuncHTTP_transportFailureIdempotency covers the difference between a
// request that never reached the server and one whose outcome is unknown.
//
// Before this distinction existed, every transport failure was retriable, so a POST
// that was received and then timed out was retried — performing the operation a
// second time rather than retrying a failure.
func Test_taskFuncHTTP_transportFailureIdempotency(t *testing.T) {
	var received atomic.Int32

	// Accepts the request, then never answers: the side effect happened, the caller
	// cannot know it.
	hanging := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(hanging.Close)

	// A port with nothing listening, so the dial fails and nothing is sent.
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedURL := "http://" + closed.Addr().String()
	require.NoError(t, closed.Close())

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(150*time.Millisecond))
	require.NoError(t, err)

	fn := taskFuncHTTP(policy)

	tests := []struct {
		name      string
		method    string
		url       string
		wantKind  ErrorKind
		wantRetry bool
	}{
		// The case that mattered: sent, unknown outcome, not safe to repeat.
		{
			name:     "POST whose response never came is not retried",
			method:   http.MethodPost,
			url:      hanging.URL,
			wantKind: ErrorKindUpstreamUnknown,
		},
		{
			name:     "PATCH whose response never came is not retried",
			method:   http.MethodPatch,
			url:      hanging.URL,
			wantKind: ErrorKindUpstreamUnknown,
		},

		// Idempotent by definition, so a repeat has the same effect.
		{
			name:      "GET whose response never came is retried",
			method:    http.MethodGet,
			url:       hanging.URL,
			wantKind:  ErrorKindUpstream,
			wantRetry: true,
		},
		{
			name:      "PUT whose response never came is retried",
			method:    http.MethodPut,
			url:       hanging.URL,
			wantKind:  ErrorKindUpstream,
			wantRetry: true,
		},
		{
			name:      "DELETE whose response never came is retried",
			method:    http.MethodDelete,
			url:       hanging.URL,
			wantKind:  ErrorKindUpstream,
			wantRetry: true,
		},

		// Never reached the server, so it cannot have taken effect: still retriable
		// even for POST, which keeps "the server is not up yet" working.
		{
			name:      "POST that never connected is retried",
			method:    http.MethodPost,
			url:       closedURL,
			wantKind:  ErrorKindUpstream,
			wantRetry: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := fn(t.Context(), NewNamedValues(map[string]any{
				"method": test.method,
				"url":    test.url,
				"body":   `{"charge":"100.00"}`,
			}), nil)

			var taskErr *TaskError
			require.ErrorAs(t, err, &taskErr)
			require.Equal(t, test.wantKind.String(), taskErr.Kind.String())
			require.Equal(t, test.wantRetry, taskErr.Retryable())
		})
	}

	t.Run("a body-read failure after a success is not retried for POST", func(t *testing.T) {
		// The status said it worked; only reading the reply failed. Retrying would
		// perform the operation a second time, and here the first one is known to
		// have completed rather than merely suspected. This is also the normal way a
		// chunked or event-stream response breaks, so it stops being an edge case as
		// soon as a response is anything but one buffered body.
		truncating := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1024")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("partial"))

			// Break the connection mid-body, leaving the declared length unmet.
			if hijacker, ok := w.(http.Hijacker); ok {
				conn, _, err := hijacker.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
		}))
		t.Cleanup(truncating.Close)

		for _, test := range []struct {
			method    string
			wantKind  ErrorKind
			wantRetry bool
		}{
			{method: http.MethodPost, wantKind: ErrorKindUpstreamUnknown},
			{method: http.MethodPatch, wantKind: ErrorKindUpstreamUnknown},
			{method: http.MethodGet, wantKind: ErrorKindUpstream, wantRetry: true},
			{method: http.MethodPut, wantKind: ErrorKindUpstream, wantRetry: true},
		} {
			t.Run(test.method, func(t *testing.T) {
				_, err := fn(t.Context(), NewNamedValues(map[string]any{
					"method": test.method,
					"url":    truncating.URL,
				}), nil)

				var taskErr *TaskError
				require.ErrorAs(t, err, &taskErr)
				require.Equal(t, test.wantKind.String(), taskErr.Kind.String())
				require.Equal(t, test.wantRetry, taskErr.Retryable())

				if !test.wantRetry {
					require.ErrorContains(t, err, "took effect but its result is lost")
				}
			})
		}
	})

	t.Run("an unknown outcome is permanent to the engine", func(t *testing.T) {
		// The engine derives the substrate's non-retryable list from this, so the
		// kind has to be in it or the classification changes nothing in practice.
		require.Contains(t, PermanentErrorKinds(), ErrorKindUpstreamUnknown)
		require.NotContains(t, RetryableErrorKinds(), ErrorKindUpstreamUnknown)
	})

	t.Run("the message says the outcome is unknown", func(t *testing.T) {
		_, err := fn(t.Context(), NewNamedValues(map[string]any{
			"method": http.MethodPost,
			"url":    hanging.URL,
		}), nil)

		require.ErrorContains(t, err, "whether it took effect is unknown")
		require.True(t, errors.Is(err, err), "sanity")
	})
}

// TestHTTPOutputsAreCostBounded is the coverage test the cost limit did not have.
//
// The limit itself was tested — TestEvaluatorCostLimit exercises the Evaluator and
// passes. What nothing checked was whether every evaluation site goes through it,
// and the http task's `outputs:` did not: it built a cel.Program by hand with no
// ProgramOption and ran it with Eval rather than ContextEval, so it had neither a
// cost limit nor a way to be cancelled.
//
// That is the expression an author most directly controls. A nest of `.map()`
// comprehensions — about twenty-five characters of YAML per factor of ten — ran to
// completion past the step's own timeout, and because the program never observed
// the context, a timed-out attempt kept running while Temporal scheduled the next
// one.
//
// This is the "test that A cannot reach B" mistake on a different axis: a test of
// the mechanism standing in for a test of its coverage. So this asserts the
// property at the site rather than at the evaluator.
func TestHTTPOutputsAreCostBounded(t *testing.T) {
	t.Parallel()

	server, _ := httpTaskServer(t, http.StatusOK, `{"n": [1,2,3,4,5,6,7,8,9,10]}`, http.Header{
		"Content-Type": []string{"application/json"},
	})

	// Nested comprehensions over ten elements: 10^5 iterations, which is far past
	// the cost limit and far too fast to be caught by any timeout.
	const explosive = `{"boom": json_parse(response.body).n.map(a, json_parse(response.body).n.map(b, ` +
		`json_parse(response.body).n.map(c, json_parse(response.body).n.map(d, ` +
		`json_parse(response.body).n.map(e, a + b + c + d + e)))))}`

	_, err := runHTTPTask(t, map[string]any{
		"url":     server.URL,
		"method":  http.MethodGet,
		"outputs": NewExpr(explosive),
	})

	require.Error(t, err, "an unbounded outputs expression ran to completion")
	require.Contains(t, err.Error(), "cost limit",
		"the evaluation was stopped, but not by the cost limit — so the bound may not be the thing that stopped it: %v", err)
}

// TestHTTPOutputsAreCancellable is the other half of the same defect.
//
// A bound on cost does not make an evaluation stoppable, and the two failures are
// different: one is an author asking for too much work, the other is a run being
// told to stop while the work is in flight. `Eval` cannot observe a context at all,
// so a cancelled run left the goroutine running.
func TestHTTPOutputsAreCancellable(t *testing.T) {
	t.Parallel()

	server, _ := httpTaskServer(t, http.StatusOK, `{"n": [1,2,3,4,5,6,7,8,9,10]}`, http.Header{
		"Content-Type": []string{"application/json"},
	})

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(5*time.Second))
	require.NoError(t, err)

	// Cancelled before the task runs, so the evaluation must refuse to start
	// rather than run to completion and then notice.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = taskFuncHTTP(policy)(ctx, NewNamedValues(map[string]any{
		"url":     server.URL,
		"method":  http.MethodGet,
		"outputs": NewExpr(`{"n": json_parse(response.body).n}`),
	}), nil)

	require.Error(t, err, "a cancelled run still evaluated an outputs expression")
}
