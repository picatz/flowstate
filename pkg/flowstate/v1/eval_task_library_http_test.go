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

// TestTheHTTPTaskSpeaksTheProfilesDialect is the last surviving instance of the
// split that retiring `libs:` was supposed to end.
//
// A step could once name its own extension libraries and nothing else in the file
// could, so one step spoke a richer dialect than the rest. The profile replaced that
// with a single membership. This was the same split from the other side: the http
// task's own two expressions were evaluated against `cel.NewEnv(response, json)` —
// the json library and nothing else — so a *poorer* dialect than every other
// position in the language, and one nothing in the grammar mentions.
//
// The cost of that is paid in the worst place. These expressions run *after* the
// request, so `${response.body.upperAscii()}` made the call, got its answer, and then
// failed on a function that works in a `vars:` binding, an `if:`, `items:`,
// `wait_until:` and every other task input.
//
// One case per library rather than one for the whole set, because the failure this
// guards against is a library missing from an environment — which is a per-library
// fact, and a single `upperAscii` would keep passing while the other ten went.
//
// It does not discriminate evenly, and that is worth writing down rather than
// leaving to be discovered. Reintroducing the old environment fails eight of these
// eleven; `bindings`, `comprehensions` and `json` still pass, the first two because
// their macros expand into core comprehensions that need no library at run time, and
// `json` because it was the one library the old environment had. So those three
// cases are documentation of the membership rather than a guard on it.
func TestTheHTTPTaskSpeaksTheProfilesDialect(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		library string
		expr    string
		want    string
	}{
		{library: "strings", expr: `'ok'.upperAscii()`, want: "OK"},
		{library: "digest", expr: `digest.sha256(response.body)`, want: ContentDigest([]byte(`{"n": [3, 1, 2]}`))},
		{library: "encoders", expr: `base64.encode(b'hi')`, want: "aGk="},
		{library: "regex", expr: `regex.replace('a-b', '-', '+')`, want: "a+b"},
		{library: "lists", expr: `string(json_parse(response.body).n.sort()[0])`, want: "1"},
		{library: "math", expr: `string(math.greatest(1, 2))`, want: "2"},
		{library: "sets", expr: `string(sets.contains([1, 2], [1]))`, want: "true"},
		{library: "bindings", expr: `cel.bind(x, 'y', x)`, want: "y"},
		{library: "comprehensions", expr: `string([1, 2].transformList(i, v, v * 2)[1])`, want: "4"},
		{library: "optional", expr: `optional.of('v').value()`, want: "v"},
		{library: "json", expr: `string(json_parse(response.body).n[0])`, want: "3"},
	} {
		t.Run(test.library, func(t *testing.T) {
			t.Parallel()

			// One server per subtest, not one for the table. `httpTaskServer`
			// records the request it received into a struct with no
			// synchronisation, which is right for a test making one request and a
			// data race the moment two of these run at once — as they do, since
			// they are parallel. Found by CI rather than locally: two handler
			// goroutines have to overlap for the detector to see it, and on this
			// machine they did not.
			server, _ := httpTaskServer(t, http.StatusOK, `{"n": [3, 1, 2]}`, http.Header{
				"Content-Type": []string{"application/json"},
			})

			// Through `outputs:`, which is the position that runs after the request
			// and so is the one where the old failure was most expensive.
			out, err := runHTTPTask(t, map[string]any{
				"url":     server.URL,
				"method":  http.MethodGet,
				"outputs": NewExpr(`{"v": ` + test.expr + `}`),
			})
			require.NoError(t, err,
				"a function from the %q library does not exist inside an http `outputs:` expression, "+
					"though it does everywhere else in the language", test.library)

			require.Contains(t, out.GetNamedValues(), "v")
			require.Equal(t, test.want, out.GetNamedValues()["v"].GetLiteral().GetStringValue(),
				"the expression evaluated to the wrong value, so the library is present but not behaving")
		})
	}
}

// TestTheHTTPTasksExpectSpeaksItToo covers the other deferred input.
//
// Written separately rather than folded into the table above because the two reach
// the environment through different functions — `taskFuncHTTP` and
// `httpExpectSatisfied` — and only one of them was fixed by the first attempt.
func TestTheHTTPTasksExpectSpeaksItToo(t *testing.T) {
	t.Parallel()

	server, _ := httpTaskServer(t, http.StatusOK, `{"ok": true}`, http.Header{
		"Content-Type": []string{"application/json"},
	})

	_, err := runHTTPTask(t, map[string]any{
		"url":    server.URL,
		"method": http.MethodGet,
		"expect": NewExpr(`string(response.status_code).startsWith('2') && math.greatest(1, 2) == 2`),
	})

	require.NoError(t, err,
		"an `expect:` expression could not use the profile's functions, though every other position can")
}

// TestTheHTTPTaskSaysWhereItIs is the behavioural half of progress reporting, and
// it is in this package on purpose.
//
// Where the phases *go* is a driver's business — the durable driver puts them in an
// activity heartbeat, the local driver has nowhere to put them and installs
// nothing. Whether the task reports them at all is not: it is a property of the
// task, in the layer both drivers share, and it is the part that rots silently. A
// heartbeat that faithfully carries a phase no task ever sets is indistinguishable
// from one that works, right up until somebody asks what a stuck step is doing.
//
// The order is asserted rather than the set. "Requesting" and "reading the
// response" are a diagnosis precisely because they are sequential — a step stuck on
// the first is waiting for a peer that has said nothing, and one stuck on the
// second has a peer that answered and then stopped talking. Reported in the wrong
// order they would be worse than absent.
func TestTheHTTPTaskSaysWhereItIs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(5*time.Second))
	require.NoError(t, err)

	var heard []string
	ctx := ContextWithProgress(t.Context(), func(phase Phase) {
		heard = append(heard, phase.String())
	})

	_, err = taskFuncHTTP(policy)(ctx, NewNamedValues(map[string]any{
		"url":    srv.URL,
		"method": http.MethodGet,
	}), nil)
	require.NoError(t, err)

	require.Equal(t, []string{"requesting", "reading the response"}, heard,
		"the http task no longer says where it is, so a step stuck on it is opaque again")
}
