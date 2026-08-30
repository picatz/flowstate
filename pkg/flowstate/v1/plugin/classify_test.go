package plugin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestKindForCode checks the mapping from a plugin's status code onto the
// engine's error kinds, which is what decides whether a step is attempted again.
//
// Getting it wrong is expensive in both directions: a transient failure
// classified as permanent fails a run that would have succeeded, and a permanent
// one classified as transient spends a step's whole retry budget re-asking a
// question whose answer cannot change.
func TestKindForCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want flowstatev1.ErrorKind
	}{
		{
			name: "invalid argument is bad inputs",
			err:  connect.NewError(connect.CodeInvalidArgument, errors.New("x")),
			want: flowstatev1.ErrorKindInvalidInput,
		},
		{
			name: "permission denied is policy, not inputs",
			err:  connect.NewError(connect.CodePermissionDenied, errors.New("x")),
			want: flowstatev1.ErrorKindPolicyDenied,
		},
		{
			name: "unauthenticated is policy",
			err:  connect.NewError(connect.CodeUnauthenticated, errors.New("x")),
			want: flowstatev1.ErrorKindPolicyDenied,
		},
		{
			name: "unimplemented means the plugin does not have the task",
			err:  connect.NewError(connect.CodeUnimplemented, errors.New("x")),
			want: flowstatev1.ErrorKindUnknownTask,
		},
		{
			name: "not found means the plugin does not have the task",
			err:  connect.NewError(connect.CodeNotFound, errors.New("x")),
			want: flowstatev1.ErrorKindUnknownTask,
		},
		{
			name: "unavailable is transient",
			err:  connect.NewError(connect.CodeUnavailable, errors.New("x")),
			want: flowstatev1.ErrorKindUpstream,
		},
		{
			name: "resource exhausted is transient, unlike a response too large to read",
			err:  connect.NewError(connect.CodeResourceExhausted, errors.New("x")),
			want: flowstatev1.ErrorKindUpstream,
		},
		{
			name: "a plugin-owned deadline is transient upstream failure",
			err:  connect.NewError(connect.CodeDeadlineExceeded, errors.New("backend timed out")),
			want: flowstatev1.ErrorKindUpstream,
		},
		{
			name: "cancellation reported by the plugin is not the task's fault",
			err:  connect.NewError(connect.CodeCanceled, errors.New("x")),
			want: flowstatev1.ErrorKindUpstream,
		},
		{
			name: "the caller's own cancellation is not the plugin's fault",
			err:  context.Canceled,
			want: flowstatev1.ErrorKindUpstream,
		},
		{
			// Not the plugin's fault either, and since #1147 the deadline on a
			// plugin call *is* the step's `timeout:` — so this is the engine's
			// own bound being reached, which is what both drivers answer
			// [flowstatev1.ErrorKindTimeout] for when they end the attempt
			// themselves rather than letting the call return (#915). Upstream
			// gave one fact two names depending on which side of that race won.
			name: "the caller's own deadline is the step's bound, not a dependency failing",
			err:  context.DeadlineExceeded,
			want: flowstatev1.ErrorKindTimeout,
		},
		{
			name: "an unclassified failure",
			err:  errors.New("something"),
			want: flowstatev1.ErrorKindInternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := kindForCode(test.err); got != test.want {
				t.Errorf("kindForCode(%v) = %s, want %s", test.err, got, test.want)
			}
		})
	}
}

// TestTaskErrorClassifiesCallerDeadlineInEitherRaceOrdering pins #1233 without
// racing clocks. The local ordering returns context.DeadlineExceeded directly;
// the peer ordering returns the same inherited deadline as a Connect status
// carrying the serving side's provenance detail. A plugin-owned deadline has
// the identical status but no provenance and must remain Upstream.
func TestTaskErrorClassifiesCallerDeadlineInEitherRaceOrdering(t *testing.T) {
	t.Parallel()

	peerDeadline := connect.NewError(connect.CodeDeadlineExceeded, errors.New("context deadline exceeded"))
	detail, err := connect.NewErrorDetail(&pluginv1.TaskErrorProvenance{CallerDeadlineExceeded: true})
	require.NoError(t, err)
	peerDeadline.AddDetail(detail)

	tests := []struct {
		name string
		err  error
		want flowstatev1.ErrorKind
	}{
		{
			name: "host timer observed first",
			err:  context.DeadlineExceeded,
			want: flowstatev1.ErrorKindTimeout,
		},
		{
			name: "propagated peer timer observed first",
			err:  peerDeadline,
			want: flowstatev1.ErrorKindTimeout,
		},
		{
			name: "plugin-owned backend deadline",
			err:  connect.NewError(connect.CodeDeadlineExceeded, errors.New("backend timed out")),
			want: flowstatev1.ErrorKindUpstream,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := taskError("some_task", "some-plugin", test.err, secrets.NewScrubber())
			var taskErr *flowstatev1.TaskError
			require.ErrorAs(t, got, &taskErr)
			assert.Equal(t, test.want, taskErr.Kind)
		})
	}
}

// TestTaskErrorKeepsTheCause checks that a plugin's own verdict on retrying does
// not overwrite what the failure was.
//
// The two are separate questions, and only the plugin can answer the first. Using
// its answer for both is how a permission failure ends up reported to a workflow
// author as "inputs that do not satisfy the task's schema", telling them to fix
// inputs that are fine.
func TestTaskErrorKeepsTheCause(t *testing.T) {
	t.Parallel()

	withDetail := func(code connect.Code, retryable bool) error {
		err := connect.NewError(code, errors.New("from the plugin"))
		detail, detailErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{Retryable: retryable})
		if detailErr != nil {
			t.Fatalf("building the detail: %v", detailErr)
		}
		err.AddDetail(detail)
		return err
	}

	tests := []struct {
		name          string
		err           error
		wantKind      flowstatev1.ErrorKind
		wantRetryable bool
	}{
		{
			name:          "permanent, and denied",
			err:           withDetail(connect.CodePermissionDenied, false),
			wantKind:      flowstatev1.ErrorKindPolicyDenied,
			wantRetryable: false,
		},
		{
			name:          "permanent, and bad inputs",
			err:           withDetail(connect.CodeInvalidArgument, false),
			wantKind:      flowstatev1.ErrorKindInvalidInput,
			wantRetryable: false,
		},
		{
			// The plugin says its backend failure was transient even though the
			// code alone would not say so. It knows its backend; the host does
			// not.
			name:          "transient, despite an internal code",
			err:           withDetail(connect.CodeInternal, true),
			wantKind:      flowstatev1.ErrorKindUpstream,
			wantRetryable: true,
		},
		{
			// The plugin says permanent and the code-derived kind is retryable.
			// The plugin wins, because it is the one that knows.
			name:          "permanent, despite an unavailable code",
			err:           withDetail(connect.CodeUnavailable, false),
			wantRetryable: false,
		},
		{
			name:          "no verdict at all, with a permanent code",
			err:           connect.NewError(connect.CodePermissionDenied, errors.New("x")),
			wantKind:      flowstatev1.ErrorKindPolicyDenied,
			wantRetryable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := taskError("some_task", "some-plugin", test.err, secrets.NewScrubber())

			var taskErr *flowstatev1.TaskError
			if !errors.As(err, &taskErr) {
				t.Fatalf("taskError returned %T, want a *flowstatev1.TaskError", err)
			}

			if test.wantKind != "" && taskErr.Kind != test.wantKind {
				t.Errorf("kind = %s, want %s", taskErr.Kind, test.wantKind)
			}
			if got := taskErr.Retryable(); got != test.wantRetryable {
				t.Errorf("Retryable = %v (kind %s), want %v", got, taskErr.Kind, test.wantRetryable)
			}

			// The plugin is named, so an operator knows which one to look at.
			if !strings.Contains(err.Error(), "some-plugin") {
				t.Errorf("error = %q, want it to name the plugin", err.Error())
			}
		})
	}
}

// TestOversizedErrorBodyIsBounded checks the bound that Connect's own read limit
// does not provide.
//
// connect.WithReadMaxBytes bounds a successful response. On a failure Connect
// builds a separate unmarshaler for the error body without that limit, so
// without a bound at the transport a plugin could answer any request with a
// failure and a body of its choosing and exhaust the worker's memory — the bound
// would be on the path a hostile plugin would not take.
func TestOversizedErrorBodyIsBounded(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "huge-error"))
	cfg.MaxResponseBytes = 8 << 10 // The fake's error message is 512 KiB.

	host := openHost(t, cfg)

	_, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
	if err == nil {
		t.Fatal("the call succeeded, want a failure")
	}

	// The point is not which error comes back but that the body was never
	// buffered: an error carrying the plugin's half-megabyte message is an error
	// that was read whole.
	if len(err.Error()) > cfg.MaxResponseBytes {
		t.Errorf("the error is %d bytes, so the oversized body was read", len(err.Error()))
	}
	if strings.Count(err.Error(), "zzzzzzzzzz") > 1 {
		t.Errorf("the error carries the plugin's oversized payload: %d bytes", len(err.Error()))
	}
}

// TestBackoffFor checks that the relaunch delay grows, stays inside its cap, and
// is jittered.
func TestBackoffFor(t *testing.T) {
	t.Parallel()

	const (
		base = 100 * time.Millisecond
		max  = time.Second
	)

	t.Run("stays within the cap", func(t *testing.T) {
		t.Parallel()

		for attempt := range 20 {
			got := backoffFor(attempt, base, max)
			if got < 0 {
				t.Fatalf("attempt %d gave a negative delay %v", attempt, got)
			}
			if got > max {
				t.Errorf("attempt %d gave %v, over the %v cap", attempt, got, max)
			}
		}
	})

	t.Run("grows", func(t *testing.T) {
		t.Parallel()

		// Jitter makes any single pair unreliable, so compare the floors: the
		// delay is at least half the doubled interval each time.
		first, later := backoffFor(1, base, max), backoffFor(4, base, max)
		if later <= first {
			t.Errorf("attempt 4 gave %v, want more than attempt 1's %v", later, first)
		}
	})

	t.Run("is jittered", func(t *testing.T) {
		t.Parallel()

		// Several plugins sharing a backend crash together; identical delays
		// would have them relaunch in lockstep and hit it together every time.
		seen := make(map[time.Duration]struct{})
		for range 50 {
			seen[backoffFor(3, base, max)] = struct{}{}
		}
		if len(seen) < 2 {
			t.Errorf("50 delays produced %d distinct values, want jitter", len(seen))
		}
	})

	t.Run("an enormous base does not overflow", func(t *testing.T) {
		t.Parallel()

		// Doubling an already-huge duration wraps to a negative one, and a timer
		// treats that as already elapsed — turning the backoff into none at all.
		got := backoffFor(30, time.Duration(1)<<60, max)
		if got < 0 || got > max {
			t.Errorf("delay = %v, want one inside [0, %v]", got, max)
		}
	})
}
