package sdk

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

// TestTaskServiceExecuteInstallsCallerBeforeFn checks the wiring
// [CallerFromContext] depends on: taskService.Execute installs the request's
// identity and namespace on the context before calling Fn, so a task that
// reads them sees what the wire actually carried rather than nothing.
func TestTaskServiceExecuteInstallsCallerBeforeFn(t *testing.T) {
	t.Parallel()

	var gotCaller Caller
	var gotOK bool

	svc := &taskService{tasks: map[string]Task{
		"whoami": {
			Name: "whoami",
			Fn: func(ctx context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				gotCaller, gotOK = CallerFromContext(ctx)
				return &flowstatev1.Node_Outputs{}, nil
			},
		},
	}}

	_, err := svc.Execute(t.Context(), connect.NewRequest(&pluginv1.ExecuteRequest{
		Task:      &flowstatev1.Task{Name: "whoami"},
		Identity:  &flowstatev1.WorkloadIdentity{Subject: "ci", Namespace: "team-a"},
		Namespace: "team-a",
	}))
	require.NoError(t, err)

	require.True(t, gotOK, "Fn ran without a caller installed on its context")
	assert.Equal(t, "ci", gotCaller.Identity.GetSubject())
	assert.Equal(t, "team-a", gotCaller.Namespace)
}

func TestTaskServiceScrubsPanicBeforeConfiguredLogger(t *testing.T) {
	t.Parallel()

	const material = "resolved-secret-in-panic"
	var logs syncBuffer
	ctx := context.WithValue(t.Context(), telemetryKey{}, telemetryContext{
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	svc := &taskService{plugin: "example", tasks: map[string]Task{
		"panic": {
			Name:         "panic",
			SecretInputs: []string{"token"},
			Fn: func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				panic("received " + material)
			},
		},
	}}

	_, err := svc.Execute(ctx, connect.NewRequest(&pluginv1.ExecuteRequest{
		Task: &flowstatev1.Task{
			Name:   "panic",
			Inputs: map[string]*flowstatev1.Value{"token": flowstatev1.NewLiteral(material)},
		},
	}))
	requireUnknownOutcomeWithoutPanicValue(t, err, material)
	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "plugin task panicked; outcome unknown")
	}, time.Second, time.Millisecond)
	assert.NotContains(t, logs.String(), material)
	assert.Contains(t, logs.String(), secrets.Redacted)
}

func TestTaskServicePanicVerdictDoesNotWaitForLogger(t *testing.T) {
	t.Parallel()

	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { writer.once.Do(func() { close(writer.started) }); close(writer.release) })
	ctx := context.WithValue(t.Context(), telemetryKey{}, telemetryContext{
		logger: slog.New(slog.NewTextHandler(writer, nil)),
	})
	svc := &taskService{plugin: "example", tasks: map[string]Task{
		"panic": {
			Name: "panic",
			Fn: func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				panic("boom")
			},
		},
	}}

	done := make(chan error, 1)
	go func() {
		_, err := svc.Execute(ctx, connect.NewRequest(&pluginv1.ExecuteRequest{
			Task: &flowstatev1.Task{Name: "panic"},
		}))
		done <- err
	}()

	select {
	case err := <-done:
		requireUnknownOutcomeWithoutPanicValue(t, err, "boom")
	case <-time.After(time.Second):
		t.Fatal("the unknown-outcome response waited for the configured logger")
	}

	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("the asynchronous panic report never reached the configured logger")
	}
}

// TestTaskConnectErrorMarksOnlyTheInheritedRequestDeadline proves the SDK's
// side of #1233: provenance follows the request context, not the status code.
// That leaves a task's own backend deadline unmarked for the host to classify
// as Upstream.
func TestTaskConnectErrorMarksOnlyTheInheritedRequestDeadline(t *testing.T) {
	t.Parallel()

	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	tests := []struct {
		name        string
		ctx         context.Context
		err         error
		want        bool
		wantUnknown bool
	}{
		{
			name: "request deadline",
			ctx:  expired,
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "plugin-owned deadline while request is live",
			ctx:  t.Context(),
			err:  connect.NewError(connect.CodeDeadlineExceeded, errors.New("backend timed out")),
			want: false,
		},
		{
			name: "unrelated failure after request deadline",
			ctx:  expired,
			err:  connect.NewError(connect.CodeUnavailable, errors.New("backend unavailable")),
			want: false,
		},
		{
			name:        "explicit unknown outcome after request deadline",
			ctx:         expired,
			err:         OutcomeUnknown("commit status: %w", context.DeadlineExceeded),
			want:        false,
			wantUnknown: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var connectErr *connect.Error
			require.ErrorAs(t, taskConnectError(test.ctx, test.err), &connectErr)

			got := false
			gotUnknown := false
			for _, detail := range connectErr.Details() {
				value, detailErr := detail.Value()
				require.NoError(t, detailErr)
				if provenance, ok := value.(*pluginv1.TaskErrorProvenance); ok {
					got = provenance.GetCallerDeadlineExceeded()
				}
				if response, ok := value.(*pluginv1.ExecuteResponse); ok {
					gotUnknown = response.GetUnknownOutcome()
				}
			}
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.wantUnknown, gotUnknown)
		})
	}
}

// TestTaskServiceExecuteInstallsCallerEvenWithNoIdentity checks the
// single-tenant, no-identity-provider case: the caller is still installed,
// carrying a nil Identity and the empty namespace, rather than the request
// leaving nothing for [CallerFromContext] to find.
func TestTaskServiceExecuteInstallsCallerEvenWithNoIdentity(t *testing.T) {
	t.Parallel()

	var gotOK bool

	svc := &taskService{tasks: map[string]Task{
		"whoami": {
			Name: "whoami",
			Fn: func(ctx context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				_, gotOK = CallerFromContext(ctx)
				return &flowstatev1.Node_Outputs{}, nil
			},
		},
	}}

	_, err := svc.Execute(t.Context(), connect.NewRequest(&pluginv1.ExecuteRequest{
		Task: &flowstatev1.Task{Name: "whoami"},
	}))
	require.NoError(t, err)
	assert.True(t, gotOK)
}
