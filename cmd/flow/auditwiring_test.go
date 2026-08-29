package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureRealStderr redirects the process's actual os.Stderr for the life of
// the test and returns a way to read what landed on it.
//
// Needed specifically for the audit trail's default sink: [audit.NewRecorder]
// writes to the literal os.Stderr rather than through any *cobra.Command's
// configurable error writer, which is the whole point — an audit record must
// reach a caller who redirected the command's own output and forgot auditing
// existed. That means [newSurface]'s captured stream (what
// devHTTPServer/Temporal's own banner and warnings go through in these tests)
// never sees an audit line, and a test asserting against it would be asserting
// against a stream this feature deliberately bypasses.
func captureRealStderr(t *testing.T) func() string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err, "opening the pipe this test redirects the real stderr through")

	original := os.Stderr
	os.Stderr = w

	buf := &syncWriter{}
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(buf, r)
		close(copied)
	}()

	t.Cleanup(func() {
		os.Stderr = original
		_ = w.Close()
		<-copied
		_ = r.Close()
	})

	return buf.String
}

// isolateAudit gives a test the audit state a fresh process has, and restores
// whatever was there afterward.
//
// Same shape as [isolateTelemetry], for the same reason: [startAudit] memoizes
// the recorder once per process, and a test that built one under a particular
// --audit-required/OTEL_* combination must not hand the next test that same
// recorder.
func isolateAudit(t *testing.T) {
	t.Helper()

	auditState.mu.Lock()
	started, recorder, shutdown, err := auditState.started, auditState.recorder, auditState.shutdown, auditState.err
	auditState.started, auditState.recorder, auditState.shutdown, auditState.err = false, nil, nil, nil
	auditState.mu.Unlock()

	t.Cleanup(func() {
		flushAudit()

		auditState.mu.Lock()
		auditState.started, auditState.recorder, auditState.shutdown, auditState.err = started, recorder, shutdown, err
		auditState.mu.Unlock()
	})
}

// TestServerDevAuditsAuthorizedAndDeniedDecisions runs a real `flow server
// dev` stack and reaches it through the connect handler exactly as a caller
// would, then checks that both an authorized and a refused request left a
// record on the always-on stderr sink — end to end, not a call into
// pkg/flowstate/v1/audit directly.
func TestServerDevAuditsAuthorizedAndDeniedDecisions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: needs a Temporal dev server; CI runs the full suite")
	}

	for _, name := range []string{
		"FLOWSTATE_ADDRESS", "FLOWSTATE_AUTH_POLICY", "TEMPORAL_ADDRESS", "TEMPORAL_PROFILE",
		"TEMPORAL_CONFIG_FILE", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
		"OTEL_LOGS_EXPORTER",
	} {
		t.Setenv(name, "")
	}
	isolateAudit(t)

	dir := t.TempDir()
	workflow := filepath.Join(dir, scaffoldWorkflow)
	require.NoError(t, runFlow(t, "init", dir).Err, "scaffolding the workflow the run is about")

	// Captures the process's real os.Stderr, which is where the audit trail's
	// default sink writes regardless of what any *cobra.Command's own error
	// writer is set to — see [captureRealStderr].
	stderr := captureRealStderr(t)

	out, errOut := &syncWriter{}, &syncWriter{}
	root := newRootCommand()
	root.SetOut(out)
	root.SetErr(errOut)
	root.SetArgs([]string{"server", "dev", "--listen", "localhost:0", "--ui-port", "0", "-o", "json"})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	stopped := make(chan error, 1)
	go func() { stopped <- root.ExecuteContext(ctx) }()

	stack, err := awaitDevStack(t, out, stopped)
	if err != nil {
		if !devServerUnavailable(err) {
			t.Fatalf("`flow server dev` failed to start, and not for want of a Temporal binary: %v", err)
		}
		t.Skipf("SKIPPING the audit gate: this environment cannot start a Temporal dev server (%v). "+
			"That is an environment limitation, not a passing test: nothing below this line ran.", err)
	}

	// The allow: a real run, authorized under workload.write (the Run RPC).
	run := runFlow(t, "run", workflow, "--address", stack.FlowstateAddress)
	require.NoError(t, run.Err, "the durable run: %s", run.Output())

	// The deny: a Get for a workflow id nothing ever started. authorizeRun
	// denies with AUDIT_DENY_CODE_RESOURCE_NOT_FOUND even for an anonymous
	// caller — see authorizeRunDecision in pkg/flowstate/v1/server/lifecycle.go.
	get := runFlow(t, "get", "flowstate-workflow-does-not-exist-0000", "--address", stack.FlowstateAddress)
	require.Error(t, get.Err, "a Get for a run nobody started must be refused")

	cancel()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-t.Context().Done():
		t.Fatal("`flow server dev` did not return")
	}

	trail := stderr()

	// protojson's default naming keeps single-word proto field names as-is:
	// "rpc", "action", "decision", "denyCode". One line per decision, and the
	// two decisions under test have to each appear at least once.
	assert.Contains(t, trail, `"rpc":"Run"`, "the allowed Run decision must reach the stderr audit sink:\n%s", trail)
	assert.Contains(t, trail, `"decision":"AUDIT_DECISION_ALLOW"`,
		"an authorized decision must be recorded as an allow:\n%s", trail)
	assert.Contains(t, trail, `"rpc":"Get"`, "the denied Get decision must reach the stderr audit sink:\n%s", trail)
	assert.Contains(t, trail, `"decision":"AUDIT_DECISION_DENY"`,
		"a refused decision must be recorded as a deny:\n%s", trail)
	assert.Contains(t, trail, `"denyCode":"AUDIT_DENY_CODE_RESOURCE_NOT_FOUND"`,
		"the deny record must carry the code the decision was refused under, not the caller's prose:\n%s", trail)

	// The deny record's own line is closed JSON with the field set
	// audit.proto declares (asserted structurally in the audit package's own
	// TestTheRecordHasNoFieldAPayloadCouldGoIn): no key here can hold the
	// refusal's prose, only the code above and the identifiers a caller
	// addressed. What this end-to-end test adds is that the wiring actually
	// reaches that structural guarantee through a real request rather than a
	// constructed record.
}

// TestAuditRequiredStopsARequestWhenTheSinkFails drives the same wiring under
// a sink that cannot be written: an OTel logs endpoint nothing listens on.
// With --audit-required the request must fail before any mutation; without
// it, the same failure is best-effort and the request succeeds.
func TestAuditRequiredStopsARequestWhenTheSinkFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: needs a Temporal dev server; CI runs the full suite")
	}

	for _, name := range []string{"FLOWSTATE_ADDRESS", "FLOWSTATE_AUTH_POLICY",
		"TEMPORAL_ADDRESS", "TEMPORAL_PROFILE", "TEMPORAL_CONFIG_FILE"} {
		t.Setenv(name, "")
	}

	// Reachable enough to build an exporter, not enough to ever answer:
	// otlploghttp fails the export with a connection refusal rather than
	// waiting out a timeout, which is what keeps this test fast.
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_LOGS_EXPORTER", "")

	cases := []struct {
		name     string
		required bool
		check    func(t *testing.T, get flowResult)
	}{
		{
			name:     "without --audit-required, a down sink only logs",
			required: false,
			check: func(t *testing.T, get flowResult) {
				t.Helper()
				require.Error(t, get.Err, "the resource still does not exist")
				assert.NotContains(t, get.Err.Error(), "audit:",
					"a best-effort recorder must not turn a sink outage into the caller's error: %s", get.Err)
			},
		},
		{
			name:     "with --audit-required, a down sink fails the request",
			required: true,
			check: func(t *testing.T, get flowResult) {
				t.Helper()
				require.Error(t, get.Err)
				assert.Contains(t, get.Err.Error(), "audit:",
					"a required recorder's sink failure must be the caller's own error, not the resource's: %s", get.Err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateAudit(t)

			out, errOut := &syncWriter{}, &syncWriter{}
			root := newRootCommand()
			root.SetOut(out)
			root.SetErr(errOut)

			args := []string{"server", "dev", "--listen", "localhost:0", "--ui-port", "0", "-o", "json"}
			if tc.required {
				args = append(args, "--"+auditRequiredFlag)
			}
			root.SetArgs(args)

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)

			stopped := make(chan error, 1)
			go func() { stopped <- root.ExecuteContext(ctx) }()

			stack, err := awaitDevStack(t, out, stopped)
			if err != nil {
				if !devServerUnavailable(err) {
					t.Fatalf("`flow server dev` failed to start, and not for want of a Temporal binary: %v", err)
				}
				t.Skipf("SKIPPING: this environment cannot start a Temporal dev server (%v).", err)
			}

			get := runFlow(t, "get", "flowstate-workflow-does-not-exist-0000", "--address", stack.FlowstateAddress)
			tc.check(t, get)

			cancel()
			select {
			case err := <-stopped:
				require.NoError(t, err)
			case <-t.Context().Done():
				t.Fatal("`flow server dev` did not return")
			}
		})
	}
}
