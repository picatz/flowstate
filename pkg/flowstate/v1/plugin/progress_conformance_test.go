package plugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// Issue #790: a plugin's own [flowstatev1.ReportProgress] calls never reached
// the host, because a plugin runs as a separate subprocess and nothing in the
// plugin protocol relayed a report across that boundary. `flow watch` showed
// one frozen phase — PhaseCallingPlugin — for a call's whole duration
// regardless of how long it took or what it was doing.
//
// # Why a real subprocess rather than an in-process call
//
// The gap this fixes lives entirely in the wire crossing: [sdk.Run]'s
// TaskService.ExecuteStream handler installs a reporter on the context it
// hands a task's own Fn, and [Plugin.executeTask] on the host side reads
// whatever that handler streams back and forwards it to whatever reporter
// the *caller's* context carries. Calling a task function directly, or
// calling this SDK's handler method in-process, would exercise neither the
// serialization that turns a Phase into a TaskProgress message nor the
// deserialization that turns one back — which is exactly the shape of bug
// this issue reports, so [runProgressPlugin] is a real SDK plugin served
// through [sdk.Run] over the plugin protocol's real Unix socket, the same
// pattern [runErrorsPlugin] in task_conformance_test.go uses for the
// identical reason.

// runProgressPlugin serves the progress-relay conformance fixture: a real SDK
// plugin whose one task reports two phases, with a plain return between them,
// before returning.
//
// It runs in the subprocess [TestMain] hands to a host that launched this
// binary under the `progress` name.
func runProgressPlugin() int {
	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "progress",
		Version:     "0.0.1",
		Description: "a fixture plugin that reports progress before returning",
		Tasks: []sdk.Task{
			{
				Name:    "reporting",
				Summary: "reports two phases, in order, before returning",
				Input:   &flowstatev1.Task_Log_Inputs{},
				Output:  &flowstatev1.Task_Log_Outputs{},
				Fn: func(ctx context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
					flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)
					flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)
					return &flowstatev1.Node_Outputs{}, nil
				},
			},
			{
				// telemetry_probe is the fixture
				// TestExecuteStreamPropagatesTelemetry drives: it writes what
				// the plugin process itself observed — whether [sdk.Tracer]
				// came back non-nil, and the trace ID of the span the SDK's
				// own telemetryInterceptor started — to the file its
				// "message" input names, so the host-side test can read it
				// back and compare against the trace ID of the span the host
				// started before calling in.
				Name:    "telemetry_probe",
				Summary: "records whether an incoming trace propagated, for TestExecuteStreamPropagatesTelemetry",
				Input:   &flowstatev1.Task_Log_Inputs{},
				Output:  &flowstatev1.Task_Log_Outputs{},
				Fn: func(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
					path := inputs["message"].GetLiteral().GetStringValue()
					sc := trace.SpanContextFromContext(ctx)
					line := fmt.Sprintf("tracer=%v traceid=%s\n", sdk.Tracer(ctx) != nil, sc.TraceID().String())
					if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
						return nil, err
					}
					return &flowstatev1.Node_Outputs{}, nil
				},
			},
			{
				// app_unimplemented is the fixture TestExecuteStreamNeverRerunsATaskOnItsOwnApplicationError
				// exercises: it does real, observable work — appending a line to
				// the file its "message" input names — and then fails with the
				// exact Connect code (CodeUnimplemented) an unregistered
				// ExecuteStream route would also answer with. A host that told
				// the two apart by error code alone would rerun this Fn a second
				// time on the strength of an error it deliberately chose to
				// return, which is exactly what CAPABILITY_TASK_PROGRESS-based
				// dispatch (rather than probe-and-fallback) exists to make
				// impossible: see [Plugin.executeTask]'s own doc comment.
				Name:    "app_unimplemented",
				Summary: "does real work, then fails with CodeUnimplemented on purpose",
				Input:   &flowstatev1.Task_Log_Inputs{},
				Output:  &flowstatev1.Task_Log_Outputs{},
				Fn: func(_ context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
					path := inputs["message"].GetLiteral().GetStringValue()
					f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
					if err != nil {
						return nil, err
					}
					_, writeErr := f.WriteString("ran\n")
					closeErr := f.Close()
					if writeErr != nil {
						return nil, writeErr
					}
					if closeErr != nil {
						return nil, closeErr
					}

					return nil, connect.NewError(connect.CodeUnimplemented,
						errors.New("this backend does not support this operation"))
				},
			},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "progress fixture: %v\n", err)
		return 1
	}

	return 0
}

// findTaskDef returns the def a host serves under name, for a test that needs
// to call it directly rather than through a registry.
func findTaskDef(t *testing.T, host *Host, name string) flowstatev1.TaskDef {
	t.Helper()

	for _, def := range host.TaskDefs() {
		if def.Name == name {
			return def
		}
	}

	t.Fatalf("host does not serve task %q", name)
	return flowstatev1.TaskDef{}
}

// recordingReporter is a [flowstatev1.ContextWithProgress] reporter that
// records every phase it is given, in order, safe for the concurrent access
// the real reporter installed by [engine.withHeartbeat] has to tolerate too
// (a task's own goroutine reports; nothing here assumes it always will be the
// same one that reads the result back).
type recordingReporter struct {
	mu     sync.Mutex
	phases []flowstatev1.Phase
}

func (r *recordingReporter) report(p flowstatev1.Phase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases = append(r.phases, p)
}

func (r *recordingReporter) recorded() []flowstatev1.Phase {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]flowstatev1.Phase, len(r.phases))
	copy(out, r.phases)
	return out
}

// TestPluginProgressCrossesTheSubprocessBoundary is issue #790's acceptance
// criterion: a plugin's own ReportProgress calls reach the host's reporter,
// proven across a real subprocess rather than an in-process call.
//
// [Plugin.taskFunc] always reports PhaseCallingPlugin itself before the RPC
// leaves the worker (task.go:152, unchanged by this issue) — that is not
// this fixture's doing, and it is why the reporter sees it first regardless
// of what the plugin goes on to say. What issue #790 was about is everything
// after it: without the fix, that is the only phase this test would ever
// see, no matter how long the fixture ran or how many times it called
// ReportProgress — which is exactly [TestPluginThatNeverReportsProgress]'s
// case, run against a fixture that (unlike this one) never calls it at all.
func TestPluginProgressCrossesTheSubprocessBoundary(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "progress")))
	def := findTaskDef(t, host, "progress.reporting")

	reporter := &recordingReporter{}
	ctx := flowstatev1.ContextWithProgress(t.Context(), reporter.report)

	_, err := def.Fn(ctx, nil, nil)
	require.NoError(t, err, "executing the plugin task")

	assert.Equal(t,
		[]flowstatev1.Phase{
			flowstatev1.PhaseCallingPlugin,
			flowstatev1.PhaseRequesting,
			flowstatev1.PhaseReadingResponse,
		},
		reporter.recorded(),
		"the plugin's own progress reports did not reach the host's reporter, in order, across the subprocess boundary",
	)
}

// TestPluginThatNeverReportsProgressIsUnaffected is issue #790's other
// acceptance criterion: a plugin that never calls ReportProgress, or whose
// build predates ExecuteStream, behaves exactly as it did before this issue
// was fixed — one fixed phase, PhaseCallingPlugin, for the call's whole
// duration, and nothing else.
//
// The "ok" fixture plugin (helper_test.go's fakeTaskService) never
// advertises CAPABILITY_TASK_PROGRESS, which is what a plugin built before
// this issue's fix looks like from the host's side: [Plugin.executeTask]
// reads that once, from the manifest, and never attempts ExecuteStream for
// this plugin at all — calling Execute, unary, exactly as every call went
// before ExecuteStream existed.
func TestPluginThatNeverReportsProgressIsUnaffected(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))
	p, ok := host.Lookup("ok")
	require.True(t, ok, "plugin was not launched")
	require.False(t, p.HasCapability(pluginv1.Capability_CAPABILITY_TASK_PROGRESS),
		"the fixture must not advertise the capability, or this test is not exercising the fallback path")

	def := findTaskDef(t, host, "ok.ok_task")

	reporter := &recordingReporter{}
	ctx := flowstatev1.ContextWithProgress(t.Context(), reporter.report)

	outputs, err := def.Fn(ctx, map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral("hi"),
	}, nil)
	require.NoError(t, err, "executing the plugin task")
	assert.Equal(t, "hi", outputs.GetNamedValues()["result"].GetLiteral().GetStringValue(),
		"the task's own result did not survive the plain Execute path")

	assert.Equal(t,
		[]flowstatev1.Phase{flowstatev1.PhaseCallingPlugin},
		reporter.recorded(),
		"a plugin that never reports progress must show exactly the phase the host always reported, nothing more",
	)
}

// TestExecuteStreamNeverRerunsATaskOnItsOwnApplicationError is Codex's review
// finding on this PR (picatz/flowstate#803): a task built with the SDK is
// free to fail an RPC with connect.CodeUnimplemented directly — the same code
// an unregistered ExecuteStream route answers with — after doing real,
// non-idempotent work. A host that used the error code alone to decide
// "this plugin does not implement ExecuteStream, retry on Execute" would run
// that task's Fn a second time on the strength of an error the task chose on
// purpose. [Plugin.executeTask] reads CAPABILITY_TASK_PROGRESS from the
// manifest instead of probing, so this never happens; this test proves it by
// counting how many times the fixture task's side effect actually occurred.
func TestExecuteStreamNeverRerunsATaskOnItsOwnApplicationError(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "progress")))
	p, ok := host.Lookup("progress")
	require.True(t, ok, "plugin was not launched")
	require.True(t, p.HasCapability(pluginv1.Capability_CAPABILITY_TASK_PROGRESS),
		"the fixture must advertise the capability, or this test is not exercising ExecuteStream at all")

	def := findTaskDef(t, host, "progress.app_unimplemented")

	counter := filepath.Join(t.TempDir(), "ran")

	_, err := def.Fn(t.Context(), map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral(counter),
	}, nil)
	require.Error(t, err, "the task was supposed to fail")

	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr, "the task's own connect.Error must survive to the caller")
	assert.Equal(t, connect.CodeUnimplemented, connectErr.Code(),
		"the task's own chosen code must reach the caller unchanged")

	data, readErr := os.ReadFile(counter)
	require.NoError(t, readErr, "the task's side effect never ran at all")
	assert.Equal(t, "ran\n", string(data),
		"the task's Fn ran more than once: its own CodeUnimplemented was mistaken for a missing route")
}

// TestExecuteStreamPropagatesTelemetry is Codex's round-3 finding on this PR
// (picatz/flowstate#803): CAPABILITY_TASK_PROGRESS routes every task call
// through ExecuteStream now, but both propagationInterceptor (this package,
// telemetry.go) and the SDK's telemetryInterceptor (plugin/sdk/telemetry.go)
// used to be built as connect.UnaryInterceptorFunc, whose streaming wrappers
// Connect no-ops. A plugin's task therefore ran with no trace/baggage header
// on the wire, no server span, and sdk.Tracer(ctx) == nil — a real
// observability regression the moment a plugin advertised the capability
// this PR adds.
//
// This drives the identical path [TestPluginProgressCrossesTheSubprocessBoundary]
// does — a real subprocess over the real Unix socket, CAPABILITY_TASK_PROGRESS
// confirmed so ExecuteStream is definitely the route taken — and proves
// propagation rather than merely exercising the code: the host starts a real
// span before calling in, and the plugin process (a different OS process
// entirely, sharing no globals with this test) writes back the trace ID of
// the span its own telemetryInterceptor started from the incoming header.
// The two trace IDs matching is only possible if the header crossed the
// socket at all.
func TestExecuteStreamPropagatesTelemetry(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	cfg := testConfig(t, pluginDir(t, "progress"))
	cfg.TracerProvider = provider

	host := openHost(t, cfg)
	p, ok := host.Lookup("progress")
	require.True(t, ok, "plugin was not launched")
	require.True(t, p.HasCapability(pluginv1.Capability_CAPABILITY_TASK_PROGRESS),
		"the fixture must advertise the capability, or this test is not exercising ExecuteStream at all")

	def := findTaskDef(t, host, "progress.telemetry_probe")

	probe := filepath.Join(t.TempDir(), "probe")

	_, err := def.Fn(t.Context(), map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral(probe),
	}, nil)
	require.NoError(t, err, "executing the plugin task")

	// The host's telemetry records more than this one call — launching the
	// plugin and opening the host both produce spans of their own — so pick
	// out the execute span this call itself produced rather than assuming
	// it is the only one recorded.
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	var executeSpan *tracetest.SpanStub
	for i, stub := range stubs {
		if stub.Name == "flowstate.plugin.execute" {
			executeSpan = &stubs[i]
		}
	}
	require.NotNil(t, executeSpan, "the host must have recorded a span for this call")
	wantTraceID := executeSpan.SpanContext.TraceID().String()
	require.NotEqual(t, trace.TraceID{}.String(), wantTraceID, "the host's own span must carry a real trace ID")

	data, readErr := os.ReadFile(probe)
	require.NoError(t, readErr, "the plugin process never wrote its probe, which means its Fn never ran")

	got := strings.TrimSpace(string(data))
	assert.Equal(t, fmt.Sprintf("tracer=true traceid=%s", wantTraceID), got,
		"the plugin process must see a non-nil Tracer and a span continuing the host's own trace — "+
			"if this shows tracer=false or a mismatched trace ID, the streaming path is not propagating telemetry")
}
