package plugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

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
				// looping is issue #804's own fixture: a task whose only work
				// is calling ReportProgress a caller-chosen number of times,
				// cycling through the three real phases, before returning a
				// trivial terminal response. The count travels in the "message"
				// input as a decimal string, reusing Task_Log_Inputs rather
				// than adding a schema type for one test fixture's sake.
				//
				// Three tests drive this task, each with the loop count and
				// config the acceptance criterion it proves needs:
				// [TestProgressLoopDoesNotStarveTheTerminalResponse] (a small
				// MaxResponseBytes and a loop count whose progress frames
				// alone would have exhausted the pre-#804-fix shared budget
				// before this Fn ever returns), [TestProgressFramesPastTheCapAreDropped]
				// (a small MaxProgressFrames and a loop count well past it),
				// and [TestUnboundedProgressFloodIsStillRefused] (both turned
				// down together, and a loop count large enough that the
				// aggregate reserve — finite, however many frames
				// MaxProgressFrames allows — is still exhausted before the
				// loop is).
				Name:    "looping",
				Summary: "reports progress a caller-chosen number of times before returning",
				Input:   &flowstatev1.Task_Log_Inputs{},
				Output:  &flowstatev1.Task_Log_Outputs{},
				Fn: func(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
					n, err := strconv.Atoi(inputs["message"].GetLiteral().GetStringValue())
					if err != nil {
						return nil, fmt.Errorf("looping fixture: bad count: %w", err)
					}
					// Each branch names a declared phase directly, the same
					// discipline [reportWirePhase] (task.go) follows and
					// TestEveryPhaseReportedIsOneOfTheDeclaredOnes
					// (progress_test.go) requires of every ReportProgress call
					// site in the tree: an indexed slice or a variable built
					// from the switch reads identically to that AST walk
					// whether it can only ever hold one of these three
					// constants, as this one can, or whether it holds
					// something built from a task's own inputs — which is
					// exactly the mistake the walk exists to catch.
					for i := 0; i < n; i++ {
						switch i % 3 {
						case 0:
							flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)
						case 1:
							flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)
						case 2:
							flowstatev1.ReportProgress(ctx, flowstatev1.PhaseCallingPlugin)
						}
					}
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

// starvationCallTimeout is the per-call timeout the three tests below run
// under — the ones whose assertion is a *byte* budget: that a task's own
// progress frames cannot spend the share its terminal response needs.
//
// It is deliberately far longer than [testConfig]'s own 3 seconds, and the
// reason is launch_test.go's `timeoutIsTheBound` lesson arriving a second
// time (issue #852). A timeout is the bound under test in exactly one place
// in this package — a plugin that never handshakes — and incidental
// everywhere else, so it must be short there and generous here. These three
// tests stream a thousand-frame flood through a real subprocess, over a real
// Unix socket, under the race detector; that takes about 1.4 seconds on an
// idle machine and comfortably more than 3 on a machine several test
// binaries are sharing. Sharing testConfig's 3 seconds made the deadline do
// two jobs at once — the mechanism *and* the incidental bound — so a loaded
// machine failed them with `deadline_exceeded` having relayed 810 of the
// 1000 frames with nothing starved at all. That is a test reporting a
// scheduling fact in the voice of a correctness one.
//
// Nothing is weakened by the length. A byte bound that stopped applying
// fails these tests on the bytes, in well under a second (measured: with
// transport.go's reserve reverted, the stream is truncated around frame 250
// and connect reports one of its envelope protocol errors), and the
// assertions below are about what crossed the wire rather than about when.
// This timeout's only remaining job is the one a timeout should have here:
// stopping a hang, so a plugin that never answers cannot hold a test open
// until `go test -timeout` kills the whole binary.
const starvationCallTimeout = 60 * time.Second

// progressWireBytes reports what n progress frames of the shape the "looping"
// fixture sends occupy on the wire, envelope included — the same measurement
// [TestProgressFrameWireSizeStaysWithinBudget] makes against
// maxProgressFrameWireBytes, made here so the starvation tests below can
// assert their own premise rather than restate a number from a comment.
//
// The premise is what makes those tests mean anything: unless the progress
// frames alone outgrow MaxResponseBytes, a shared budget would have been
// large enough all along and the call succeeding proves nothing. Deriving it
// is CLAUDE.md's "prefer deriving to duplicating" applied to a test's own
// arithmetic — a schema change that shrinks a frame silently turns a
// hardcoded 1000 into a loop count too small to reach the bound, and this
// fails instead.
func progressWireBytes(t *testing.T, n int) int {
	t.Helper()

	b, err := proto.Marshal(&pluginv1.ExecuteStreamResponse{
		Message: &pluginv1.ExecuteStreamResponse_Progress{
			Progress: &pluginv1.TaskProgress{Phase: pluginv1.TaskPhase_TASK_PHASE_REQUESTING},
		},
	})
	require.NoError(t, err, "marshaling one progress frame")

	return n * (len(b) + 5) // Connect's own streaming envelope: 1 flag byte + 4 length bytes.
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

// TestProgressLoopDoesNotStarveTheTerminalResponse is issue #804's own
// acceptance criterion: a task that reports progress many times in a loop
// must still complete successfully under a deliberately small
// MaxResponseBytes, rather than having its own reporting frequency exhaust
// the budget its terminal response needs.
//
// The numbers are chosen from real measurements, not guesses.
// [TestProgressFrameWireSizeStaysWithinBudget] measures one legitimate
// TaskProgress frame at 9 bytes on the wire (envelope included), and the
// "looping" fixture's own trivial terminal response marshals to the
// identical 9 bytes. 1000 loop iterations therefore put roughly 9000 bytes
// on the wire — comfortably more than the 8192-byte MaxResponseBytes this
// test configures (still generous enough for the unrelated Describe call
// every Open makes to succeed on its own, unaffected share), which is the
// whole point: before #804's fix, boundedTransport (transport.go) capped the
// *entire* ExecuteStream response at that one number, progress and terminal
// response sharing it, so this call would have failed on a progress frame
// around iteration 910, never reaching the terminal response at all.
// Reverting transport.go's progressReserve and task.go's use of it
// reproduces exactly that failure — this test is what caught it before the
// fix landed.
func TestProgressLoopDoesNotStarveTheTerminalResponse(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "progress"))
	cfg.MaxResponseBytes = 8192             // Smaller than the ~9000 bytes 1000 progress frames plus the terminal response add up to.
	cfg.CallTimeout = starvationCallTimeout // The bound under test is the byte budget above, not the clock — see the constant.

	host := openHost(t, cfg)
	def := findTaskDef(t, host, "progress.looping")

	reporter := &recordingReporter{}
	ctx := flowstatev1.ContextWithProgress(t.Context(), reporter.report)

	const loopCount = 1000

	// The premise, asserted rather than left to the comment above: the
	// progress frames alone have to outgrow MaxResponseBytes, or a shared
	// budget would have sufficed and everything below passes for a reason
	// that is not this test's.
	require.Greater(t, progressWireBytes(t, loopCount), cfg.MaxResponseBytes,
		"this test asserts nothing unless %d progress frames outgrow MaxResponseBytes (%d) on their own",
		loopCount, cfg.MaxResponseBytes)

	_, err := def.Fn(ctx, map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral(strconv.Itoa(loopCount)),
	}, nil)
	require.NoError(t, err, "a task's own progress-reporting frequency must never starve its terminal response")

	// findTaskDef's call reports PhaseCallingPlugin itself before the RPC even
	// leaves the worker (task.go, unchanged by #804), so the wire relayed
	// loopCount reports on top of that one.
	assert.Len(t, reporter.recorded(), loopCount+1,
		"every progress report from a loop under MaxProgressFrames must reach the caller, "+
			"not merely the call succeeding despite some going missing")
}

// TestProgressFramesPastTheCapAreDropped is CLAUDE.md's own lesson applied to
// this issue: a bound has to be shown *reached*, not merely not exceeded. It
// configures a MaxProgressFrames far smaller than the fixture's loop count
// and asserts the caller's reporter saw exactly that many relayed progress
// reports — not "at least", not "at most", exactly — proving
// [Plugin.executeTask]'s frame counter is the thing doing the dropping,
// rather than the call merely happening to succeed for some unrelated
// reason.
func TestProgressFramesPastTheCapAreDropped(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "progress"))
	cfg.MaxProgressFrames = 5

	host := openHost(t, cfg)
	def := findTaskDef(t, host, "progress.looping")

	reporter := &recordingReporter{}
	ctx := flowstatev1.ContextWithProgress(t.Context(), reporter.report)

	const loopCount = 40 // Eight times the cap, so a bug that merely reduces relaying rather than stopping it at exactly the cap would still be caught.
	_, err := def.Fn(ctx, map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral(strconv.Itoa(loopCount)),
	}, nil)
	require.NoError(t, err, "dropping progress frames past the cap must never fail the call")

	// PhaseCallingPlugin first (task.go's own report, unaffected by the cap),
	// then exactly cfg.MaxProgressFrames of the fixture's own cycle —
	// Requesting, ReadingResponse, CallingPlugin, Requesting, ReadingResponse
	// — and nothing past it, though the fixture called ReportProgress 40
	// times.
	assert.Equal(t,
		[]flowstatev1.Phase{
			flowstatev1.PhaseCallingPlugin,
			flowstatev1.PhaseRequesting,
			flowstatev1.PhaseReadingResponse,
			flowstatev1.PhaseCallingPlugin,
			flowstatev1.PhaseRequesting,
			flowstatev1.PhaseReadingResponse,
		},
		reporter.recorded(),
		"exactly MaxProgressFrames reports must be relayed, no fewer and no more, "+
			"regardless of how many times the fixture actually called ReportProgress",
	)
}

// TestUnboundedProgressFloodIsStillRefused is #804's other acceptance
// criterion: progressReserve (transport.go) is a fixed, small, deliberately
// bounded amount of extra headroom, not a second unlimited budget — a plugin
// whose ExecuteStream output is unbounded, not merely frequent, must still be
// refused once even the enlarged aggregate ceiling is exhausted.
//
// Both MaxResponseBytes and MaxProgressFrames are turned down together here,
// to a combined ceiling small enough that the fixture's 200,000-iteration
// loop cannot possibly complete within it — MaxResponseBytes stays just
// large enough for Open's own unrelated Describe call to succeed (it is
// bounded by the same number, independently: see [newClients]), and at 9
// measured bytes per frame (see [TestProgressFrameWireSizeStaysWithinBudget])
// the combined ceiling below still exhausts in well under a hundred frames.
//
// What tells this apart from the call merely running out its CallTimeout is
// the *shape* of the refusal, not how long it took to arrive. Reaching the
// byte ceiling truncates the stream mid-envelope, which connect reports as
// CodeInvalidArgument ("protocol error: incomplete envelope"); a deadline
// expiring reports CodeDeadlineExceeded and satisfies
// errors.Is(err, context.DeadlineExceeded). A bound that quietly stopped
// applying produces neither: the fixture would run its whole loop (cheaply,
// since the SDK's reporter silently drops every send once the first one
// fails — see sdk.go's taskService.ExecuteStream), return its terminal
// response, and the call would *succeed*, which the require.Error below
// catches on its own.
//
// This used to be an elapsed-time assertion — refused in under 2 seconds,
// against a 3-second CallTimeout — and issue #852 is what that costs: on a
// loaded machine a one-second margin is a coin flip, and the two starvation
// tests beside this one were failing on precisely that. Asking what the
// error *is* answers the same question without asking the machine how busy
// it was.
func TestUnboundedProgressFloodIsStillRefused(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "progress"))
	cfg.MaxResponseBytes = 8192             // Small, but enough headroom for Open's own Describe call to succeed.
	cfg.MaxProgressFrames = 5               // Reserve = 5 * maxProgressFrameWireBytes = 160 bytes; combined ceiling of 8352 exhausts in well under a thousand frames.
	cfg.CallTimeout = starvationCallTimeout // Only a hang-stopper here; the refusal under test is a byte one.

	host := openHost(t, cfg)
	def := findTaskDef(t, host, "progress.looping")

	const loopCount = 200_000 // Far more than the combined ceiling could ever admit.

	_, err := def.Fn(t.Context(), map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral(strconv.Itoa(loopCount)),
	}, nil)

	require.Error(t, err, "a plugin streaming far more than the combined ceiling admits must be refused, "+
		"not accepted on the strength of the reserve #804 adds")
	assert.NotEqual(t, connect.CodeDeadlineExceeded, connect.CodeOf(err),
		"the refusal must come from the byte bound being reached, not from cfg.CallTimeout expiring: %v", err)
	assert.False(t, errors.Is(err, context.DeadlineExceeded),
		"the refusal must come from the byte bound being reached, not from cfg.CallTimeout expiring: %v", err)
}

// TestTaskServiceExecuteStreamDoesNotStarveTheTerminalResponse is Codex's
// review finding on this PR (picatz/flowstate#813): [Plugin.executeTask]
// (task.go) is not the only path to ExecuteStream. [Plugin.TaskService] and
// [Host.TaskServiceForTask] (service.go) hand back the generated
// TaskServiceClient directly — the package's own extension point for a
// caller that wants to drive a plugin's task execution itself, proven live
// by [TestTaskServiceExecuteStreamIsBoundedByCallTimeout] — and
// taskService.ExecuteStream used to dial out through inst.clients.task, the
// unreserved client, rather than inst.clients.taskStream. A caller reaching
// ExecuteStream through this exported surface would have hit #804's exact
// starvation, unfixed, even after task.go's own call site was corrected.
//
// This drives the identical "looping" fixture and small-MaxResponseBytes
// shape [TestProgressLoopDoesNotStarveTheTerminalResponse] does, but through
// [Plugin.TaskService] and a hand-rolled receive loop instead of a TaskDef's
// Fn, so it exercises service.go's call site specifically rather than
// task.go's.
func TestTaskServiceExecuteStreamDoesNotStarveTheTerminalResponse(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "progress"))
	cfg.MaxResponseBytes = 8192             // Smaller than the ~9000 bytes 1000 progress frames plus the terminal response add up to — see TestProgressLoopDoesNotStarveTheTerminalResponse.
	cfg.CallTimeout = starvationCallTimeout // The bound under test is the byte budget above, not the clock — see the constant.

	host := openHost(t, cfg)
	p, ok := host.Lookup("progress")
	require.True(t, ok, "plugin was not launched")

	service, err := p.TaskService()
	require.NoError(t, err, "TaskService")

	const loopCount = 1000

	require.Greater(t, progressWireBytes(t, loopCount), cfg.MaxResponseBytes,
		"this test asserts nothing unless %d progress frames outgrow MaxResponseBytes (%d) on their own",
		loopCount, cfg.MaxResponseBytes)

	stream, err := service.ExecuteStream(t.Context(), connect.NewRequest(&pluginv1.ExecuteStreamRequest{
		Task: &flowstatev1.Task{
			Name:   "looping",
			Inputs: map[string]*flowstatev1.Value{"message": flowstatev1.NewLiteral(strconv.Itoa(loopCount))},
		},
	}))
	require.NoError(t, err, "ExecuteStream")

	// frames counts what arrived ahead of the terminal response, so the
	// assertion below is about the order things crossed the wire in — a
	// flood larger than the whole configured response budget, and the
	// terminal response still behind it — rather than about how long any of
	// it took. This path has no MaxProgressFrames counter of its own (see
	// [taskService.ExecuteStream]'s doc comment for why), so every frame the
	// fixture sent should be here.
	var gotResponse bool
	var frames int
	for stream.Receive() {
		if stream.Msg().GetProgress() != nil {
			frames++
		}
		if stream.Msg().GetResponse() != nil {
			gotResponse = true
			break
		}
	}
	require.NoError(t, stream.Err(), "reading the stream: a task's own progress-reporting frequency must never "+
		"starve its terminal response, on this exported path exactly as on the internal one")
	assert.True(t, gotResponse, "the stream ended without ever delivering the terminal ExecuteResponse")
	assert.Equal(t, loopCount, frames,
		"the terminal response must arrive behind the whole progress flood — %d frames, %d bytes, more than the "+
			"%d byte MaxResponseBytes a pre-#804 shared budget would have spent on them",
		loopCount, progressWireBytes(t, loopCount), cfg.MaxResponseBytes)
	require.NoError(t, stream.Close())
}
