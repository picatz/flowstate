package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What this file is for, and why the tests beside it were not enough.
//
// #523's gap 3 gave the local driver the same `flowstate.task/<name>` span the
// durable driver opens, and every test of it — in `pkg/flowstate/v1`, in
// `engine`, and [TestLocalRunLogLineCarriesTheTraceOfItsTaskSpan] one file over
// — installs its own tracer provider before running the workflow. That proves
// the spans exist and say the right things. It proves nothing at all about the
// command a person types, and the command was broken: [startTelemetry] had two
// callers, [temporalConfig] and the RPC client's constructor, and `flow run
// local` reaches neither. A rehearsal with OTEL_EXPORTER_OTLP_* configured
// recorded every one of those spans into the global no-op provider.
//
// So this tier runs the real binary, in a real process, with the environment an
// operator would set, and reads what arrives at a collector. It is the only
// tier that can see the difference between "the driver opens a span" and "the
// command exports one", which is the entire distance CLAUDE.md's "a capability
// is not done until it is reachable" is about.

// traceCollector keeps the spans a subprocess exported.
type traceCollector struct {
	// URL is the OTLP endpoint to point the subprocess at.
	URL string

	mu    sync.Mutex
	spans []*tracepb.Span
}

// newTraceCollector starts a stub OTLP collector accepting trace exports.
//
// Shaped like [logCollectorTo] one file over, and gzip-aware for the same
// reason that one is: otlptracehttp compresses by default, so a decoder that
// only handled identity would find every body unparseable and report an empty
// collector for a pipeline that was working.
//
// It answers 200 to everything, including the metrics and logs exports the same
// endpoint variable turns on — those are not what this reads, and a stub that
// refused them would make the subprocess spend its shutdown retrying.
func newTraceCollector(t *testing.T) *traceCollector {
	t.Helper()

	collector := &traceCollector{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer w.WriteHeader(http.StatusOK)

		if r.URL.Path != "/v1/traces" {
			return
		}

		if err := collector.accept(r); err != nil {
			t.Errorf("decoding an OTLP trace export: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	collector.URL = server.URL

	return collector
}

// accept decodes one export request and keeps its spans.
func (c *traceCollector) accept(r *http.Request) error {
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		unzipped, err := gzip.NewReader(r.Body)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer unzipped.Close()

		body = unzipped
	}

	raw, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("reading the body: %w", err)
	}

	var request coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(raw, &request); err != nil {
		return fmt.Errorf("unmarshaling: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, resource := range request.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			c.spans = append(c.spans, scope.GetSpans()...)
		}
	}

	return nil
}

// exported returns the spans received so far.
func (c *traceCollector) exported() []*tracepb.Span {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]*tracepb.Span{}, c.spans...)
}

// names lists what arrived, for a failure message that says what happened
// instead of what was wanted.
func (c *traceCollector) names() []string {
	var names []string
	for _, span := range c.exported() {
		names = append(names, span.GetName())
	}

	return names
}

// tracedLocalWorkflow is a workload with two task steps, so a trace that
// arrives with one span is distinguishable from one that arrives whole.
const tracedLocalWorkflow = `edition: v2026.3
name: traced-locally
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
`

// TestALocalRunExportsItsTaskSpans is the end-to-end claim, and the negative of
// what this command did before.
//
// Through the compiled binary rather than in process, because the two things
// being checked are both properties of a *process*: that `flow run local`
// starts telemetry at all, and that [main]'s unconditional [flushTelemetry]
// pushes the batch before exit. A run that lives for a tenth of a second is far
// shorter than a batch exporter's window, so an in-process harness — which
// never reaches [main] — would pass this while the shipped binary exported
// nothing.
func TestALocalRunExportsItsTaskSpans(t *testing.T) {
	collector := newTraceCollector(t)
	bin := buildFlowBinary(t)

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(tracedLocalWorkflow), 0o600))

	cmd := flowBinaryCommand(bin, "run", "local", path)
	cmd.Env = append(cmd.Env,
		"OTEL_EXPORTER_OTLP_ENDPOINT="+collector.URL,
		// Named explicitly so the subprocess cannot inherit an endpoint from
		// whatever this machine has set for one signal and quietly export
		// somewhere else.
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=",
	)

	res := runFlowBinaryWith(t, cmd)
	require.Equal(t, 0, res.ExitCode, "the run failed: %s", res.Output())

	names := collector.names()
	want := v1.TaskSpanName("log")

	count := 0
	for _, name := range names {
		if name == want {
			count++
		}
	}

	require.Equal(t, 2, count,
		"a local run with OTEL_EXPORTER_OTLP_ENDPOINT set exported %d %s spans, want one per task step; everything that arrived: %v",
		count, want, names)
}

// TestATaskInvocationExportsItsTaskSpan is the same claim for the other verb
// that executes locally.
//
// `flow task run` compiles a one-step workflow and hands it to the same
// [v1.RunWithInputs] `flow run local` reaches, so it had the identical hole —
// and taskrun.go's own comment claimed a task's log lines "reach a configured
// collector the same way", which was a sentence about something that could not
// happen. Fixed together, tested together: the next person to add a verb that
// executes in this process has two call sites to match rather than one to
// notice.
func TestATaskInvocationExportsItsTaskSpan(t *testing.T) {
	collector := newTraceCollector(t)
	bin := buildFlowBinary(t)

	cmd := flowBinaryCommand(bin, "task", "run", "log", "--input", "message=one")
	cmd.Env = append(cmd.Env,
		"OTEL_EXPORTER_OTLP_ENDPOINT="+collector.URL,
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=",
	)

	res := runFlowBinaryWith(t, cmd)
	require.Equal(t, 0, res.ExitCode, "the invocation failed: %s", res.Output())

	require.Contains(t, collector.names(), v1.TaskSpanName("log"),
		"a task invocation with a collector configured exported no span for the task it ran; everything that arrived: %v",
		collector.names())
}

// TestALocalRunWithNoCollectorConfiguredExportsNothing is invariant 8 at the
// command: the default is silence, and adding the span code must not have made
// an unconfigured rehearsal reach for a network.
//
// The endpoint variables are cleared rather than left alone, so the assertion is
// about the binary's default and not about whatever this machine exports to.
func TestALocalRunWithNoCollectorConfiguredExportsNothing(t *testing.T) {
	collector := newTraceCollector(t)
	bin := buildFlowBinary(t)

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(tracedLocalWorkflow), 0o600))

	cmd := flowBinaryCommand(bin, "run", "local", path)
	cmd.Env = append(cmd.Env,
		"OTEL_EXPORTER_OTLP_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=",
	)

	res := runFlowBinaryWith(t, cmd)
	require.Equal(t, 0, res.ExitCode, "the run failed: %s", res.Output())

	require.Empty(t, collector.exported(),
		"a run nobody configured for telemetry reached a collector anyway")
	require.NotContains(t, res.Stderr, "WARNING: telemetry",
		"an unconfigured run warned about telemetry it was never asked for")
}

// TestALocalRunSurvivesACollectorThatIsNotThere pins the disposition the
// warning in [runLocalWorkflow] states: a rehearsal is not a tracing session,
// and a bad endpoint costs the trace rather than the run.
func TestALocalRunSurvivesACollectorThatIsNotThere(t *testing.T) {
	bin := buildFlowBinary(t)

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(tracedLocalWorkflow), 0o600))

	cmd := flowBinaryCommand(bin, "run", "local", path)
	cmd.Env = append(cmd.Env,
		// A port nothing is listening on. The exporter is lazy, so this costs
		// the export attempt at flush and nothing before it.
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:1",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=",
	)

	res := runFlowBinaryWith(t, cmd)
	require.Equal(t, 0, res.ExitCode,
		"an unreachable collector failed the rehearsal, which is the trade this command does not make: %s", res.Output())
	require.Contains(t, res.Stdout, `"first"`,
		"the run's own answer went missing behind a telemetry problem: %s", res.Output())
	require.Contains(t, res.Stdout, `"second"`,
		"the run stopped short of its second step: %s", res.Output())
}
