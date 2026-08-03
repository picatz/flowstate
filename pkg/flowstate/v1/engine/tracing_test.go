package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/sdk/testsuite"
)

// What these tests are for.
//
// A span is not like a log line. It leaves this process for a collector, gets
// indexed, and is read by people and systems that have no relationship to the
// run that produced it — so invariant 7 ("secrets never enter workflow history")
// has an exact counterpart here, and it is stricter, because a collector is not
// even tenant-scoped the way Temporal history is.
//
// The containment is therefore tested the way CLAUDE.md demands for fmt: not by
// asserting that the one attribute somebody remembered to check is clean, but by
// rendering the whole recorded span — name, attributes, events, links, and
// status description — through the %v family and requiring the material to be
// absent from every one of them. That is the assertion that survives somebody
// later adding an attribute without reading this file.

// theSecret is the material that must never reach a span, distinctive enough
// that a substring search cannot match it by accident.
const theSecret = "s3cr3t-material-that-must-never-be-exported"

// recordSpans installs a recording tracer provider for the duration of a test
// and returns the recorder.
//
// The global provider, because that is where the engine's spans go — the same
// place otelconnect and the Temporal interceptor read from — and restored
// afterwards, since these tests run in a binary shared with every other engine
// test and a leaked recorder would keep every later span in memory.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return recorder
}

// renderedSpans renders every recorded span through the %v family.
//
// One string per verb, over the whole slice and over each span individually,
// which is the containment shape rather than the containment value: a struct
// printed with %#v reaches fields no accessor exposes, and a slice printed with
// %v is how a set of spans would be dumped by anything debugging them.
func renderedSpans(recorder *tracetest.SpanRecorder) []string {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	rendered := []string{
		fmt.Sprintf("%v", stubs),
		fmt.Sprintf("%+v", stubs),
		fmt.Sprintf("%#v", stubs),
	}

	for _, stub := range stubs {
		rendered = append(rendered,
			fmt.Sprintf("%v", stub),
			fmt.Sprintf("%+v", stub),
			fmt.Sprintf("%#v", stub),
			stub.Name,
			stub.Status.Description,
		)

		for _, attr := range stub.Attributes {
			rendered = append(rendered, string(attr.Key), attr.Value.Emit(),
				fmt.Sprintf("%v", attr), fmt.Sprintf("%#v", attr))
		}

		for _, event := range stub.Events {
			rendered = append(rendered, event.Name, fmt.Sprintf("%+v", event))
		}
	}

	return rendered
}

// requireNoSecretInSpans is the assertion itself.
func requireNoSecretInSpans(t *testing.T, recorder *tracetest.SpanRecorder, material string) {
	t.Helper()

	for _, rendered := range renderedSpans(recorder) {
		require.NotContains(t, rendered, material,
			"secret material reached a span, which is exported to a collector")
	}
}

// spanAttributes flattens one recorded span for assertion.
func spanAttributes(t *testing.T, recorder *tracetest.SpanRecorder, name string) map[string]string {
	t.Helper()

	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != name {
			continue
		}

		attrs := make(map[string]string, len(stub.Attributes))
		for _, attr := range stub.Attributes {
			attrs[string(attr.Key)] = attr.Value.Emit()
		}

		return attrs
	}

	t.Fatalf("no span named %q was recorded; the ones that were: %v", name, spanNames(recorder))

	return nil
}

func spanNames(recorder *tracetest.SpanRecorder) []string {
	var names []string
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}

	return names
}

// registerSecretReadingTask registers a task that resolves a reference and
// reports something about the value without returning it.
//
// The task is what makes this test a test of the real path: the secret is
// resolved inside the activity, through the same [v1.ResolveSecret] the http
// task's `bearer:` uses, while a span covering that activity is open.
func registerSecretReadingTask(t *testing.T, name string, fail bool) {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:            name,
		AuthorityInputs: []string{"credential"},
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			secret, err := v1.ResolveSecret(ctx, inputs["credential"].GetSecretRef())
			if err != nil {
				return nil, err
			}

			if fail {
				// The nastiest realistic case, and the one that motivated
				// keeping the message out of the span status: a task whose
				// error quotes what it was given. An http task naming a URL it
				// built from a secret is the same shape.
				return nil, fmt.Errorf("upstream rejected credential %s", secret.Reveal())
			}

			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"length": v1.NewValue(int64(secret.Len())),
			}}, nil
		},
	}))
}

// secretReadingWorkflow builds the run both tests execute.
func secretReadingWorkflow(taskName string) *v1.RunState {
	workflow := &v1.Workflow{Name: "traced-secret-workflow", Steps: []*v1.Node{{
		Id: "read",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: taskName, Inputs: map[string]*v1.Value{
			"credential": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
				Scheme: "traced-secret", Name: "token",
			}}},
		}}},
	}}}

	return &v1.RunState{Workflow: workflow, Identity: &v1.WorkloadIdentity{
		Subject: "caller", Issuer: "https://issuer.example", Namespace: "acme",
	}}
}

// tracedSecretProvider hands out the material under test.
type tracedSecretProvider struct{}

func (tracedSecretProvider) Scheme() string { return "traced-secret" }

func (tracedSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, theSecret), nil
}

// tracedSecretRuntime assembles a worker that may read the traced scheme.
func tracedSecretRuntime(t *testing.T) engine.TaskRuntimeConfig {
	t.Helper()

	store, err := secrets.NewStore(tracedSecretProvider{})
	require.NoError(t, err)

	policy, err := (auth.SecretAccessPolicy{Allow: []string{`true`}}).Compile()
	require.NoError(t, err)

	runtime, err := engine.NewTaskRuntimeConfig(store, policy, nil)
	require.NoError(t, err)

	return runtime
}

// TestTaskSpanNamesTheSecretReferenceAndNeverTheSecret is the containment test,
// and it asserts both directions on purpose.
//
// The negative alone would pass on a span carrying nothing at all, which is a
// test of an absent feature rather than of a contained one. So it first requires
// that the reference *is* named — scheme and name, which is the whole point of
// tracing a secret read — and then that the value is nowhere.
func TestTaskSpanNamesTheSecretReferenceAndNeverTheSecret(t *testing.T) {
	recorder := recordSpans(t)

	const taskName = "traced-secret-task"
	registerSecretReadingTask(t, taskName, false)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, tracedSecretRuntime(t))
	env.ExecuteWorkflow(engine.Run, secretReadingWorkflow(taskName))
	require.NoError(t, env.GetWorkflowError())

	attrs := spanAttributes(t, recorder, "flowstate.task/"+taskName)
	require.Equal(t, taskName, attrs["flowstate.task.name"])
	require.Equal(t, "read", attrs["flowstate.step.id"],
		"the authorized activity knows which step it is; the span must say so")
	// Emit renders a string slice attribute as a JSON array, which is what a
	// collector receives.
	require.Equal(t, `["traced-secret:token"]`, attrs["flowstate.secret.refs"],
		"the reference the step read must be named, by scheme and name")
	require.Equal(t, "1", attrs["flowstate.secret.ref.count"])
	require.Contains(t, attrs, "flowstate.attempt")

	// And the material itself, nowhere.
	requireNoSecretInSpans(t, recorder, theSecret)
}

// TestFailedTaskSpanCarriesTheClassificationNotTheMessage is the leak this
// design most nearly had.
//
// A span's error status is the obvious place to put err.Error(), and a task's
// error text can quote what the task was handed — so the status carries the
// classification instead, and RecordError is deliberately not called, since it
// writes the message into an exception event. Both are asserted: the status says
// something useful, and it does not say the secret.
func TestFailedTaskSpanCarriesTheClassificationNotTheMessage(t *testing.T) {
	recorder := recordSpans(t)

	const taskName = "traced-secret-failing-task"
	registerSecretReadingTask(t, taskName, true)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, tracedSecretRuntime(t))
	env.ExecuteWorkflow(engine.Run, secretReadingWorkflow(taskName))
	require.Error(t, env.GetWorkflowError(), "the task fails on purpose")

	var described bool
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != "flowstate.task/"+taskName {
			continue
		}

		require.Equal(t, "Error", stub.Status.Code.String(), "a failed task must mark its span")
		require.NotEmpty(t, stub.Status.Description, "a status with no description says nothing")
		require.NotContains(t, strings.ToLower(stub.Status.Description), "rejected credential",
			"the task's own error message reached the span status")
		require.Empty(t, stub.Events, "no exception event, because an exception event carries the message")

		described = true
	}
	require.True(t, described, "no span covered the failing task; recorded: %v", spanNames(recorder))

	requireNoSecretInSpans(t, recorder, theSecret)
}

// TestNoSpansWithoutATracerProvider is invariant 8 at this layer: a worker in a
// process nobody configured for telemetry does the work and records nothing.
//
// Asserted through the recorder being installed only *after* the run, so
// anything the activity minted would have gone to the global no-op provider and
// left no trace — which is what the guard in startTaskSpan makes cheap as well
// as silent.
func TestNoSpansWithoutATracerProvider(t *testing.T) {
	const taskName = "untraced-secret-task"
	registerSecretReadingTask(t, taskName, false)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, tracedSecretRuntime(t))
	env.ExecuteWorkflow(engine.Run, secretReadingWorkflow(taskName))
	require.NoError(t, env.GetWorkflowError())

	recorder := recordSpans(t)
	require.Empty(t, recorder.Ended(),
		"an unconfigured process recorded spans, which it has no provider to record into")
}
