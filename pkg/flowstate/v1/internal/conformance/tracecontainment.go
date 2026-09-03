package conformance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// The end-to-end containment case: one run, carrying every kind of material a
// span must refuse, executed by both drivers.
//
// # Why a run rather than a hand-built span
//
// The containment claims each driver made before this were synthetic. The local
// driver registered a task that failed quoting a constant, and asserted the
// constant was absent; the durable driver did the same with a resolved secret.
// Both prove that the one value the test put on the one path the test knew
// about did not survive — which is the "test that A can reach A" shape, applied
// to telemetry. Neither exercised a *declared* sensitive input, an expression's
// result, or a reference nested inside a structure, and each proved its half in
// a package the other one cannot see.
//
// So the fixture below puts all five kinds of material on one execution path and
// both drivers run it: a sensitive declared input, a secret resolved through the
// real [v1.ResolveSecret] path from a reference nested inside a structure, a URL
// nobody's egress policy admits, a value that exists only because CEL computed
// it, and a task error quoting all four. What is asserted is that none of them
// appears in any rendering of any exported span, and that the identifiers a span
// *is* for — the task's name, the step's id, the secret's reference — do.
//
// # The positive half is not decoration
//
// A negative-only assertion passes on a run that recorded nothing at all, which
// is a test of an absent feature rather than a contained one. The expected-name
// arguments are what keeps it honest, and they differ per driver on purpose: the
// local driver opens a `flowstate.run/<workflow>` span covering the whole run and
// the durable driver does not, because Temporal's interceptor already opens one
// at that seam (see [v1.StartRunSpan] and taskSpanPrefix's comment above). Each
// caller names the roots its own driver is obliged to produce.

// The identifiers a span is allowed to carry, and which both drivers must
// produce the same way.
const (
	// ContainmentWorkflowName names the run, and is what the local driver's run
	// span is called.
	ContainmentWorkflowName = "trace-containment-workflow"

	// ContainmentTaskName is the registered task, which is what the task span is
	// called under either driver.
	ContainmentTaskName = "trace_containment_failure"

	// ContainmentStepID is the step the task runs for.
	ContainmentStepID = "contained-failure"

	// ContainmentSecretScheme and ContainmentSecretName address the reference the
	// step reads, from inside a structure rather than as a whole input — the
	// position [v1.SecretRefsIn] has to walk to find.
	ContainmentSecretScheme = "containment-secret"
	ContainmentSecretName   = "nested-token"
)

// ContainmentSecretRef is the reference as a span names it.
//
// Composed from the two constants above rather than derived through
// [v1.SecretRefsIn], which is the function the span attribute is built from: a
// derivation through the code under test would agree with a driver that walked
// the task wrongly, and what this pins is that the reference reaches a collector
// spelled `scheme:name` and carrying no material.
const ContainmentSecretRef = ContainmentSecretScheme + ":" + ContainmentSecretName

// The material the fixture puts on the execution path and no span may carry.
// Each is distinctive enough that a substring search cannot match it by
// accident, and each arrives by a different route.
const (
	// ContainmentInputMaterial is submitted as the run's one declared input,
	// marked sensitive.
	ContainmentInputMaterial = "sensitive-run-input-Z7pQ4m"

	// ContainmentSecretMaterial is what the fixture provider resolves the nested
	// reference to, revealed inside the task the way a real one is.
	ContainmentSecretMaterial = "resolved-secret-N8vK2x"

	// ContainmentURLMaterial is an address a step names as a literal. A URL is
	// material in its own right — it carries a path an operator's collector has
	// no business indexing — and it is the value an http task's error most often
	// quotes.
	ContainmentURLMaterial = "https://unrestricted.invalid/private/P3cW9r"

	// ContainmentExprMaterial exists only because CEL computed it: see
	// [containmentExpr], which builds the expression from halves so that this
	// string appears nowhere in the workflow document. A rendering containing it
	// can therefore only have got it from an evaluated value, which is what the
	// arm is for.
	ContainmentExprMaterial = "evaluated-cel-value-H6tJ5s"

	// ContainmentFailureMessage is what the task's own error leads with. A task's
	// error can quote whatever it was handed — this one quotes all four values
	// above — which is why a failed span records the classification instead.
	ContainmentFailureMessage = "raw-task-failure-B4yD1n"
)

// ContainmentProhibitedValues is every value the fixture deliberately puts on
// the execution path and which must not become telemetry.
func ContainmentProhibitedValues() []string {
	return []string{
		ContainmentInputMaterial,
		ContainmentSecretMaterial,
		ContainmentURLMaterial,
		ContainmentExprMaterial,
		ContainmentFailureMessage,
	}
}

// containmentExpr is the step's CEL input, spelled so that
// [ContainmentExprMaterial] does not appear in the workflow it is part of.
//
// Split in the middle and concatenated by the evaluator, derived from the
// constant rather than written beside it, so the two halves cannot drift from
// the value the assertion looks for.
func containmentExpr() *v1.Value {
	half := len(ContainmentExprMaterial) / 2

	return v1.NewExpr(strconv.Quote(ContainmentExprMaterial[:half]) +
		" + " + strconv.Quote(ContainmentExprMaterial[half:]))
}

// TraceContainmentWorkflow returns the one fixture both execution drivers run.
func TraceContainmentWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    ContainmentWorkflowName,
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "private", Type: v1.InputDeclaration_TYPE_STRING, Required: true, Sensitive: true,
		}},
		Steps: []*v1.Node{{
			Id: ContainmentStepID,
			Kind: &v1.Node_Task{Task: &v1.Task{Name: ContainmentTaskName, Inputs: map[string]*v1.Value{
				"private": v1.NewExpr("inputs.private"),
				"request": v1.NewStructureMap(map[string]*v1.Value{
					"authorization": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
						Scheme: ContainmentSecretScheme, Name: ContainmentSecretName,
					}}},
					"url": v1.NewLiteral(ContainmentURLMaterial),
				}),
				"evaluated": containmentExpr(),
			}}},
			// One attempt, declared: the task fails on purpose, and five of them
			// would spend fifteen seconds of backoff proving what the retry cases
			// already own.
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
		}},
	}
}

// TraceContainmentInputs is what the run is submitted with.
func TraceContainmentInputs() map[string]*v1.Value {
	return map[string]*v1.Value{"private": v1.NewLiteral(ContainmentInputMaterial)}
}

// TraceContainmentAuthority is the fixture worker capability the run needs,
// built from the same [Authority] every other shared case installs — so the two
// drivers get one secret provider and one compiled policy rather than a copy
// each, and the identity a denial would name cannot differ between them.
func TraceContainmentAuthority() Authority {
	return Authority{
		Scheme:       ContainmentSecretScheme,
		FixtureValue: ContainmentSecretMaterial,
		Allow:        []string{"true"},
		Identity: auth.WorkloadIdentity{
			Subject: "trace-caller", Issuer: "https://issuer.example", Namespace: "trace-tenant",
		},
	}
}

// RegisterTraceContainmentTask installs the failing task both drivers run, for
// the duration of tb.
//
// The task is what makes this end to end rather than synthetic: it reaches the
// nested reference through [v1.StructureMap] and resolves it through the same
// [v1.ResolveSecret] the http task's `bearer:` uses, with the task span open,
// and then fails quoting everything it was handed — which is the nastiest
// realistic shape, and the one that motivated recording a classification
// instead of a message.
//
// Removed rather than restored on cleanup: this name is not in the registry
// otherwise, and a definition left behind is one `TestEveryTaskDescribesItself`
// walks and rightly refuses, since a task with no schema is not something the
// build ships.
func RegisterTraceContainmentTask(tb testing.TB) {
	tb.Helper()

	registry := v1.DefaultRegistry()
	if err := registry.Register(v1.TaskDef{
		Name: ContainmentTaskName,
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			request, ok := v1.StructureMap(inputs["request"])
			if !ok {
				return nil, fmt.Errorf("%s: request was not a map", ContainmentFailureMessage)
			}

			secret, err := v1.ResolveSecret(ctx, request["authorization"].GetSecretRef())
			if err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("%s private=%s secret=%s url=%s evaluated=%s",
				ContainmentFailureMessage,
				inputs["private"].GetLiteral().GetStringValue(),
				secret.Reveal(),
				request["url"].GetLiteral().GetStringValue(),
				inputs["evaluated"].GetLiteral().GetStringValue())
		},
	}); err != nil {
		tb.Fatalf("registering the containment fixture task: %v", err)
	}

	tb.Cleanup(func() { registry.Unregister(ContainmentTaskName) })
}

// AssertTraceContainment is the shared assertion both drivers make, over every
// span the run exported.
//
// Both directions, for the reason the positive half is named above: the material
// is nowhere, and the identifiers a span exists to carry are somewhere.
// expectedNames are the span names this driver is obliged to have opened, which
// is the one thing the two drivers legitimately differ on.
//
// The rendering is [RenderedSpans], the same shape [AssertTaskSpans] and
// [AssertHTTPSpan] check: every verb over the batch, over each span, and over a
// struct reaching them through an *unexported* field, which is the arrangement
// `fmt` cannot call a method on and reflects into instead.
func AssertTraceContainment(tb testing.TB, recorder *tracetest.SpanRecorder, expectedNames ...string) {
	tb.Helper()

	if len(recorder.Ended()) == 0 {
		tb.Fatal("the containment run exported no completed spans, so this would pass by having nothing to check")
	}

	rendered := RenderedSpans(recorder)
	all := strings.Join(rendered, "\n")

	for _, prohibited := range ContainmentProhibitedValues() {
		if strings.Contains(all, prohibited) {
			tb.Errorf("%q reached an exported span, which a collector indexes and anyone may read", prohibited)
		}
	}

	for _, safe := range append(expectedNames, ContainmentTaskName, ContainmentStepID, ContainmentSecretRef) {
		if !strings.Contains(all, safe) {
			tb.Errorf("%q is absent from every exported span, so the trace does not say what ran; recorded: %v",
				safe, spanNames(recorder))
		}
	}
}
