// Package tests contains fixtures shared by the local and durable execution
// driver tests.
package tests

import (
	"context"
	"fmt"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	ContainmentWorkflowName = "trace-containment-workflow"
	ContainmentTaskName     = "trace-containment-failure"
	ContainmentStepID       = "contained-failure"
	ContainmentSecretRef    = "containment-secret:nested-token"

	SensitiveInputMaterial = "sensitive-run-input-Z7pQ4m"
	SecretMaterial         = "resolved-secret-N8vK2x"
	UnrestrictedURL        = "https://unrestricted.invalid/private/P3cW9r"
	EvaluatedCELMaterial   = "evaluated-cel-value-H6tJ5s"
	RawFailureMessage      = "raw-task-failure-B4yD1n"
)

// ProhibitedTraceValues is every value the fixture deliberately puts on the
// execution path but which must not become telemetry.
var ProhibitedTraceValues = []string{
	SensitiveInputMaterial,
	SecretMaterial,
	UnrestrictedURL,
	EvaluatedCELMaterial,
	RawFailureMessage,
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
					"authorization": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "containment-secret", Name: "nested-token"}}},
					"url":           v1.NewLiteral(UnrestrictedURL),
				}),
				"evaluated": v1.NewExpr(`"evaluated-cel-" + "value-H6tJ5s"`),
			}}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
		}},
	}
}

func TraceContainmentInputs() map[string]*v1.Value {
	return map[string]*v1.Value{"private": v1.NewLiteral(SensitiveInputMaterial)}
}

// RegisterTraceContainmentTask installs the failing task used by both drivers.
func RegisterTraceContainmentTask(t testing.TB) {
	t.Helper()
	registry := v1.DefaultRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: ContainmentTaskName,
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			request, ok := v1.StructureMap(inputs["request"])
			if !ok {
				return nil, fmt.Errorf("%s: request was not a map", RawFailureMessage)
			}
			secret, err := v1.ResolveSecret(ctx, request["authorization"].GetSecretRef())
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("%s private=%s secret=%s url=%s evaluated=%s",
				RawFailureMessage, inputs["private"].GetLiteral().GetStringValue(), secret.Reveal(),
				request["url"].GetLiteral().GetStringValue(), inputs["evaluated"].GetLiteral().GetStringValue())
		},
	}))
	t.Cleanup(func() { registry.Unregister(ContainmentTaskName) })
}

type containmentSecretProvider struct{}

func (containmentSecretProvider) Scheme() string { return "containment-secret" }
func (containmentSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, SecretMaterial), nil
}

func TraceContainmentAuthority(t testing.TB) (*secrets.Store, *auth.SecretPolicy) {
	t.Helper()
	store, err := secrets.NewStore(containmentSecretProvider{})
	require.NoError(t, err)
	policy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	return store, policy
}

// AssertTraceContainment renders the complete exported span representation in
// each fmt shape used by the original containment test.
func AssertTraceContainment(t testing.TB, recorder *tracetest.SpanRecorder, expectedNames ...string) {
	t.Helper()
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	require.NotEmpty(t, stubs, "the containment run emitted no completed spans")

	rendered := []string{fmt.Sprintf("%v", stubs), fmt.Sprintf("%+v", stubs), fmt.Sprintf("%#v", stubs)}
	for _, stub := range stubs {
		complete := struct {
			Name       string
			Attributes any
			Events     any
			Links      any
			Status     string
		}{stub.Name, stub.Attributes, stub.Events, stub.Links, stub.Status.Description}
		rendered = append(rendered, fmt.Sprintf("%v", complete), fmt.Sprintf("%+v", complete), fmt.Sprintf("%#v", complete))
	}
	all := fmt.Sprint(rendered)
	for _, prohibited := range ProhibitedTraceValues {
		require.NotContains(t, all, prohibited, "prohibited execution material reached exported span data")
	}
	for _, safe := range append(expectedNames, ContainmentTaskName, ContainmentStepID, ContainmentSecretRef) {
		require.Contains(t, all, safe, "stable operation name or safe identifier is absent from the trace")
	}
}
