package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// A secret reference nested in a task input — `headers: {Authorization:
// ${secret('env:TOKEN')}}` — is carried, never evaluated. The corpus cases in
// `tests` prove it on both drivers from the outside, by running the workflow and
// checking what the run recorded.
//
// What is checked here is the inside: the thing that *travels*. The durable driver
// resolves a task's inputs in workflow code and then hands the resolved task to an
// activity, which is a Temporal payload written to history. So the question with
// teeth is not whether the run's outputs are clean — it is whether the value could
// have been in the payload at all, and the answer has to be that no code between
// the specification and the activity is even able to produce it.

// secretHeaderTask is the step both tests below use: one header written by hand,
// one carrying a reference.
func secretHeaderTask(url string) *v1.Task {
	return &v1.Task{
		Name: "http",
		Inputs: map[string]*v1.Value{
			"url": v1.NewLiteral(url),
			"headers": v1.NewStructureMap(map[string]*v1.Value{
				"Accept":        v1.NewLiteral("application/json"),
				"Authorization": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
			}),
			"outputs": v1.NewExpr(`{"reflected": response.headers["X-Reflected"][0]}`),
		},
	}
}

// TestNestedSecretStaysAReferenceInTheActivityPayload pins what the durable
// driver puts on the wire.
//
// [v1.ResolveTaskInputs] is the last thing workflow code does to a task before the
// activity is scheduled, so its result *is* the payload. A structure has to come
// out of it unchanged: an expression is evaluated there, and a reference evaluated
// anywhere in workflow code is a secret in history.
func TestNestedSecretStaysAReferenceInTheActivityPayload(t *testing.T) {
	t.Parallel()

	task := secretHeaderTask("https://api.example.com/events")

	resolved, err := v1.ResolveTaskInputs(t.Context(), task, v1.NewScope(v1.CurrentProfile, nil))
	require.NoError(t, err)

	entries, isMap := v1.StructureMap(resolved.GetInputs()["headers"])
	require.True(t, isMap, "the headers input stopped being a structure on the way to the activity")

	reference := entries["Authorization"].GetSecretRef()
	require.NotNil(t, reference, "the nested entry is no longer a reference: %v", entries["Authorization"])
	require.Equal(t, "fixture-secret", reference.GetScheme())
	require.Equal(t, "API_TOKEN", reference.GetName())

	// The bytes themselves, which is what a payload is. A reference is a scheme
	// and a name and holds no material by construction, so what this shows is that
	// nothing *else* was added: the payload names the secret and carries no value
	// for it, on a path where a resolved header would have been one string away.
	payload, err := proto.Marshal(resolved)
	require.NoError(t, err)
	require.Contains(t, string(payload), "fixture-secret",
		"the payload no longer names the reference, so the worker cannot resolve it")

	// And the activity it would be scheduled on is the one that carries the
	// identity to resolve with. A nested reference in an input that is not named
	// in AuthorityInputs used to answer no here, which would have sent this step
	// to an activity with no authority — where it fails closed, correctly, and
	// for a reason no author could act on.
	require.True(t, v1.TaskNeedsAuthority(resolved),
		"a task with a nested reference does not ask for the identity-aware activity")
	require.Equal(t, []string{"fixture-secret:API_TOKEN"}, v1.SecretRefsIn(resolved),
		"the references a step will read are not reported, so a trace cannot say which secret it used")
}

// TestNestedSecretIsNotInAnyRenderingOfTheInputs is the containment matrix
// CLAUDE.md asks for, aimed at the values that hold the reference rather than at
// the run's outputs: the inputs map, the task, and a slice of tasks, each rendered
// the four ways `fmt` renders anything.
//
// It runs a real request first, so the assertion is made *after* the material has
// actually been resolved once. Asserting on a specification nothing ever resolved
// would prove only that the test did not write the value into it.
func TestNestedSecretIsNotInAnyRenderingOfTheInputs(t *testing.T) {
	const material = "material-that-must-not-appear-in-any-rendering-inputs"

	baseURL := tests.NewHTTPServer(t)
	authority := tests.Authority{
		Scheme: "fixture-secret", FixtureValue: material,
		Allow: []string{"true"},
		Identity: auth.WorkloadIdentity{
			Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
		},
	}

	task := secretHeaderTask(baseURL + "/reflect-authorization")
	workflow := &v1.Workflow{
		Name:  "nested-secret-containment",
		Steps: []*v1.Node{{Id: "call", Kind: &v1.Node_Task{Task: task}}},
	}

	ctx := v1.ContextWithTaskRuntime(t.Context(), v1.TaskRuntime{
		Store: authority.Store(t), Policy: authority.Policy(t), Identity: authority.Identity,
	})

	out, err := v1.Run(ctx, workflow)
	require.NoError(t, err)

	// The peer reflected the header, so the value did reach the request — without
	// which the containment below would be asserting nothing.
	require.Equal(t, "[REDACTED]",
		out.GetStepValues()["call"].GetNamedValues()["reflected"].GetLiteral().GetStringValue())
	tests.AssertNoLeak(t, out, material)

	// A struct holding the inputs through an unexported field: the arrangement
	// `fmt` cannot call a method on and therefore reflects into, which is how a
	// redacting String() comes to protect nothing.
	type holder struct{ task *v1.Task }

	inputs := task.GetInputs()
	renderings := map[string]string{
		"%v on the inputs map":  fmt.Sprintf("%v", inputs),
		"%+v on the inputs map": fmt.Sprintf("%+v", inputs),
		"%#v on the inputs map": fmt.Sprintf("%#v", inputs),
		"%v on the task":        fmt.Sprintf("%v", task),
		"%+v on the task":       fmt.Sprintf("%+v", task),
		"%#v on the task":       fmt.Sprintf("%#v", task),
		"%s on the task":        fmt.Sprintf("%s", task),
		"%v on a struct":        fmt.Sprintf("%v", holder{task: task}),
		"%+v on a struct":       fmt.Sprintf("%+v", holder{task: task}),
		"%#v on a struct":       fmt.Sprintf("%#v", holder{task: task}),
		"%v on a slice":         fmt.Sprintf("%v", []holder{{task: task}}),
		"%+v on a slice":        fmt.Sprintf("%+v", []holder{{task: task}}),
		"%#v on a slice":        fmt.Sprintf("%#v", []holder{{task: task}}),
	}
	for name, rendered := range renderings {
		if strings.Contains(rendered, material) {
			t.Errorf("the revealed value appears under %s, so the specification kept what only the "+
				"request was supposed to see", name)
		}
	}

	// The reference is still there, in the same renderings — the positive half,
	// which is what stops the assertion above from passing on an empty structure.
	require.Contains(t, fmt.Sprintf("%v", inputs), "API_TOKEN")
}
