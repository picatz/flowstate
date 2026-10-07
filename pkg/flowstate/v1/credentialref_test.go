package flowstatev1_test

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func credentialTask(stepID, input, target string) *v1.Node {
	return &v1.Node{Id: stepID, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "unregistered.plugin",
		Inputs: map[string]*v1.Value{input: v1.NewCredentialRef(target)},
	}}}
}

// TestValueHoldsCredentialRef pins the walk that decides whether executing a
// value needs the authority to mint a credential: the whole value, nested at any
// depth, and conservatively past the depth bound, with the secret walk left
// answering only its own question.
func TestValueHoldsCredentialRef(t *testing.T) {
	ref := v1.NewCredentialRef("anthropic")
	secret := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}}

	assert.True(t, v1.ValueHoldsCredentialRef(ref))
	assert.True(t, v1.ValueHoldsCredentialRef(v1.NewStructureMap(map[string]*v1.Value{"a": ref})))
	assert.True(t, v1.ValueHoldsCredentialRef(v1.NewStructureList(v1.NewLiteral("x"), v1.NewStructureList(ref))))

	assert.False(t, v1.ValueHoldsCredentialRef(nil))
	assert.False(t, v1.ValueHoldsCredentialRef(v1.NewLiteral("anthropic")))
	assert.False(t, v1.ValueHoldsCredentialRef(v1.NewExpr("credential_name")))
	assert.False(t, v1.ValueHoldsCredentialRef(secret), "a secret reference is the other walk's question")
	assert.False(t, v1.ValueHoldsSecretRef(ref), "a credential reference is the other walk's question")

	// Past the walk's bound the answer is yes: too deep to inspect may hold one,
	// and every consumer fails closed rather than open at depth.
	deep := ref
	for range v1.MaxStructureDepth + 2 {
		deep = v1.NewStructureList(deep)
	}
	assert.True(t, v1.ValueHoldsCredentialRef(deep))
	tooDeepForNothing := v1.NewLiteral("x")
	for range v1.MaxStructureDepth + 2 {
		tooDeepForNothing = v1.NewStructureList(tooDeepForNothing)
	}
	assert.True(t, v1.ValueHoldsCredentialRef(tooDeepForNothing),
		"a structure too deep to read must answer as though it holds one")
}

func TestCredentialRefsIn(t *testing.T) {
	task := &v1.Task{Name: "t", Inputs: map[string]*v1.Value{
		"b":     v1.NewCredentialRef("zeta"),
		"a":     v1.NewCredentialRef("alpha"),
		"again": v1.NewCredentialRef("alpha"),
		"text":  v1.NewLiteral("alpha"),
	}}
	assert.Equal(t, []string{"alpha", "zeta"}, v1.CredentialRefsIn(task))
	assert.Empty(t, v1.CredentialRefsIn(&v1.Task{Name: "t"}))
}

func TestValidateCredentialTarget(t *testing.T) {
	require.NoError(t, v1.ValidateCredentialTarget("anthropic"))
	require.NoError(t, v1.ValidateCredentialTarget("aws-prod.eu_1"))
	require.NoError(t, v1.ValidateCredentialTarget(strings.Repeat("a", v1.MaxCredentialTargetLen)))

	require.ErrorContains(t, v1.ValidateCredentialTarget(""), "must not be empty")
	require.ErrorContains(t, v1.ValidateCredentialTarget(strings.Repeat("a", v1.MaxCredentialTargetLen+1)), "longer than")
	require.ErrorContains(t, v1.ValidateCredentialTarget("any\nthing"), "control character")
	require.ErrorContains(t, v1.ValidateCredentialTarget("any\x00thing"), "control character")
}

// TestCredentialReferenceTargetsArePreflighted pins the half of the feature that
// makes an unknown target a diagnostic before the run starts: every position a
// task can sit in is walked, the refusal names the step, the input and the
// target, and a configured target passes.
func TestCredentialReferenceTargetsArePreflighted(t *testing.T) {
	configured := []string{"anthropic", "partner"}

	t.Run("a configured target passes", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{credentialTask("call", "api_key", "anthropic")}}
		require.NoError(t, v1.ValidateCredentialTargets(wf, configured))
	})

	t.Run("an unknown target is refused naming the step, input and target", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{credentialTask("call", "api_key", "openai")}}
		err := v1.ValidateCredentialTargets(wf, configured)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `step "call"`)
		assert.Contains(t, err.Error(), `input "api_key"`)
		assert.Contains(t, err.Error(), `credential target "openai" is not configured`)
		assert.Contains(t, err.Error(), "configured: [anthropic partner]", "the refusal lists what is configured")
	})

	t.Run("a deployment that federates nothing refuses every target", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{credentialTask("call", "api_key", "anthropic")}}
		require.ErrorContains(t, v1.ValidateCredentialTargets(wf, nil), `credential target "anthropic" is not configured`)
	})

	t.Run("a malformed target is refused whatever is configured", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{credentialTask("call", "api_key", "")}}
		require.ErrorContains(t, v1.ValidateCredentialTargets(wf, []string{""}), "must not be empty")
	})

	t.Run("a reference nested in a structure is still found", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{{Id: "call", Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "unregistered.plugin",
			Inputs: map[string]*v1.Value{"headers": v1.NewStructureMap(map[string]*v1.Value{
				"Authorization": v1.NewCredentialRef("openai"),
			})},
		}}}}}
		require.ErrorContains(t, v1.ValidateCredentialTargets(wf, configured), `"openai"`)
	})

	t.Run("a value too deep to read is refused rather than passed", func(t *testing.T) {
		deep := v1.NewCredentialRef("anthropic")
		for range v1.MaxStructureDepth + 2 {
			deep = v1.NewStructureList(deep)
		}
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{{Id: "call", Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "unregistered.plugin", Inputs: map[string]*v1.Value{"api_key": deep},
		}}}}}
		require.ErrorContains(t, v1.ValidateCredentialTargets(wf, configured), "too deep")
	})

	// Every position a step can sit in: a refusal that only looked at the top
	// level would pass a target nobody configured as long as it was written
	// inside a body.
	positions := map[string]func(*v1.Node) *v1.Node{
		"for_each body": func(inner *v1.Node) *v1.Node {
			return &v1.Node{Id: "outer", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList("a"), Iterator: "x", Body: []*v1.Node{inner},
			}}}
		},
		"loop body": func(inner *v1.Node) *v1.Node {
			return &v1.Node{Id: "outer", Kind: &v1.Node_Loop{Loop: &v1.Loop{Body: []*v1.Node{inner}}}}
		},
		"parallel branch": func(inner *v1.Node) *v1.Node {
			return &v1.Node{Id: "outer", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{{Steps: []*v1.Node{inner}}},
			}}}
		},
		"inlined callee": func(inner *v1.Node) *v1.Node {
			return &v1.Node{Id: "outer", Kind: &v1.Node_Call{Call: &v1.Call{
				Workflow: &v1.Workflow{Name: "callee", Steps: []*v1.Node{inner}},
			}}}
		},
		"compensation": func(inner *v1.Node) *v1.Node {
			return &v1.Node{
				Id:   "outer",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
				Undo: &v1.Compensation{Task: inner.GetTask()},
			}
		},
	}
	for name, wrap := range positions {
		t.Run("inside a "+name, func(t *testing.T) {
			bad := &v1.Workflow{Name: "w", Steps: []*v1.Node{wrap(credentialTask("inner", "api_key", "openai"))}}
			require.ErrorContains(t, v1.ValidateCredentialTargets(bad, configured), `credential target "openai"`)

			good := &v1.Workflow{Name: "w", Steps: []*v1.Node{wrap(credentialTask("inner", "api_key", "anthropic"))}}
			require.NoError(t, v1.ValidateCredentialTargets(good, configured))
		})
	}
}

// TestCredentialReferenceIsRefusedWorkflowSide pins invariant 7 for the new kind:
// every way workflow code could come to hold a credential reference's value is
// refused, and the refusal names the target and never anything resembling a
// credential.
func TestCredentialReferenceIsRefusedWorkflowSide(t *testing.T) {
	t.Run("an expression reading a step output that holds one", func(t *testing.T) {
		prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"s0": {NamedValues: map[string]*v1.Value{v1.ValueOutput: v1.NewLiteral(0)}},
			"b":  {NamedValues: map[string]*v1.Value{"token": v1.NewCredentialRef("anthropic")}},
		}}
		activation := v1.Activation(t.Context(), v1.CurrentProfile, prev, nil, nil, nil, nil, true, nil, nil)

		_, err := v1.DefaultEvaluator().EvalParsedBase(
			t.Context(), v1.CurrentProfile, v1.NewExpr("steps.b.token").GetExpr(), activation)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "a credential reference cannot be read in an expression")
		assert.Contains(t, err.Error(), "anthropic")

		// The neighbouring read still works: the refusal is the read that reaches
		// the reference, not the run.
		out, err := v1.DefaultEvaluator().EvalParsedBase(
			t.Context(), v1.CurrentProfile, v1.NewExpr("steps.s0.value").GetExpr(), activation)
		require.NoError(t, err)
		assert.Equal(t, int64(0), out.Value())
	})

	t.Run("a caller submitting one as a run input", func(t *testing.T) {
		declaration := &v1.InputDeclaration{Type: v1.InputDeclaration_TYPE_STRING}
		err := v1.CheckInputValue("key", declaration, v1.NewCredentialRef("anthropic"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), `input "key" is a credential reference, which a caller may not choose`)
	})

	t.Run("a var holding one, at either level", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Vars: map[string]*v1.Value{"key": v1.NewCredentialRef("anthropic")}}
		require.ErrorContains(t, v1.CheckVarsHoldNoSecretRef(wf), `workflow var "key" is a credential reference`)

		wf = &v1.Workflow{Name: "w", Steps: []*v1.Node{{
			Id: "a", Vars: map[string]*v1.Value{"key": v1.NewCredentialRef("anthropic")},
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
		}}}
		require.ErrorContains(t, v1.CheckVarsHoldNoSecretRef(wf), `step "a" var "key" is a credential reference`)
	})
}

// TestTaskNeedsAuthorityForAHeldCredentialReference pins that a task carrying a
// credential reference is routed to the activity that has the worker's runtime,
// whatever any registry knows about the task.
func TestTaskNeedsAuthorityForAHeldCredentialReference(t *testing.T) {
	assert.True(t, v1.TaskNeedsAuthority(&v1.Task{
		Name: "no-registry-has-heard-of-this", Inputs: map[string]*v1.Value{"api_key": v1.NewCredentialRef("anthropic")},
	}))
	assert.True(t, v1.TaskNeedsAuthority(&v1.Task{
		Name: "no-registry-has-heard-of-this", Inputs: map[string]*v1.Value{
			"headers": v1.NewStructureMap(map[string]*v1.Value{"a": v1.NewCredentialRef("anthropic")}),
		},
	}))
	assert.False(t, v1.TaskNeedsAuthority(&v1.Task{
		Name: "no-registry-has-heard-of-this", Inputs: map[string]*v1.Value{"api_key": v1.NewLiteral("anthropic")},
	}))
}

// TestBuiltinTasksRefuseACredentialReference pins the fail-closed default for
// every task that does not accept one: the http task takes its `bearer:` as a
// secret reference only, and a typed scalar field names the reference rather than
// reporting a Go type.
func TestBuiltinTasksRefuseACredentialReference(t *testing.T) {
	t.Run("a scalar field", func(t *testing.T) {
		_, err := v1.Run(t.Context(), &v1.Workflow{Name: "w", Profile: v1.CurrentProfile, Steps: []*v1.Node{{
			Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewCredentialRef("anthropic"),
			}}},
		}}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "credential reference")
		assert.Contains(t, err.Error(), "anthropic")
	})

	t.Run("the http bearer", func(t *testing.T) {
		_, err := v1.Run(t.Context(), &v1.Workflow{Name: "w", Profile: v1.CurrentProfile, Steps: []*v1.Node{{
			Id: "a", Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url":    v1.NewLiteral("https://api.example.com/events"),
				"bearer": v1.NewCredentialRef("anthropic"),
			}}},
		}}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bearer must be a secret reference")
	})
}
