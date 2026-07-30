package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The catalog is what something other than a person reads to find out what this build
// can do — an agent driving the CLI, a generator, an editor that is not this project's
// language server. Until the retirement it could describe the whole language by listing
// tasks, because one of the tasks *was* how you computed things.
//
// It cannot now. Two tasks remain and one of them produces nothing, so a consumer
// reading only the task list would reasonably conclude the language has almost no way
// to compute anything at all. The roots are how it finds out otherwise.

// TestTheCatalogSaysHowAValueIsReached is the claim, stated as what a consumer needs.
//
// Written against the evaluator's own constants rather than against the strings, so
// renaming a root in one place cannot leave the catalog handing out the old spelling —
// which would be worse than saying nothing, since a reference built from it would not
// resolve.
func TestTheCatalogSaysHowAValueIsReached(t *testing.T) {
	t.Parallel()

	catalog := v1.Catalog()

	require.NotEmpty(t, catalog.GetValueRoots(),
		"the catalog lists tasks and never says where a value comes from, so a consumer "+
			"reading it cannot write an expression that resolves")

	assert.Contains(t, catalog.GetValueRoots(), v1.VarsRoot,
		"the catalog does not name the root a file's own values are read through")
	assert.Contains(t, catalog.GetValueRoots(), v1.StepsRoot,
		"the catalog does not name the root a step's outputs are read through")
}

// TestEveryValueRootIsOneTheEvaluatorBinds keeps the list honest in the direction that
// costs a consumer real work.
//
// A root the catalog names and the evaluator does not bind is an expression somebody
// builds from this answer and then cannot run. Checked by asking the evaluator, so a
// root added here without being bound fails rather than being believed.
func TestEveryValueRootIsOneTheEvaluatorBinds(t *testing.T) {
	t.Parallel()

	for _, root := range v1.Catalog().GetValueRoots() {
		// An empty scope still binds the roots themselves — that is what makes them
		// roots rather than names a particular run happens to have. `size(vars)` on a
		// run with no vars is zero, not an unbound name.
		evaluator := v1.DefaultEvaluator()

		env, err := evaluator.ProfileEnv(v1.CurrentProfile)
		require.NoError(t, err)

		ast, issues := env.Parse("size(" + root + ")")
		require.NoError(t, issues.Err())

		scope := v1.NewScope(v1.CurrentProfile, nil)
		_, err = evaluator.Eval(t.Context(), env, ast, scope.Activation(t.Context()))

		assert.NoError(t, err,
			"the catalog names %q as a root and an expression cannot reach it", root)
	}
}

// TestTheStepsRootIsBoundBeforeAnythingHasRun is the asymmetry the catalog check found.
//
// `vars` resolved to an empty map on a scope carrying no outputs, and `steps` resolved
// to *nothing at all* — one root that is always there and one that appears once a step
// has finished. A root that is only sometimes a root is not a root, and the difference
// showed up as an unbound-name error rather than as the zero it should be.
//
// `size(steps)` before the first step has run is zero: nothing has run. It is the same
// answer the second step gets, one larger.
func TestTheStepsRootIsBoundBeforeAnythingHasRun(t *testing.T) {
	t.Parallel()

	evaluator := v1.DefaultEvaluator()

	env, err := evaluator.ProfileEnv(v1.CurrentProfile)
	require.NoError(t, err)

	ast, issues := env.Parse("size(" + v1.StepsRoot + ")")
	require.NoError(t, issues.Err())

	// A scope with no outputs at all, which is what a workflow's own `vars:` block is
	// evaluated against — before the first step, by construction.
	scope := v1.NewScope(v1.CurrentProfile, nil)

	got, err := evaluator.Eval(t.Context(), env, ast, scope.Activation(t.Context()))
	require.NoError(t, err,
		"the steps root is unbound until something has run, so an expression asking how "+
			"much has happened fails instead of answering none")

	assert.Equal(t, int64(0), got.Value(),
		"nothing has run and the steps root does not say so")
}

// TestAStepNamedStepsStillWinsOverTheRoot is the direction the fix could have broken.
//
// A spec compiled before this root existed may contain a step literally called `steps`,
// and its outputs have to keep resolving — a worker evaluates the stored AST out of
// RunState rather than re-parsing, so a run started on an older build must keep meaning
// what it meant. That is invariant 10, and it is why the root is answered *last* where
// there are outputs to check first.
func TestAStepNamedStepsStillWinsOverTheRoot(t *testing.T) {
	t.Parallel()

	evaluator := v1.DefaultEvaluator()

	env, err := evaluator.ProfileEnv(v1.CurrentProfile)
	require.NoError(t, err)

	ast, issues := env.Parse(v1.StepsRoot + ".result")
	require.NoError(t, issues.Err())

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			v1.StepsRoot: {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("mine")}},
		},
	})

	got, err := evaluator.Eval(t.Context(), env, ast, scope.Activation(t.Context()))
	require.NoError(t, err)
	assert.Equal(t, "mine", got.Value(),
		"a step named `steps` lost to the root, so a run compiled before the root existed "+
			"changed meaning")
}
