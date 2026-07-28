package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// noop is a task body, for definitions whose name is the thing under test.
func noop(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
	return &v1.Node_Outputs{}, nil
}

// TestRegisterRefusesANameTheGrammarUses covers the one thing standing between a
// plugin and an ambiguous Flowfile.
//
// The plugin protocol validates a task name as `^[a-z][a-z0-9_]*$`, and *every*
// word the step grammar uses matches that pattern — `timeout`, `retry`, `sleep`,
// `parallel`, `if`. A plugin advertising a task called `sleep` was legal, and the
// host registered it verbatim.
//
// That is a live hazard today, because a step key is already read as grammar
// first: a workflow naming such a task could not reach it, and the failure would
// arrive as a confusing diagnostic about a key rather than as anything pointing at
// the plugin. It becomes a correctness hazard the moment a step names its task
// directly, because then both readings of the key are legitimate and no parser can
// choose between them.
//
// So it is refused where the name is chosen. A plugin author learns at startup;
// the alternative is a Flowfile author learning much later that their step did
// something else.
func TestRegisterRefusesANameTheGrammarUses(t *testing.T) {
	t.Parallel()

	for _, name := range v1.ReservedStepKeys() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			registry := v1.NewRegistry()
			err := registry.Register(v1.TaskDef{Name: name, Fn: noop})

			require.Error(t, err, "a task named %q was accepted, and `%s:` on a step is now ambiguous", name, name)
			require.Contains(t, err.Error(), name, "the refusal does not name the task: %v", err)

			_, found := registry.Lookup(name)
			require.False(t, found, "the task was refused and registered anyway")
		})
	}
}

// TestRegisterAcceptsANameTheGrammarDoesNot is the other direction, and the reason
// it is worth writing: a rule that refuses everything satisfies the test above.
func TestRegisterAcceptsANameTheGrammarDoesNot(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "slack_post", Fn: noop}))

	_, found := registry.Lookup("slack_post")
	require.True(t, found)
}

// TestNoBuiltInTaskTakesAReservedName checks the rule against the tasks that ship.
//
// MustRegister panics on a refusal, so a built-in taking a reserved name would
// already fail at package initialization — which is to say this test would not run
// at all rather than fail. Asserting it directly is what makes the reason visible
// when somebody adds a task called `parallel` and wonders why nothing starts.
func TestNoBuiltInTaskTakesAReservedName(t *testing.T) {
	t.Parallel()

	for _, name := range v1.TaskNames() {
		require.False(t, v1.IsReservedStepKey(name),
			"built-in task %q takes a name the step grammar uses", name)
	}
}
