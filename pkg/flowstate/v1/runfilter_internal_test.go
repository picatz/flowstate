package flowstatev1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTheFilterSpeaksTheProfilesLanguage pins that the run filter's environment
// and the workflow expression environment come from one profile table (#1689):
// every example the catalog carries for the optional library — the section of
// docs/reference/cel.md that teaches `.?` — compiles under the filter's
// environment as it does under the profile's. Derived from
// [ProfileFunctions] rather than listed, so a library added to the profile is
// checked here without anybody remembering to come back.
func TestTheFilterSpeaksTheProfilesLanguage(t *testing.T) {
	t.Parallel()

	filterEnv, err := runFilterEnv()
	require.NoError(t, err)

	libs, err := ProfileLibraries(CurrentProfile)
	require.NoError(t, err)
	profileEnv, err := DefaultEvaluator().Env(libs...)
	require.NoError(t, err)

	examples := 0
	for _, fn := range ProfileFunctions(CurrentProfile) {
		if fn.Library != "optional" || fn.Example == "" {
			continue
		}
		examples++

		_, issues := profileEnv.Compile(fn.Example)
		require.NoError(t, issues.Err(), "%s does not compile under the profile it is listed for", fn.Example)

		_, issues = filterEnv.Compile(fn.Example)
		require.NoError(t, issues.Err(), "%s compiles for a workflow and not for a filter", fn.Example)
	}
	require.NotZero(t, examples, "the optional library carries no examples, so this proves nothing")

	// And the traversal itself, which is syntax rather than a function and so
	// has no catalog row: the spelling the CLI's help teaches.
	_, issues := filterEnv.Compile(`labels.?team.orValue("") == "payments"`)
	require.NoError(t, issues.Err())
}

// TestAFilterSpendsUnderTheSameControlsTheDriversApply is the other half of
// [TestTheFilterSpeaksTheProfilesLanguage]: a filter compiles the workflow
// profile's whole vocabulary, so it has to be bounded the way that vocabulary
// is bounded everywhere else.
//
// The filter's program was built from [cel.CostLimit] alone, which leaves out
// the two controls [Limits.programOptions] exists to install — the byte-aware
// estimator, which is what decides a unit of budget buys a bounded number of
// bytes rather than one call of any size, and the element bound, which refuses
// a list an expression manufactured past it. A filter could therefore spend the
// profile's own amplifying calls under weaker enforcement than either driver
// applies to them, once per run scanned (#1119).
//
// Refusal is the assertion rather than a measurement: both of these are the
// shapes the missing controls exist to stop.
func TestAFilterSpendsUnderTheSameControlsTheDriversApply(t *testing.T) {
	t.Parallel()

	for name, filter := range map[string]string{
		// Manufactured elements, which the element bound answers.
		"a list built past the element bound": `size(lists.range(9000).map(i, [i])) > 0`,
		// Manufactured bytes, which the estimator prices by size.
		"a string amplified by replacement": `size("` + strings.Repeat("x", 1400) +
			`".replace("x", "` + strings.Repeat("y", 1400) + `")) > 0`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			compiled, err := NewRunFilter(filter)
			require.NoError(t, err, "the filter must compile, or the refusal below proves nothing about evaluation")

			_, err = compiled.Match(t.Context(), &RunSummary{WorkflowId: "orders-1"})
			require.Error(t, err, "a filter spent past its budget and was answered anyway")
		})
	}

	// The bound reached rather than merely exceeded: an ordinary filter over
	// the same vocabulary still answers, so the refusals above are the budget
	// rather than the controls refusing everything they are now installed for.
	compiled, err := NewRunFilter(`labels.?team.orValue("") == "platform" && lists.range(10).size() == 10`)
	require.NoError(t, err)

	matched, err := compiled.Match(t.Context(), &RunSummary{
		WorkflowId: "orders-1",
		Labels:     map[string]string{"team": "platform"},
	})
	require.NoError(t, err)
	require.True(t, matched)
}
