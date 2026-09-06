package flowstatev1

import (
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
