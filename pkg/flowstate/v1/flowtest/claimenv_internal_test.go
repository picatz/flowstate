package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTheLoadClaimEnvironmentIsNotTheLibraryLessOne pins the property the
// end-to-end cases depend on, at the seam rather than through a whole run.
//
// The bug was one call: `Env()` with no libraries where the run used the
// profile's. A regression would be the same one call, and it would show up here
// as `.?` failing to parse — the syntax whose absence #1512 was reported for —
// before any suite had to be written to notice.
//
// Not a source grep for a library-less `Env()`, because this package has
// legitimate ones: a `vars:` value evaluates in the base environment on
// purpose, which is the whole reason [varProfileFunctions] exists to name a
// profile-gated function an author reached for. The property worth guarding is
// about the claim path, so it is asserted about the claim path.
func TestTheLoadClaimEnvironmentIsNotTheLibraryLessOne(t *testing.T) {
	t.Parallel()

	ev := v1.DefaultEvaluator()

	env, err := loadClaimEnv(ev)
	require.NoError(t, err)

	_, issues := env.Parse(`x.?a.orValue(0) == 1`)
	assert.NoError(t, issues.Err(),
		"the load-time claim environment refuses optional syntax, so it is the library-less one again")

	// And the base environment still refuses it, so the assertion above is
	// about the environment rather than about cel-go having changed.
	base, err := ev.Env()
	require.NoError(t, err)

	_, baseIssues := base.Parse(`x.?a.orValue(0) == 1`)
	require.Error(t, baseIssues.Err(),
		"the base environment now accepts optional syntax, so the case above proves nothing")
}

// TestEveryProfileLibraryCoversEveryProfile keeps the union honest.
//
// It is the reason a claim in any profile's vocabulary loads, so a profile added
// without its libraries reaching this set would narrow what loads without
// anything failing.
func TestEveryProfileLibraryCoversEveryProfile(t *testing.T) {
	t.Parallel()

	union := everyProfileLibrary()
	require.NotEmpty(t, union, "an empty union is the library-less environment by another name")

	for _, name := range v1.ProfileNames() {
		libs, err := v1.ProfileLibraries(name)
		require.NoError(t, err)

		for _, lib := range libs {
			assert.Contains(t, union, lib,
				"profile %q includes %q, which the load-time claim environment does not", name, lib)
		}
	}
}
