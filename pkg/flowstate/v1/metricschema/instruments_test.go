package metricschema_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// What this file is for.
//
// [metricschema.Instruments] is a declaration, and a declaration nothing checks
// is a comment. These tests make it load-bearing in the two directions that can
// fail: an instrument declaring an attribute key the schema would refuse (so the
// table promises something the filter would silently drop), and an instrument
// created somewhere in the repository that the table does not know about (so the
// one list of what this system emits is not the list).

// TestEveryInstrumentDeclaresAllowlistedKeys is the first direction.
//
// A declaration naming a key outside the allowlist is worse than no declaration:
// it reads as a promise that the label is there, while [metricschema.Attributes]
// drops it at the recording site. The two have to agree, and this is the check
// that makes the table's Keys mean what they say.
func TestEveryInstrumentDeclaresAllowlistedKeys(t *testing.T) {
	t.Parallel()

	for _, instrument := range metricschema.Instruments {
		for _, key := range instrument.Keys {
			_, ok := metricschema.Classification(key)
			require.Truef(t, ok,
				"%s declares %q, which the schema refuses on any instrument — either it is bounded and belongs in Table, or it must not be a label",
				instrument.Name, key)
		}
	}
}

// TestInstrumentNamesAreDistinctAndNamespaced pins the shape of a name.
//
// Every instrument in this repository is a `flowstate.` one — the plugin surface
// already was, and #526's engine-level instruments follow it — because renaming
// an instrument breaks every dashboard built on it, so the convention is worth
// enforcing on the day one is added rather than the day after.
func TestInstrumentNamesAreDistinctAndNamespaced(t *testing.T) {
	t.Parallel()

	seen := map[string]struct{}{}
	for _, instrument := range metricschema.Instruments {
		require.Truef(t, strings.HasPrefix(instrument.Name, "flowstate."),
			"%s is not in this repository's namespace", instrument.Name)

		_, duplicate := seen[instrument.Name]
		require.Falsef(t, duplicate, "%s is declared twice", instrument.Name)
		seen[instrument.Name] = struct{}{}
	}

	require.Len(t, metricschema.InstrumentNames(), len(metricschema.Instruments))
}

// TestEveryInstrumentCreatedInTheRepositoryIsDeclared is the second direction,
// and the one that would catch an instrument nobody wrote down.
//
// The walk looks for the OTel constructors — Int64Counter, Float64Histogram and
// the rest — and requires that the name each is given is a
// [metricschema.Instruments] constant rather than a string literal. A literal
// name is exactly how the six plugin instruments came to be a list only the
// plugin package knew about (#526's own survey had to grep for them), so the
// rule is: the schema declares the name, the recording site reads it.
func TestEveryInstrumentCreatedInTheRepositoryIsDeclared(t *testing.T) {
	t.Parallel()

	// A constructor call whose first argument is a string literal, which is the
	// shape this test refuses. Matching the literal rather than the constant is
	// what makes the failure specific: the fix is always to move the name.
	literal := regexp.MustCompile(`\.(Int64Counter|Int64UpDownCounter|Int64Gauge|Int64Histogram|Float64Counter|Float64UpDownCounter|Float64Gauge|Float64Histogram|Int64ObservableCounter|Int64ObservableUpDownCounter|Int64ObservableGauge|Float64ObservableCounter|Float64ObservableUpDownCounter|Float64ObservableGauge)\("`)

	var offenders []string

	require.NoError(t, filepath.WalkDir(repoRoot(t), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata":
				return filepath.SkipDir
			}
			// Same reason [TestEveryMetricRecordingSiteGoesThroughTheSchema]
			// prunes it: a worktree is another checkout of this repository, and
			// walking into one reports every site twice.
			if rel, err := filepath.Rel(repoRoot(t), path); err == nil {
				if filepath.ToSlash(rel) == ".claude/worktrees" {
					return filepath.SkipDir
				}
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(contents), "\n") {
			if literal.MatchString(line) {
				rel, _ := filepath.Rel(repoRoot(t), path)
				offenders = append(offenders, rel+": "+strings.TrimSpace(line))
			}
		}

		return nil
	}))

	sort.Strings(offenders)

	require.Empty(t, offenders,
		"these create an instrument under a literal name; declare it in metricschema.Instruments and read the constant, "+
			"so one file lists what this system emits")
}
