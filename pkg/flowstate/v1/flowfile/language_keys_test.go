package flowfile

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLanguageGuideNamesEveryKey keeps docs/LANGUAGE.md's "Keys at a glance"
// section tied to the grammar the parser accepts. The guide says it teaches every
// construct; a key the parser accepts and the table omits is a construct a reader
// learns about from a diagnostic, which is how three top-level keys and two step
// kinds shipped undocumented in one week.
//
// The check is one-directional on purpose. The table may name more than the
// lists below (a task name, a reserved word, a prose aside), but every key the
// parser takes must appear in it as a code span. Adding a key to the grammar
// without teaching it fails here, in the same pull request.
func TestLanguageGuideNamesEveryKey(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(repoRoot(), "docs", "LANGUAGE.md"))
	require.NoError(t, err)

	_, table, found := strings.Cut(string(data), "\n## Keys at a glance\n")
	require.True(t, found, `docs/LANGUAGE.md has no "Keys at a glance" section`)

	named := map[string]bool{}
	for _, m := range regexp.MustCompile("`([a-z_]+)`").FindAllStringSubmatch(table, -1) {
		named[m[1]] = true
	}

	groups := map[string][]string{
		"top level":           workflowKeys,
		"input declaration":   inputKeys,
		"output declaration":  outputKeys,
		"step property":       stepPropertyKeys,
		"step kind":           nodeKindKeys,
		"retry":               retryKeys,
		"for_each":            forEachKeys,
		"loop":                loopKeys,
		"branch":              branchKeys,
		"switch":              switchKeys,
		"switch case":         switchCaseKeys,
		"switch default":      switchDefaultKeys,
		"signal wait":         signalKeys,
		"batched signal wait": signalBatchKeys,
		"quorum":              signalQuorumKeys,
		"type":                typeKeys,
		"function":            functionKeys,
		"declared error":      errorKeys,
		"fail":                failKeys,
		"manual trigger":      manualKeys,
		"webhook trigger":     webhookKeys,
		"webhook signal":      webhookSignalKeys,
		"schedule":            scheduleKeys,
		"calendar":            calendarKeys,
		"calendar range":      calendarRangeKeys,
		"concurrency":         concurrencyKeys,
		"signal policy":       signalPolicyKeys,
		"signal rule":         signalRuleKeys,
	}

	var problems []string
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		var missing []string
		for _, key := range groups[group] {
			if !named[key] {
				missing = append(missing, key)
			}
		}
		if len(missing) > 0 {
			slices.Sort(missing)
			problems = append(problems, group+": "+strings.Join(missing, ", "))
		}
	}
	require.Empty(t, problems,
		"docs/LANGUAGE.md \"Keys at a glance\" omits keys the parser accepts; teach each where the guide explains the construct and list it in the table")
}
