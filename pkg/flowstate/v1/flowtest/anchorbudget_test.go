package flowtest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestManyMissingOutputsAnchorWithinOneBudget is #1119's quadratic post-run
// pass, asserted where the budget is observable rather than on a clock.
//
// A case may expect any number of outputs — nothing bounds that map's entries
// beyond the file's own megabyte — and a workflow producing none of them makes
// one failure per expected output. Anchoring then asks the retained YAML tree
// where each was written, and locating a mapping key scans that mapping from
// its start, so N outputs cost N scans of a mapping N long. A lookup budget
// granted afresh to each finding bounds an individual scan and says nothing
// about their sum.
//
// Shared across the pass, the budget runs out partway through and the findings
// after it are reported without a position — which is this mechanism's own
// documented degradation, and the thing a per-finding budget could never
// produce: every individual scan here is far inside 100,000 steps, so under
// that rule all of them would be positioned however many there were.
//
// So the claim is exactly the one the fix makes: some are placed, some are not,
// and every finding is still reported either way.
func TestManyMissingOutputsAnchorWithinOneBudget(t *testing.T) {
	t.Parallel()

	const workflow = "edition: v2026.3\nname: demo\nsteps:\n- id: hi\n  log:\n    message: hello\n"

	// Enough entries that the shared budget is spent partway through, and far
	// too few for any single scan to approach it on its own.
	const outputs = 3000

	var tests strings.Builder
	tests.WriteString("tests:\n  - name: many\n    stubs:\n      - task: log\n        returns: {}\n")
	tests.WriteString("    expect:\n      outputs:\n")
	for i := range outputs {
		fmt.Fprintf(&tests, "        out%06d: x\n", i)
	}

	report := flowtest.RunSource("<submitted>", []byte(workflow), []byte(tests.String()))

	require.Empty(t, report.GetRefused(), "the tests document itself must load: %v", report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	failures := report.GetCases()[0].GetFailures()
	require.NotEmpty(t, failures, "the workflow declares none of these outputs, so the case must report findings")

	var placed, unplaced int
	for _, finding := range failures {
		if finding.GetLine() == 0 {
			unplaced++

			continue
		}
		placed++
	}

	require.Positive(t, placed,
		"no finding was positioned at all, so the budget is not being spent on lookups that work")
	require.Positive(t, unplaced,
		"every one of %d findings was positioned, so each got a budget of its own and their sum is unbounded",
		len(failures))
}
