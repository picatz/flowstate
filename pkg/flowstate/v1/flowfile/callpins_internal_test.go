package flowfile

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEveryStepListIsSpelledStepsKey holds the claim [stepsKey] makes, which
// [pinCollector] rests its whole notion of "this mapping is a step" on: every
// place in the grammar that takes a list of steps writes it under the same key.
//
// Read from the grammar's own key lists rather than from a second list written
// here, so a block that grows a step list under some other spelling is caught by
// its key list not containing this one — the reason [stepsKey] is a constant at
// all. Without it, a pin inside that new block would silently stop being read,
// and `flow fmt` would drop it: the #339 failure returning by way of a grammar
// addition nobody connected to the formatter.
func TestEveryStepListIsSpelledStepsKey(t *testing.T) {
	for name, keys := range map[string][]string{
		"workflow":        workflowKeys,
		"for_each":        forEachKeys,
		"loop":            loopKeys,
		"parallel:branch": branchKeys,
		"switch:case":     switchCaseKeys,
		"switch:default":  switchDefaultKeys,
	} {
		assert.True(t, slices.Contains(keys, stepsKey),
			"%s takes a list of steps but does not spell it %q, so pin collection cannot see the steps in it",
			name, stepsKey)
	}
}
