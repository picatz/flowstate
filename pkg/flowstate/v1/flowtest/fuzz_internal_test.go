package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The run bound lives in the library as well as the command: a caller of
// RunOptions cannot ask for more than the command would generate.
func TestNewFuzzerClampsRunsToTheBound(t *testing.T) {
	t.Parallel()

	assert.Equal(t, MaxFuzzRuns, newFuzzer(FuzzOptions{Runs: MaxFuzzRuns * 1000}).opts.Runs)
	assert.Equal(t, 7, newFuzzer(FuzzOptions{Runs: 7}).opts.Runs)
	assert.Nil(t, newFuzzer(FuzzOptions{}))
}

// The first seeds walk every boundary value of every input, one at a time with
// the rest as the case wrote them, and every optional input absent, so a small
// run still exercises each of them and a failure is localized to one input.
func TestCorpusWalksEachBoundaryOneInputAtATime(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{DeclaredInputs: []*v1.InputDeclaration{
		{Name: "flag", Type: v1.InputDeclaration_TYPE_BOOL, Required: true},
		{Name: "count", Type: v1.InputDeclaration_TYPE_BOOL},
	}}
	base := map[string]any{"flag": true, "count": true}
	slots, _ := inputSlots(spec)

	var seen []map[string]any
	for n := range uint64(8) {
		inputs, ok := corpusInputs(spec, base, slots, n)
		if !ok {
			break
		}
		seen = append(seen, inputs)
	}
	assert.Equal(t, []map[string]any{
		{"flag": true, "count": true},
		{"flag": false, "count": true},
		{"flag": true},
		{"flag": true, "count": true},
		{"flag": true, "count": false},
	}, seen)
}
