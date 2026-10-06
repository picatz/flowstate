package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The run bound lives in the library as well as the command: a caller of
// RunOptions cannot ask for more than the command would generate.
func TestNewFuzzerClampsRunsToTheBound(t *testing.T) {
	t.Parallel()

	assert.Equal(t, MaxFuzzRuns, newFuzzer(FuzzOptions{Runs: MaxFuzzRuns * 1000}).opts.Runs)
	assert.Equal(t, 7, newFuzzer(FuzzOptions{Runs: 7}).opts.Runs)
	assert.Nil(t, newFuzzer(FuzzOptions{}))
}
