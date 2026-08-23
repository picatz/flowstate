package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow test --run <pattern>` (#929 slice 1): filter cases by name, the
// `go test -run` precedent, under the house honesty rule that a skip must be
// a decision you can read — every filtered run says what it left out, beside
// the file and on the summary line.

// writeFilterFixture is a two-case suite whose names a pattern can tell
// apart.
func writeFilterFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(scheduleStraightWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.3
tests:
  - name: the fast case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [only]
  - name: the slow case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [only]
`), 0o600))
	return dir
}

// TestRunFilterSelectsByNameAndSaysWhatItLeftOut: the selected case runs, the
// other does not, and both the per-file line and the summary carry the count.
func TestRunFilterSelectsByNameAndSaysWhatItLeftOut(t *testing.T) {
	dir := writeFilterFixture(t)

	out, err := runFlowTest(t, "--run", "fast", dir)
	require.NoError(t, err)
	assert.Contains(t, out, "the fast case")
	assert.NotContains(t, out, "the slow case")
	assert.Contains(t, out, `--run "fast": ran 1 of 2 cases, 1 filtered out`)
	assert.Contains(t, out, "1 passed · 1 case filtered out ·",
		"the summary line must carry the filter count, so its green is legibly a subset's")
}

// TestRunFilterMatchingNothingStillSaysSo: a pattern that selects nothing
// passes vacuously — the go test precedent — and the two honesty lines are
// what keep that green readable as "nothing ran" rather than "all good".
func TestRunFilterMatchingNothingStillSaysSo(t *testing.T) {
	dir := writeFilterFixture(t)

	out, err := runFlowTest(t, "--run", "nothing matches this", dir)
	require.NoError(t, err)
	assert.Contains(t, out, "ran 0 of 2 cases, 2 filtered out")
	assert.Contains(t, out, "0 passed · 2 cases filtered out ·")
}

// TestRunFilterRefusesCoverageRequired: a subset's gaps are not the suite's,
// so enforcing the suite's bar over a filtered run is refused rather than
// quietly answered about the wrong thing.
func TestRunFilterRefusesCoverageRequired(t *testing.T) {
	dir := writeFilterFixture(t)

	_, err := runFlowTest(t, "--run", "fast", "--coverage-required", dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--coverage-required cannot be combined with --run")
}

// TestRunFilterRefusesABadPattern: a pattern that does not compile selects
// nothing knowable, and the refusal names it.
func TestRunFilterRefusesABadPattern(t *testing.T) {
	dir := writeFilterFixture(t)

	_, err := runFlowTest(t, "--run", "[", dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `--run "[" is not a valid regular expression`)
}

// TestRunFilterInMachineMode: stdout stays exactly the document — carrying
// only the selected cases — and the honesty line lands on stderr, where the
// schedules account already goes for the same reason.
func TestRunFilterInMachineMode(t *testing.T) {
	dir := writeFilterFixture(t)

	out, errOut, err := runFlowTestStreams(t, "-o", "json", "--run", "fast", dir)
	require.NoError(t, err)
	require.True(t, json.Valid([]byte(out)), "stdout was not a single JSON document:\n%s", out)

	var envelope struct {
		Files []struct {
			Cases []struct {
				Name string `json:"name"`
			} `json:"cases"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &envelope))
	require.Len(t, envelope.Files, 1)
	require.Len(t, envelope.Files[0].Cases, 1)
	assert.Equal(t, "the fast case", envelope.Files[0].Cases[0].Name)

	assert.Contains(t, errOut, "ran 1 of 2 cases, 1 filtered out")
	assert.NotContains(t, out, "filtered out")
}

// TestNoFilterPrintsNoFilterLines is the compatibility half: without the
// flag, neither the per-file line nor the summary part exists, so an
// unfiltered run renders exactly as it did before the flag.
func TestNoFilterPrintsNoFilterLines(t *testing.T) {
	dir := writeFilterFixture(t)

	out, err := runFlowTest(t, dir)
	require.NoError(t, err)
	assert.NotContains(t, out, "--run")
	assert.NotContains(t, out, "filtered")
}
