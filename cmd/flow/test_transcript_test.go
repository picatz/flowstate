package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The failure transcript in the CLI (#929 slice 2): a failing case's unmet
// expectation arrives with the account it was judged against, `-v` shows
// every case's, and machine stdout carries none of it — where a machine
// transcript lives, if anywhere, is #923's decision.

// writeTranscriptFixture is one passing case and one failing case over a
// two-step workflow, so both the default and the verbose direction have
// something to show or withhold.
func writeTranscriptFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: pair
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
outputs: {}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.3
tests:
  - name: passes quietly
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [first, second]
  - name: expects an output nothing produces
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [first, second]
      outputs:
        ghost: 1
`), 0o600))
	return dir
}

// TestAFailingCasePrintsItsTranscript: the default. The failing case's
// account appears with no flag at all; the passing case's does not.
func TestAFailingCasePrintsItsTranscript(t *testing.T) {
	dir := writeTranscriptFixture(t)

	out, err := runFlowTest(t, dir)
	require.Error(t, err)
	assert.Contains(t, out, "t=0s")
	assert.Contains(t, out, `stub 1 (task "log")`)
	assert.Equal(t, 1, strings.Count(out, "t=0s     first"),
		"only the failing case's account prints without -v; the passing case ran the same step")
}

// TestVerbosePrintsEveryCasesTranscript: `-v` is the go test reading — every
// case's account, passing or not.
func TestVerbosePrintsEveryCasesTranscript(t *testing.T) {
	dir := writeTranscriptFixture(t)

	out, err := runFlowTest(t, "-v", dir)
	require.Error(t, err, "the failing case still fails; -v changes what prints, never the verdict")
	assert.Equal(t, 2, strings.Count(out, "t=0s     first"),
		"both cases' accounts print under -v")
}

// TestMachineModeCarriesNoTranscript: stdout stays exactly the schema
// document; the transcript is CLI text until #923 decides a schema home.
func TestMachineModeCarriesNoTranscript(t *testing.T) {
	dir := writeTranscriptFixture(t)

	out, errOut, err := runFlowTestStreams(t, "-o", "json", dir)
	require.Error(t, err)
	assert.NotContains(t, out, "t=0s")
	assert.NotContains(t, errOut, "t=0s")
}
