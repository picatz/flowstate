package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow run local` is the command an author uses most, and the one with the most
// ways to get the two streams wrong: it is the only verb that both narrates a run
// and produces its result, in the same process, at the same time.
//
// The rule the CLI states everywhere applies hardest here. stdout is the answer —
// one JSON document, nothing else, whatever the workload logged. stderr is the
// account of it.

// runLocal runs a workflow through the real command and returns its two streams
// separately, which is the whole point: a test that merged them could not see the
// mistake this is guarding against.
//
// Through [execute] rather than root.Execute, because that is the entry point — it
// is where SilenceUsage is set and where a failure is rendered. Calling Execute
// directly tests a CLI nobody runs, and the first version of this did: it saw
// cobra's usage block on stdout and reported it as the command's mistake.
//
// Serial for the reason TestBuildingTheCLITwiceBuildsTheSameCLI records: building a
// CLI writes to package state.
func runLocal(t *testing.T, body string, extra ...string) (stdout, stderr string, err error) {
	t.Helper()

	return runLocalUnder(t, t.Context(), body, extra...)
}

// runLocalUnder is the same, under a context the caller controls, which is how a
// test stands in for somebody pressing ctrl+c.
func runLocalUnder(t *testing.T, ctx context.Context, body string, extra ...string) (stdout, stderr string, err error) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"run", "local", path}, extra...))

	err = execute(ctx, root)

	return out.String(), errOut.String(), err
}

// A workload that logs, so the narration and the result are both present and can be
// told apart.
const narratingWorkflow = `edition: v2026.2
name: narrates
steps:
  - id: hello
    log:
      message: hello from the workload
`

// TestALocalRunWritesOneDocumentToStdout is the property a pipe depends on.
//
// `flow run local x | jq` has to receive exactly one JSON document. A workload that
// narrates itself — which is what `log:` is for — must not be able to break that by
// existing, and neither may the command's own status line.
func TestALocalRunWritesOneDocumentToStdout(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, narratingWorkflow)
	require.NoError(t, err)

	var outputs map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &outputs),
		"stdout is not a single JSON document:\n%s", stdout)

	assert.Contains(t, outputs, "stepValues",
		"the document on stdout is not the run's outputs")
	assert.NotContains(t, stdout, "hello from the workload",
		"what the workload logged reached the stream a pipe reads")
}

// TestALocalRunSaysHowItWentOnStderr pins where the account goes, and in whose
// words.
//
// This line used to be `log.Println("run completed")` — the one user-facing line in
// the CLI that went through the standard logger, so it arrived timestamped and
// unstyled directly beneath the themed lines a `log:` step had just written. Two
// renderings of one program's output, one line apart.
//
// It now says what `flow get` says about the same outcome, which is the vocabulary
// rule applied to the two drivers: a run that finished is COMPLETED whichever one
// executed it.
func TestALocalRunSaysHowItWentOnStderr(t *testing.T) {
	t.Parallel()

	_, stderr, err := runLocal(t, narratingWorkflow)
	require.NoError(t, err)

	assert.Contains(t, stderr, "COMPLETED",
		"a local run did not say how it went, or did not use the word the rest of the CLI uses")
	assert.Contains(t, stderr, "narrates",
		"the status line does not name the workload it is about")
	assert.Contains(t, stderr, "hello from the workload",
		"what the workload logged did not reach the stream the account goes to")

	// The shape the old line had, which is what made it look like a different
	// program. Nothing here should carry a date.
	assert.NotRegexp(t, `\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}`, stderr,
		"a status line arrived timestamped by the standard logger rather than rendered")
}

// TestALocalRunThatFailsWritesNothingToStdout keeps the failure path honest.
//
// An empty stdout is a meaningful value: it is what "no answer" looks like to a
// program. A half-written or partial document would be worse than nothing, because
// a reader would parse it.
func TestALocalRunThatFailsWritesNothingToStdout(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, `edition: v2026.2
name: refuses
steps:
  - id: web
    http:
      method: FETCH
      url: https://example.com
`)
	require.Error(t, err, "a workflow the validator refuses was run anyway")

	assert.Empty(t, stdout,
		"a run that never produced an answer still wrote something to the stream a pipe reads")
}
