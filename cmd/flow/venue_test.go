package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The venue announcement, which exists because the venue used to be something a
// reader inferred.
//
// Every assertion here is about a fact the CLI states rather than about styling:
// what the line says, which stream it lands on, when it is written relative to the
// work, and what it must never contain. See venue.go.

// TestALocalRunSaysWhereItRan is the announcement at its simplest.
//
// The negative half is the point rather than the positive one: the local venue
// contacts nothing, so a line naming an address would be describing a server this
// run has no relationship with. `localhost:9233` is the default `--address` carries
// everywhere else in this CLI, which makes it exactly the string that would leak in
// if the two venues were ever announced through one code path that forgot which it
// was on.
func TestALocalRunSaysWhereItRan(t *testing.T) {
	t.Parallel()

	stdout, stderr, err := runLocal(t, narratingWorkflow)
	require.NoError(t, err)

	assert.Contains(t, stderr, "running locally",
		"a local run said nothing about where it ran")
	assert.NotContains(t, stderr, defaultServerAddress,
		"a run that contacted no server named one anyway")
	assert.NotContains(t, stdout, "running",
		"the venue announcement reached the stream a pipe reads")
}

// TestTheVenueIsAnnouncedBeforeTheWorkloadRuns pins the ordering the decision
// asked for: one line at start, before any work.
//
// An announcement written after the workload has already narrated two lines is not
// an announcement, it is a footnote. Asserted by position within one stream rather
// than by counting lines, so a workload that logs more or a command that grows
// another notice does not make this test say something else.
func TestTheVenueIsAnnouncedBeforeTheWorkloadRuns(t *testing.T) {
	t.Parallel()

	_, stderr, err := runLocal(t, narratingWorkflow)
	require.NoError(t, err)

	venue := strings.Index(stderr, "running locally")
	narration := strings.Index(stderr, "hello from the workload")

	require.NotEqual(t, -1, venue, "no venue was announced:\n%s", stderr)
	require.NotEqual(t, -1, narration, "the workload never ran:\n%s", stderr)
	assert.Less(t, venue, narration,
		"the venue was announced after the workload had already started talking:\n%s", stderr)
}

// TestAMachineCallerIsStillToldTheVenue covers the invocations that most need it.
//
// `flow run -o json` in a pipeline is the case where nobody is watching a terminal
// and the address comes from whatever the environment holds, so suppressing the
// line for a machine format would silence it exactly where a surprise costs most.
// It stays on stderr, so the document on stdout is still the single JSON value a
// pipe can read.
func TestAMachineCallerIsStillToldTheVenue(t *testing.T) {
	t.Parallel()

	stdout, stderr, err := runLocal(t, narratingWorkflow, "--output", "json")
	require.NoError(t, err)

	assert.Contains(t, stderr, "running locally",
		"a run asked for a document was told nothing about where it ran")

	var run map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &run),
		"the announcement broke the one document on stdout:\n%s", stdout)
}

// TestADurableRunNamesTheServerAndWhoItPresents is the other venue, through the
// command that reaches it.
//
// Both halves are configuration this process holds: the address it dialled, and
// the credential it will present. The tenant the server derives from that
// credential is deliberately not here; venue.go says why.
func TestADurableRunNamesTheServerAndWhoItPresents(t *testing.T) {
	fake := &fakeWorkflowService{
		runResponse: &v1.RunResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_COMPLETED,
		},
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_COMPLETED,
		},
	}
	address := serveFake(t, fake)

	cmd, _, errOut := watchCommandForTest(t)
	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))

	assert.Contains(t, errOut.String(), "running on "+address+" as an anonymous caller",
		"a durable run said nothing about which deployment it was submitted to:\n%s", errOut.String())
	assert.NotContains(t, errOut.String(), "running locally",
		"a run submitted to a server announced the venue that executes here")
}

// TestTheVenueIsAnnouncedEvenWhenTheRunNeverStarts is the case the announcement is
// most useful in and the one it would be easiest to lose.
//
// A command line that never becomes a run still went somewhere, and the shell it
// was typed in still holds whichever address it holds. Announcing only once a file
// has loaded and a server has answered would mean the invocations that fail are the
// ones that never say which deployment they were about to touch.
func TestTheVenueIsAnnouncedEvenWhenTheRunNeverStarts(t *testing.T) {
	t.Setenv("FLOWSTATE_ADDRESS", "flowstate.example.test:9233")

	cmd, _, errOut := watchCommandForTest(t)

	require.Error(t, runWorkflow(cmd, []string{filepath.Join(t.TempDir(), "absent.yaml")}),
		"a workflow file that does not exist was somehow run")
	assert.Contains(t, errOut.String(), "running on flowstate.example.test:9233",
		"a run that failed before it started never said where it was going:\n%s", errOut.String())
}

// TestTheAnnouncementNamesACredentialWithoutRevealingIt is the containment claim.
//
// A venue line is written to a terminal, into CI logs, and into whatever collects
// them, so the one thing it may never carry is the credential itself. It names the
// configuration that supplied the token instead, which is the fact a reader
// actually needs: which shell they are in.
//
// The token-file case asserts the same in the other direction, since a path is safe
// to print and the bytes at the end of it are not. Nothing here reads the file, so
// the test writes one and then checks its contents did not travel.
func TestTheAnnouncementNamesACredentialWithoutRevealingIt(t *testing.T) {
	const secret = "eyJhbGciOiJFUzI1NiJ9.this-is-the-token.signature"

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(secret), 0o600))

	for _, tc := range []struct {
		name    string
		flags   serverFlags
		environ map[string]string
		says    string
		forbids string
	}{
		{
			name:  "nothing configured",
			flags: serverFlags{address: "prod.example.test:9233"},
			says:  "running on prod.example.test:9233 as an anonymous caller",
		},
		{
			name:    "a token in the environment",
			flags:   serverFlags{address: "prod.example.test:9233"},
			environ: map[string]string{"FLOWSTATE_TOKEN": secret},
			says:    "running on prod.example.test:9233 as the identity in FLOWSTATE_TOKEN",
			forbids: secret,
		},
		{
			name:    "a token file, which wins over the variable",
			flags:   serverFlags{address: "prod.example.test:9233", tokenFile: tokenFile},
			environ: map[string]string{"FLOWSTATE_TOKEN": secret},
			says:    "running on prod.example.test:9233 as the identity in " + tokenFile,
			forbids: secret,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var out, errOut strings.Builder
			cmd := &cobra.Command{}
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)

			announceVenue(cmd, serverVenue(tc.flags, func(name string) string { return tc.environ[name] }))

			assert.Equal(t, tc.says+"\n", errOut.String())
			if tc.forbids != "" {
				assert.NotContains(t, errOut.String(), tc.forbids,
					"the credential itself was printed where a log will keep it")
			}
			assert.Empty(t, out.String(), "the announcement reached the answer stream")
		})
	}
}
