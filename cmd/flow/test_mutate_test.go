package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const mutateCLIWorkflow = `
edition: v2026.4
name: gates
inputs:
  mode:
    type: string
    required: true
steps:
  - id: always
    log:
      message: always runs
  - id: on_ready
    if: ${inputs.mode == 'ready'}
    log:
      message: ready path
`

func writeMutateFixture(t *testing.T, expect string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(mutateCLIWorkflow), 0o600))
	suite := `
tests:
  - name: ready
    workflow: ./workflow.yaml
    inputs: {mode: ready}
    stubs: [{task: log, returns: {}}]
    expect:
` + expect
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(suite), 0o600))

	return dir
}

// A suite whose one case takes a single branch cannot tell the gate from its absence: that is a
// survivor, which fails the command and names the replay.
func TestMutateFailsTheCommandOnASurvivor(t *testing.T) {
	oneBranch := writeMutateFixture(t, "      ran: [always, on_ready]\n")
	out, err := runFlowTest(t, "--mutate", oneBranch)
	// The single case takes the ready branch only, so dropping the gate cannot be
	// told from the program: the survivor says the file needs the other branch.
	require.Error(t, err)
	assert.Contains(t, out, "survived: `if:` removed from step on_ready")
	assert.Contains(t, out, "flow test --mutant if-drop@on_ready.if --")

	replay, err := runFlowTest(t, "--mutant", "if-drop@on_ready.if", oneBranch)
	require.Error(t, err)
	assert.Contains(t, replay, "1 mutant: 0 killed, 1 survived")

	missing, stderr, err := runFlowTestStreams(t, "--mutant", "if-drop@nowhere.if", oneBranch)
	require.Error(t, err)
	assert.Contains(t, stderr, "names no mutant of any workflow")
	_ = missing
}

func TestMutateFlagsAreRefusedWhereTheyCannotWork(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"with seeds": {[]string{"--mutate", "--seeds", "3"}, "run them separately"},
		"with fuzz":  {[]string{"--mutate", "--fuzz", "3"}, "run them separately"},
		"with debug": {[]string{"--mutate", "--debug"}, "cannot be combined with --debug"},
		"negative":   {[]string{"--mutate=-1"}, "not a count"},
		"too many":   {[]string{"--mutate=100000"}, "above the"},
		"both":       {[]string{"--mutate=5", "--mutant", "x"}, "pass one or the other"},
		"with watch": {[]string{"--mutate", "--watch"}, "cannot be combined with --watch"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := newTestCommand()
			require.NoError(t, cmd.ParseFlags(tc.args))
			budget, err := scheduleBudget(cmd)
			require.NoError(t, err)
			fuzz, err := fuzzOptions(cmd, budget)
			if err != nil {
				assert.Contains(t, err.Error(), tc.want)

				return
			}
			_, err = mutateOptions(cmd, budget, fuzz)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestMutationRendersSurvivorsAndFailsTheFile(t *testing.T) {
	render := func(m *v1.MutationReport) (string, testFileResult) {
		report := &v1.TestReport{File: "gates.test.yaml", Mutation: m, Cases: []*v1.TestCase{{Passed: true}}}
		var out strings.Builder
		printMutation(&out, ui.Plain(&out, &out).Theme, report)

		return out.String(), testFileResult{report: report}
	}

	clean, result := render(&v1.MutationReport{Mutants: 4, Killed: 4})
	assert.Contains(t, clean, "4 mutants: 4 killed, 0 survived")
	assert.False(t, result.failed(false, false))

	_, result = render(&v1.MutationReport{Mutants: 1, Survivors: []*v1.MutationSurvivor{{Id: "if-drop@a.if"}}})
	assert.True(t, result.failed(false, false))

	redRun, result := render(&v1.MutationReport{NotRun: "a case in this file did not pass"})
	assert.Contains(t, redRun, "not mutated")
	assert.True(t, result.failed(false, false))

	none, _ := render(nil)
	assert.Empty(t, none)
}

// A survivor fails the exit code, so the JUnit document must say so too: a
// synthetic verdict, not every case green beside a red exit.
func TestMutationVerdictsReachJUnit(t *testing.T) {
	survived := testFileResult{report: &v1.TestReport{Mutation: &v1.MutationReport{
		Mutants: 2, Survivors: []*v1.MutationSurvivor{{Id: "if-drop@a.if"}},
	}}}
	assert.Contains(t, nonCaseVerdicts(survived, false, false), "--mutate: 1 mutant(s) survived, so the file would not notice the program changing")

	notRun := testFileResult{report: &v1.TestReport{Mutation: &v1.MutationReport{NotRun: "a case did not pass"}}}
	assert.Contains(t, nonCaseVerdicts(notRun, false, false), "--mutate: not mutated: a case did not pass")

	clean := testFileResult{report: &v1.TestReport{Mutation: &v1.MutationReport{Mutants: 2, Killed: 2}}}
	assert.Empty(t, nonCaseVerdicts(clean, false, false))
}
