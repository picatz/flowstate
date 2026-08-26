package flowtesting_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest/flowtesting"
)

// A workflow with somewhere to walk to, and something to look at when you get
// there.
const stagedWorkflow = `
edition: v2026.3
name: staged
steps:
  - id: build
    value: "'web.tar.gz'"
  - id: test
    value: "'3 passed'"
  - id: deploy
    log:
      message: shipping
outputs: {}
`

// TestWalkStepsThroughACaseFromGo is the rung this package existed without.
//
// [flowtest.RunOptions.Debugger] has always accepted a session, and until
// [flowdebug.Session.Control] there was nothing a Go test could do with it —
// the run parks blocked reading a line of text, and a test has nobody to type.
// So the seam was reachable in principle and from nowhere an author writes.
//
// What this asserts is the whole of that claim end to end: a case stops where
// its steps are, the walk sees each stop in order, and what an earlier step
// produced can be read at a later one.
func TestWalkStepsThroughACaseFromGo(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), stagedWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: it ships
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, test, deploy]
`)

	var reached []string

	flowtesting.RunFile(t, path, flowtesting.WithWalk("it ships", func(walk *flowtesting.Walk) {
		// The first stop is where the run already is; every Step after it is
		// one more step of the real local driver.
		for at, ok := walk.Step(); ok; at, ok = walk.Step() {
			reached = append(reached, at.Step)
		}
	}))

	assert.Equal(t, []string{"test", "deploy"}, reached,
		"the walk did not visit the run's own steps in order")
}

// TestWalkReadsTheScopeAtAStop is the other half: moving a run is worth little
// without asking about where it stopped.
func TestWalkReadsTheScopeAtAStop(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), stagedWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: it ships
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, test, deploy]
`)

	flowtesting.RunFile(t, path, flowtesting.WithWalk("it ships", func(walk *flowtesting.Walk) {
		at, ok := walk.Until("deploy")
		require.True(t, ok, "the run finished before reaching the step named")
		assert.Equal(t, "deploy", at.Step)

		// What the earlier steps produced, read as a plain Go value — which is
		// what an assertion wants, and the reason Value exists beside the
		// session's own rendered and typed answers.
		assert.Equal(t, "web.tar.gz", walk.Value("steps.build.value"))
		assert.Equal(t, "3 passed", walk.Value("steps.test.value"))

		// And the run can say what it can name, which is what an adapter fills
		// a variables pane from.
		var steps []string
		for _, group := range walk.Names() {
			if group.Group == "steps" {
				steps = group.Names
			}
		}
		assert.Equal(t, []string{"build", "test"}, steps,
			"the paused run did not name the steps that had produced outputs")
	}))
}

// TestAWalkThatEndsEarlyStillLetsTheCaseFinish is the failure mode this option
// exists to make impossible.
//
// A session that is merely abandoned holds its run forever, so a walk that
// returns after one step — because that is all it wanted, or because an
// assertion unwound it — would leave the case parked and the test hung. A hung
// test says far less than a failed one, and takes the whole package's timeout
// with it.
func TestAWalkThatEndsEarlyStillLetsTheCaseFinish(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), stagedWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: it ships
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, test, deploy]
`)

	// One step, then nothing. The case's own expectation — that all three
	// steps ran — is what proves the run was released rather than merely
	// unblocked: this subtest fails if the walk swallowed the rest of it.
	flowtesting.RunFile(t, path, flowtesting.WithWalk("it ships", func(walk *flowtesting.Walk) {
		_, ok := walk.Step()
		require.True(t, ok)
	}))
}

// TestWalkHandsBackTheWholeSession is the escape hatch, exercised so it is not
// merely declared: anything these methods do not cover is one call away, and
// the type never has to grow a method per command.
func TestWalkHandsBackTheWholeSession(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "workflow.yaml"), stagedWorkflow)
	path := write(t, filepath.Join(dir, "workflow.test.yaml"), `
tests:
  - name: it ships
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, test, deploy]
`)

	flowtesting.RunFile(t, path, flowtesting.WithWalk("it ships", func(walk *flowtesting.Walk) {
		session := walk.Session()
		require.NotNil(t, session)

		// A verb with no method here, reaching the same dispatch a person's
		// typing reaches.
		require.NoError(t, session.Control(t.Context(), "break deploy"))

		at, ok := walk.Continue()
		require.True(t, ok)
		assert.Equal(t, "deploy", at.Step,
			"a breakpoint set through the session did not hold the run")

		position, paused := session.Paused()
		require.True(t, paused)
		assert.Equal(t, "deploy", position.Step)
		assert.False(t, position.Autopsy)

		_ = flowdebug.ErrRunOver
	}))
}
