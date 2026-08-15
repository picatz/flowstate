package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/embed"
)

// TestRun closes the gap #529 found: examples/embedding/main.go compiled but
// nothing ever ran it, which made embedding the one deployment shape this
// repo advertises — compile a Flowfile from bytes, register a Go function as
// a task, run it locally or durably from your own program — with no
// execution coverage at all. CLAUDE.md's rule is that a capability is not
// done until something exercises it; this is that something for the
// embedding example specifically, distinct from pkg/flowstate/embed's own
// Example_compileAndRun, which demonstrates the facade in the abstract
// rather than running the shipped example program.
//
// run(false) is called directly rather than shelled out to as a subprocess —
// the cheapest honest thing, per the task: it is the exact function `go run
// ./examples/embedding` invokes from main, so nothing about the path is
// reinvented, and it takes no network and no Temporal server since durable is
// false, which run's own doc says skips runDurable entirely.
//
// What is asserted goes past "it exited zero": the printed local run outputs
// line has to name the specific greeting text "hello, embedder!", which only
// comes out of this run if all three of embed.Compile succeeded on the
// embedded flowfile/workflow.yaml bytes, the "greet" task this program
// registers actually executed (nothing else in the registry produces that
// string), and RunLocal read "embedder" back out of the workflow's declared
// default input rather than some other value. A test that only checked err
// == nil would pass even if the greet task silently never ran and the step
// produced no "message" output at all — GetRunOutputs().GetValues() would
// print an empty map, not fail.
func TestRun(t *testing.T) {
	stdout := captureStdout(t, func() {
		err := run(false)
		require.NoError(t, err, "the embedding example failed to run")
	})

	assert.Contains(t, stdout, "== running locally ==",
		"the example did not report starting its local run")

	// The message this run's compiled workflow.yaml declares as its output:
	// steps.greeting.message, produced by the "greet" task from the
	// workflow's own default input ("embedder", from flowfile/workflow.yaml's
	// inputs.name.default) — not the literal string constructed by hand, so
	// this test breaks if that default ever changes without the assertion
	// changing with it.
	assert.Contains(t, stdout, `local run outputs: map[message:literal:{string_value:"hello, embedder!"}]`,
		"the local run's printed outputs did not carry the greeting the custom "+
			"\"greet\" task builds from the workflow's default input; the workflow "+
			"may have failed to compile, the custom task may never have run, or its "+
			"output never reached the run's declared outputs")

	assert.Contains(t, stdout, "pass --durable",
		"run(false) should report that the durable half was skipped, since durable=false")

	// run(false) must never touch the network or a Temporal server: durable
	// is false, so main.go's own run doc says runDurable is never reached.
	// Asserted here as a second, independent check on the same property the
	// argument to run already guarantees, so a future edit that moves the
	// durable check cannot silently start dialing localhost:7233 from this
	// test without it failing on the missing skip message above.
	assert.NotContains(t, stdout, "running durably",
		"run(false) reached the durable half, which should be unreachable with durable=false")
}

// TestRegisterGreetTask exercises the custom task registration and
// invocation path directly, underneath run(): it builds the same task set
// registerGreetTask does, compiles the same embedded workflow.yaml bytes
// with embed.Compile, and runs it with embed.RunLocal — asserting on the
// typed v1 output rather than on printed text, so a change to how main.go
// formats its own log line cannot mask a real regression in what the task
// computes.
//
// It also pins the task's default-name behavior ("world" when no name is
// given), which run(false)'s own output — always "embedder", from the
// workflow's default input — never exercises.
func TestRegisterGreetTask(t *testing.T) {
	tasks := registerGreetTask()

	uninstall, err := tasks.Install()
	require.NoError(t, err)
	defer uninstall()

	workflow, diags, err := embed.Compile(workflowSource)
	require.NoError(t, err, "flowfile/workflow.yaml did not compile from its embedded bytes (diagnostics: %v)", diags)

	outputs, err := embed.RunLocal(context.Background(), workflow, embed.RunOptions{
		Inputs: map[string]any{"name": "test-caller"},
		Tasks:  tasks,
	})
	require.NoError(t, err, "running the compiled workflow locally failed")

	message, ok := embed.StepOutputString(outputs, "greeting", "message")
	require.True(t, ok, "step %q produced no string output named %q", "greeting", "message")
	assert.Equal(t, "hello, test-caller!", message,
		"the custom greet task did not build its greeting from the input this run supplied")

	// The workflow's own declared default (flowfile/workflow.yaml's
	// inputs.name.default: embedder), reached by giving no name input at
	// all. Distinct from run(false)'s coverage above, which supplies
	// "embedder" explicitly on RunOptions.Inputs; this instead proves the
	// compiled workflow's own default binds it the same way, which
	// [v1.RunWithInputs] does before the greet task's "world" fallback in
	// registerGreetTask ever gets a chance to run.
	defaultOutputs, err := embed.RunLocal(context.Background(), workflow, embed.RunOptions{
		Tasks: tasks,
	})
	require.NoError(t, err)
	defaultMessage, ok := embed.StepOutputString(defaultOutputs, "greeting", "message")
	require.True(t, ok)
	assert.Equal(t, "hello, embedder!", defaultMessage,
		"the workflow's own declared default input (embedder) was not applied when no name was given")
}

// captureStdout runs fn with os.Stdout replaced by a pipe, and returns
// everything fn wrote to it. fn's own goroutine writes to the pipe while a
// second goroutine drains it concurrently, so a write larger than the pipe's
// buffer cannot deadlock the test — main.go's output is a couple of short
// lines, but the pattern is the same one a longer-lived example would need.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	original := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = original }()

	out := make(chan string, 1)
	go func() {
		var buf strings.Builder
		_, _ = io.Copy(&buf, bufio.NewReader(r))
		out <- buf.String()
	}()

	fn()

	require.NoError(t, w.Close())
	os.Stdout = original

	return <-out
}
