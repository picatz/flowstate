package flowdebug_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A session is driven by a stream of commands, so every test here is a real
// local run stepped by a scripted console — which is also the replay path
// (#928's record-and-replay), not a stand-in for it.

// ranSteps records which steps actually executed, so a test can tell "the
// debugger printed something about a step" from "the step ran".
type ranSteps struct{ ids []string }

func debugRegistry(t *testing.T, ran *ranSteps) *v1.Registry {
	t.Helper()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "mark", Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		id := inputs["id"].GetLiteral().GetStringValue()
		ran.ids = append(ran.ids, id)

		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"ok": v1.NewLiteral(id)}}, nil
	}}))

	return registry
}

func markStep(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "mark",
		Inputs: map[string]*v1.Value{"id": v1.NewLiteral(id)},
	}}}
}

// runDebugged runs a three-step workflow under a session fed by script, and
// returns everything the console saw.
func runDebugged(t *testing.T, script string, opts flowdebug.Options) (out string, ran []string, runErr error) {
	t.Helper()

	var console strings.Builder
	opts.In = strings.NewReader(script)
	opts.Out = &console

	session, err := flowdebug.New(opts)
	require.NoError(t, err)

	ran2 := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, ran2))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	workflow := &v1.Workflow{Name: "debugged", Steps: []*v1.Node{
		markStep("build"), markStep("test"), markStep("deploy"),
	}}

	_, runErr = v1.Run(ctx, workflow)

	return console.String(), ran2.ids, runErr
}

// TestASessionStopsAtTheFirstStepAndStepsThrough: with no breakpoints named,
// `--debug` means "let me look before anything happens", and `step` walks.
func TestASessionStopsAtTheFirstStepAndStepsThrough(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "step\nstep\nstep\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "test", "deploy"}, ran)
	assert.Contains(t, out, `break at build (task "mark")`)
	assert.Contains(t, out, `break at test (task "mark")`)
	assert.Contains(t, out, `break at deploy (task "mark")`)
	assert.Contains(t, out, `build -> ok: "build"`, "the observer's account is what a step prints")
}

// TestAnEmptyLineStepsOnce: the key a person presses most.
func TestAnEmptyLineStepsOnce(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "\n\n\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "test", "deploy"}, ran)
	assert.Contains(t, out, `break at deploy`)
}

// TestContinueRunsToTheNextBreakpoint: and past the steps between, which is
// the difference between `continue` and `step`.
func TestContinueRunsToTheNextBreakpoint(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "continue\ncontinue\n", flowdebug.Options{Breakpoints: []string{"deploy"}})
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "test", "deploy"}, ran)
	assert.NotContains(t, out, "break at build", "a named breakpoint means run to it, not walk to it")
	assert.NotContains(t, out, "break at test")
	assert.Contains(t, out, "break at deploy")
}

// TestUntilRunsToTheStepItNames: the steps between are run, not walked, and
// the run stops before the named one rather than after it.
func TestUntilRunsToTheStepItNames(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "until deploy\nquit\n", flowdebug.Options{})
	require.ErrorContains(t, err, "debug session ended")

	assert.Equal(t, []string{"build", "test"}, ran, "deploy is where it stopped, so deploy did not run")
	assert.Contains(t, out, "break at deploy")
	assert.NotContains(t, out, "break at test")
}

// TestQuitEndsTheRunWhereItStands: the refusal reaches the caller, and the
// steps behind it do not run.
func TestQuitEndsTheRunWhereItStands(t *testing.T) {
	t.Parallel()

	_, ran, err := runDebugged(t, "step\nquit\n", flowdebug.Options{})
	require.ErrorContains(t, err, "debug session ended")

	assert.Equal(t, []string{"build"}, ran)
}

// TestAnExhaustedConsoleResumesTheRun is #928's own answer to its question 4,
// applied locally: a run held by a debugger that is not there is worse than a
// run that finished unattended. It says so rather than doing it quietly.
func TestAnExhaustedConsoleResumesTheRun(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "step\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "test", "deploy"}, ran)
	assert.Contains(t, out, "no more commands")
}

// TestNoConsoleAtAllNeverHolds: a session with no reader is harmless rather
// than hung, which is what makes it safe to install one by default.
func TestNoConsoleAtAllNeverHolds(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Out: &console})
	require.NoError(t, err)

	ran := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, ran))
	ctx = v1.NewContextWithDebugger(ctx, session)

	_, err = v1.Run(ctx, &v1.Workflow{Name: "w", Steps: []*v1.Node{markStep("only")}})
	require.NoError(t, err)

	assert.Equal(t, []string{"only"}, ran.ids)
}

// TestInspectEvaluatesAgainstTheRunsOwnScope: the whole point of stopping.
func TestInspectEvaluatesAgainstTheRunsOwnScope(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "step\ninspect steps.build.ok\ninspect 1 + 1\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, `"build"`, "an inspection reads what the run has produced")
	assert.Contains(t, out, "2")
}

// TestAFailedInspectionIsNotAFailedSession: an author at a prompt is asking
// questions, and some will not compile. Answering and asking again is the
// only reasonable reading.
func TestAFailedInspectionIsNotAFailedSession(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "inspect steps.nope.missing\ninspect ((\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "test", "deploy"}, ran, "the run still finished")
	assert.Contains(t, out, "debug>", "and the session kept asking")
}

// TestAnUnknownCommandIsNamed, never ignored — the same diagnostics rule this
// repo applies to a misspelled key in a file.
func TestAnUnknownCommandIsNamed(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "setp\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, `unknown command "setp"`)
}

// TestScopeListsWhatTheRunCanName: the question asked before an author knows
// what to inspect.
func TestScopeListsWhatTheRunCanName(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "step\nscope\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "steps: build")
}

// TestBreakpointsAreAddedAndRemovedMidSession.
func TestBreakpointsAreAddedAndRemovedMidSession(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "break deploy\nbreakpoints\ncontinue\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "breakpoint at deploy")
	assert.Contains(t, out, "breakpoints: deploy")
	assert.Contains(t, out, "break at deploy")
	assert.Equal(t, []string{"build", "test", "deploy"}, ran)
}

// TestTheSessionRecordsAReplayableScript is the record half of #928's
// record-and-replay: what the session accepted, in order, ready to be fed
// back. Mistyped commands are not in it — a replay reproduces decisions, not
// typing.
func TestTheSessionRecordsAReplayableScript(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("setp\nstep\ninspect 1 + 1\nuntil deploy\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)

	ran := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, ran))
	ctx = v1.NewContextWithDebugger(ctx, session)

	_, err = v1.Run(ctx, &v1.Workflow{Name: "w", Steps: []*v1.Node{
		markStep("build"), markStep("test"), markStep("deploy"),
	}})
	require.NoError(t, err)

	assert.Equal(t, []string{"step", "inspect 1 + 1", "until deploy", "continue"}, session.Script())
	assert.False(t, session.ScriptTruncated())
}

// TestAReplayedScriptReproducesTheSession is the other half, and the claim
// that makes a session a test artifact: the recorded script, fed to a fresh
// session, makes the same decisions and produces the same console.
func TestAReplayedScriptReproducesTheSession(t *testing.T) {
	t.Parallel()

	first, firstRan, err := runDebugged(t, "step\ninspect steps.build.ok\nuntil deploy\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	var recorded *flowdebug.Session
	{
		var console strings.Builder
		session, newErr := flowdebug.New(flowdebug.Options{
			In:  strings.NewReader("step\ninspect steps.build.ok\nuntil deploy\ncontinue\n"),
			Out: &console,
		})
		require.NoError(t, newErr)

		ran := &ranSteps{}
		ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, ran))
		ctx = v1.NewContextWithDebugger(ctx, session)
		ctx = v1.NewContextWithRunObserver(ctx, session)
		_, err = v1.Run(ctx, &v1.Workflow{Name: "debugged", Steps: []*v1.Node{
			markStep("build"), markStep("test"), markStep("deploy"),
		}})
		require.NoError(t, err)
		recorded = session
	}

	replayed, replayedRan, err := runDebugged(t, strings.Join(recorded.Script(), "\n")+"\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Equal(t, firstRan, replayedRan, "the replay runs the same steps")
	assert.Equal(t, first, replayed, "and says the same things in the same order")
}

// TestASecretReferenceIsNotResolvedByAnInspection: #928 commits to
// "inspection never resolves a secret", and this pins that the commitment is
// inherited rather than enforced here. An activation refuses a
// [v1.Value_SecretRef] outright — resolving one "would produce a value in
// workflow code, and anything a workflow computes can end up in history"
// (eval.go, StepsOutputActivation.resolveValue) — so an inspection that names
// the reference does not get a refusal message from the debugger, it gets an
// attribute that does not resolve at all. There is nothing there to read.
//
// The value is really in the environment where the env provider would find
// it, so the negative assertion has something it could catch.
func TestASecretReferenceIsNotResolvedByAnInspection(t *testing.T) {
	t.Setenv("FLOWSTATE_SECRET_API_KEY", "super-secret-value")

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("step\ninspect steps.held.token\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "hold", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"token": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "API_KEY"}}},
		}}, nil
	}}))

	ctx := v1.NewContextWithRegistry(t.Context(), registry)
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, err = v1.Run(ctx, &v1.Workflow{Name: "w", Steps: []*v1.Node{
		{Id: "held", Kind: &v1.Node_Task{Task: &v1.Task{Name: "hold"}}},
		markStepless("after"),
	}})
	require.NoError(t, err)

	out := console.String()
	assert.NotContains(t, out, "super-secret-value", "no surface of a debug session may print a resolved secret")
	assert.Contains(t, out, "steps.held.token",
		"the inspection is answered by the attribute not resolving, which names it")
	assert.Contains(t, out, "secret(env://API_KEY)",
		"and the account renders the reference as the reference it is")
}

// markStepless is a value step, so the secret test needs no second task.
func markStepless(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
}

// tonedFragment is one Emit callback, for the tests below.
type tonedFragment struct {
	text string
	tone flowdebug.Tone
}

// TestEmitClassifiesTheSessionsOutput drives a run holding each kind of
// outcome and asserts every fragment arrives through Emit with the tone its
// meaning calls for — and that Out is left alone once Emit is installed,
// because two output paths would let a terminal and a capture disagree about
// what the session said.
func TestEmitClassifiesTheSessionsOutput(t *testing.T) {
	t.Parallel()

	var fragments []tonedFragment
	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("step\nsetp\ninspect ((\ncontinue\n"),
		Out: &out,
		Emit: func(text string, tone flowdebug.Tone) {
			fragments = append(fragments, tonedFragment{text: text, tone: tone})
		},
	})
	require.NoError(t, err)

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "boom", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return nil, errors.New("deliberate failure")
	}}))

	ctx := v1.NewContextWithRegistry(t.Context(), registry)
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	workflow := &v1.Workflow{Name: "toned", Steps: []*v1.Node{
		{Id: "shrugged", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
			Policy: &v1.StepPolicy{ContinueOnError: true, Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
		{Id: "fatal", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
	}}

	_, err = v1.Run(ctx, workflow)
	require.Error(t, err, "the second step fails the run")

	assert.Empty(t, out.String(), "Emit replaces Out; one session, one output path")

	toneOf := func(substr string) (flowdebug.Tone, bool) {
		for _, f := range fragments {
			if strings.Contains(f.text, substr) {
				return f.tone, true
			}
		}
		return 0, false
	}
	assertTone := func(substr string, want flowdebug.Tone) {
		t.Helper()
		tone, found := toneOf(substr)
		require.True(t, found, "no fragment contains %q", substr)
		assert.Equal(t, want, tone, "the fragment containing %q", substr)
	}

	assertTone("break at shrugged", flowdebug.ToneBreak)
	assertTone("debug> ", flowdebug.TonePrompt)
	assertTone("tolerated by continue_on_error", flowdebug.ToneWarning)
	assertTone(`unknown command "setp"`, flowdebug.ToneWarning)
	assertTone("expression", flowdebug.ToneWarning) // the failed inspection
	assertTone("FAILED", flowdebug.ToneDanger)
}

// TestNoEmitKeepsThePlainWriter: the zero-configuration session writes plain
// text to Out exactly as before Emit existed — the compatibility half.
func TestNoEmitKeepsThePlainWriter(t *testing.T) {
	t.Parallel()

	out, ran, err := runDebugged(t, "step\nstep\nstep\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Equal(t, []string{"build", "test", "deploy"}, ran)
	assert.Contains(t, out, "break at build")
	assert.NotContains(t, out, "\x1b[", "no escape sequence reaches a plain session")
}

// The autopsy (#1072): a failing case's session stops once more after the
// verdict, failures printed, scope still questionable.

// TestTheAutopsyAnswersFromTheFinishedRun: inspect and scope work over the
// corpse, the failures print as failures, and quit leaves.
func TestTheAutopsyAnswersFromTheFinishedRun(t *testing.T) {
	t.Parallel()

	var fragments []tonedFragment
	session, err := flowdebug.New(flowdebug.Options{
		In: strings.NewReader("inspect steps.build.ok\nscope\nquit\n"),
		Emit: func(text string, tone flowdebug.Tone) {
			fragments = append(fragments, tonedFragment{text: text, tone: tone})
		},
	})
	require.NoError(t, err)

	scope := &v1.Scope{Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"build": {NamedValues: map[string]*v1.Value{"ok": v1.NewLiteral("built")}},
	}}}

	session.Autopsy(t.Context(), scope, nil, []string{`expect.check[0]: check failed: steps.build.ok == 'shipped'`})

	joined := ""
	for _, f := range fragments {
		joined += f.text
	}
	assert.Contains(t, joined, "autopsy: the case failed 1 expectation(s)")
	assert.Contains(t, joined, `"built"`, "an inspection reads the finished run")
	assert.Contains(t, joined, "steps: build")

	failureTone := flowdebug.ToneInfo
	for _, f := range fragments {
		if strings.Contains(f.text, "check failed") {
			failureTone = f.tone
		}
	}
	assert.Equal(t, flowdebug.ToneDanger, failureTone, "a failure prints as one")

	assert.Equal(t, []string{"inspect steps.build.ok", "scope", "quit"}, session.Script(),
		"autopsy commands join the replay script")
}

// TestTheAutopsyMovementVerbsAllLeave: there is no run left to move, so every
// movement verb is a departure — and an exhausted console leaves too.
func TestTheAutopsyMovementVerbsAllLeave(t *testing.T) {
	t.Parallel()

	for _, verb := range []string{"step\n", "continue\n", "until deploy\n", "quit\n", ""} {
		var out strings.Builder
		session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader(verb), Out: &out})
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			session.Autopsy(t.Context(), &v1.Scope{}, nil, nil)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("autopsy did not leave on %q", strings.TrimSpace(verb))
		}
	}
}

// TestQuitAtABreakpointSuppressesTheAutopsy: quit is the one command
// advertised as leaving, so a session that quit mid-run is never re-opened
// for the autopsy — abandoning the run fails the case, and answering `quit`
// with another prompt would make it a lie (Codex, #1107).
func TestQuitAtABreakpointSuppressesTheAutopsy(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("quit\n"), Out: &console})
	require.NoError(t, err)

	ran := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, ran))
	ctx = v1.NewContextWithDebugger(ctx, session)

	workflow := &v1.Workflow{Name: "debugged", Steps: []*v1.Node{markStep("build")}}
	_, runErr := v1.Run(ctx, workflow)
	require.Error(t, runErr, "quit abandons the run")
	assert.Empty(t, ran.ids, "quit at the first breakpoint runs nothing")

	before := console.String()
	session.Autopsy(t.Context(), &v1.Scope{}, nil, []string{"expect.ran: build never ran"})
	assert.Equal(t, before, console.String(), "a session that quit is not prompted again")
	assert.NotContains(t, console.String(), "autopsy:")
}

// TestTheAutopsyAnswersWithTheChecksOwnBindings: an inspection at the autopsy
// binds what the failing check was judged under — the file's `vars` and the
// extended `run` root ride in through extra — so `inspect vars.want` asks the
// same question the check asked, not one over a scope missing half its names
// (Codex, #1107).
func TestTheAutopsyAnswersWithTheChecksOwnBindings(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("inspect vars.want\ninspect run.failed\ninspect run.error\nquit\n"),
		Out: &console,
	})
	require.NoError(t, err)

	extra := map[string]ref.Val{
		"vars": v1.TypeAdapter.NativeToValue(map[string]any{"want": "shipped"}),
		"run":  v1.TypeAdapter.NativeToValue(map[string]any{"failed": true, "error": "step build: exploded"}),
	}
	session.Autopsy(t.Context(), &v1.Scope{}, extra,
		[]string{"expect.check[0]: check failed: steps.build.ok == vars.want"})

	out := console.String()
	assert.Contains(t, out, `"shipped"`, "vars answers as the check read it")
	assert.Contains(t, out, "true", "run.failed answers")
	assert.Contains(t, out, "exploded", "run.error answers")
}

// TestTheAutopsyScopeListsItsBindings: `scope` is how an author discovers
// what to inspect, so the autopsy's extra bindings must appear in it — a
// listing that omits `vars` while `inspect vars.x` answers hides exactly the
// names it exists to reveal (Codex, #1109).
func TestTheAutopsyScopeListsItsBindings(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("scope\nquit\n"),
		Out: &console,
	})
	require.NoError(t, err)

	extra := map[string]ref.Val{
		"vars": v1.TypeAdapter.NativeToValue(map[string]any{"want": "shipped"}),
		"run":  v1.TypeAdapter.NativeToValue(map[string]any{"failed": true}),
	}
	session.Autopsy(t.Context(), &v1.Scope{}, extra, []string{"expect.check[0]: check failed"})

	assert.Contains(t, console.String(), "bound: run, vars",
		"the autopsy's own bindings are missing from the scope listing")
}

// TestACancelledContextUnblocksThePrompt: ctrl-C's first signal cancels the
// command's context, and a session parked at a prompt has to notice — a
// synchronous read would hold the process hostage for a second, harder
// signal (Codex, #1109). The console here never writes, which is exactly the
// terminal a person just interrupted.
func TestACancelledContextUnblocksThePrompt(t *testing.T) {
	t.Parallel()

	blocked, _ := io.Pipe()
	session, err := flowdebug.New(flowdebug.Options{In: blocked})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- session.BeforeStep(ctx, markStep("build"), &v1.Scope{})
	}()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled,
			"the engine must see the cancellation, not a resume or a hang")
	case <-time.After(5 * time.Second):
		t.Fatal("the prompt did not notice the cancelled context")
	}
}

// TestCloseReleasesTheReaderGoroutine (Codex, #1109): a session's reader parks
// on an unbuffered send whenever a line arrives with nobody left to take it,
// which is every session whose run ended with a command still in flight. In a
// process about to exit that is free; in a server answering debug calls it is
// a goroutine, a scanner and a script retained per call, without bound.
//
// Counted rather than asserted about in prose, because "does not leak" is
// exactly the claim a test can make vacuously.
func TestCloseReleasesTheReaderGoroutine(t *testing.T) {
	// Not parallel: it counts goroutines, and a sibling test starting one
	// concurrently would be indistinguishable from a leak.
	before := runtime.NumGoroutine()

	for range 20 {
		// `step` resumes the run, so BeforeStep returns with the reader
		// holding the *next* line and no receiver left for it — which is the
		// shape that parks. A script of non-resuming commands would drain to
		// EOF and exit on its own, proving nothing; that is how this test was
		// vacuous when first written.
		session, err := flowdebug.New(flowdebug.Options{
			In:  strings.NewReader("step\n" + strings.Repeat("scope\n", 50)),
			Out: io.Discard,
		})
		require.NoError(t, err)

		// One read, then abandon it exactly as a finished call does.
		require.NoError(t, session.BeforeStep(t.Context(), markStep("only"), &v1.Scope{}))
		require.NoError(t, session.Close())
	}

	// The readers exit asynchronously; give them a moment rather than racing.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	assert.LessOrEqual(t, runtime.NumGoroutine(), before+2,
		"twenty closed sessions left readers parked: each holds its scanner and script "+
			"for the life of the process")
}

// TestCloseIsIdempotentAndSafeWithoutAReader: a caller closing twice, or
// closing a session whose reader never started, must not panic — a session is
// closed by whoever owns it, and that owner does not know which of those
// happened.
func TestCloseIsIdempotentAndSafeWithoutAReader(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Out: io.Discard})
	require.NoError(t, err)

	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
}

// TestConcurrentCallbacksAreSerialized holds this type's own doc to account
// (Codex, #1109).
//
// [v1.RunObserver] states plainly that its callbacks arrive on the step's
// goroutine and that an implementation storing events must synchronize itself
// where a workflow has `parallel:` branches or `async:` steps; the Session doc
// answers "safe for concurrent use". It was not: Emit was called with nothing
// held, and the two emitters this repository ships both accumulate — the MCP
// adapter appends to a slice and adds up the bytes its answer's bound is
// computed from.
//
// Driven directly rather than through a workflow, deliberately. The local
// driver is sequential today, so no Flowfile produces these callbacks — a test
// written as a run would pass against the unsynchronized version and prove
// nothing. What is under test is the promise, which is made here.
func TestConcurrentCallbacksAreSerialized(t *testing.T) {
	const (
		writers = 8
		each    = 50
	)

	var (
		kept  []string
		bytes int
	)

	// An emitter that accumulates, which is the shape both real ones have,
	// and with no lock of its own: keeping two writers out of each other is
	// the session's job, and this is the callback that finds out whether it
	// did. wg.Wait below is the happens-before edge that makes reading these
	// afterwards safe.
	session, err := flowdebug.New(flowdebug.Options{
		In: strings.NewReader(""),
		Emit: func(text string, _ flowdebug.Tone) {
			kept = append(kept, text)
			bytes += len(text)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	var wg sync.WaitGroup

	for writer := range writers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range each {
				session.StepFinished(fmt.Sprintf("step-%d-%d", writer, i), nil, nil, false)
			}
		}()
	}
	wg.Wait()

	// Every fragment arrived, and the running total agrees with them. A lost
	// append and a double-counted length are the two ways the unsynchronized
	// version was wrong, and each of them is a bound computed from a number
	// nobody wrote down.
	require.Len(t, kept, writers*each, "an account was lost between two goroutines")

	total := 0
	for _, text := range kept {
		total += len(text)
	}
	require.Equal(t, total, bytes, "the running total disagrees with what was kept")
}

// TestConcurrentBoundariesAdmitOneAtATime: two goroutines reaching a step
// boundary at once must not both prompt.
//
// One session holds one run and one command stream answers one prompt, so two
// boundaries prompting together would split a script between two run
// positions — each acting on a line meant for the other, and each `inspect`
// answering from whichever branch's scope happened to win the read. Nothing
// about that is a data race; it is a session that is simply somewhere else.
//
// Made deterministic with a pipe rather than a canned script: the first
// boundary is held at its prompt with nothing written yet, the second is
// started underneath it, and the claim is that no second prompt appears until
// the first has been answered. The wait is a real interval because the wrong
// behaviour is fast — a second boundary that is admitted reaches its prompt on
// pure CPU — and it fails in the safe direction: a machine too loaded to get
// there in time makes this test prove less, never fail wrongly.
func TestConcurrentBoundariesAdmitOneAtATime(t *testing.T) {
	prompts := make(chan struct{}, 8)

	console, commands := io.Pipe()
	t.Cleanup(func() { _ = commands.Close() })

	session, err := flowdebug.New(flowdebug.Options{
		In: console,
		Emit: func(_ string, tone flowdebug.Tone) {
			if tone == flowdebug.TonePrompt {
				prompts <- struct{}{}
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	node := &v1.Node{Id: "concurrent", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	scope := &v1.Scope{}

	first := make(chan error, 1)
	go func() { first <- session.BeforeStep(t.Context(), node, scope) }()

	select {
	case <-prompts:
	case <-time.After(5 * time.Second):
		t.Fatal("the first boundary never reached a prompt")
	}

	second := make(chan error, 1)
	go func() { second <- session.BeforeStep(t.Context(), node, scope) }()

	select {
	case <-prompts:
		t.Fatal("a second boundary prompted while the first still held the session, " +
			"so one script was being read by two run positions")
	case <-time.After(250 * time.Millisecond):
	}

	_, err = commands.Write([]byte("step\n"))
	require.NoError(t, err)
	require.NoError(t, <-first)

	// And now it is the second boundary's turn, which is the other half of the
	// claim: admitted one at a time, not shut out.
	select {
	case <-prompts:
	case <-time.After(5 * time.Second):
		t.Fatal("the second boundary never got the session")
	}

	_, err = commands.Write([]byte("step\n"))
	require.NoError(t, err)
	require.NoError(t, <-second)

	require.Equal(t, []string{"step", "step"}, session.Script(),
		"each boundary consumed exactly one line")
}
