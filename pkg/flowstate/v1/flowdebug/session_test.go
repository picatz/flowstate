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

// TestARefusedCommandSaysWhichBoundItHit (Codex, #1109).
//
// A [bufio.Scanner] that meets a line longer than its buffer stops exactly as
// it stops at end of input: Scan returns false, and only Err tells the two
// apart. The producer dropped that error, so a command over MaxCommandBytes
// reached the author as "no more commands — continuing to the end of the run"
// — a bound this package advertises, hit, and reported as the console having
// wandered off. The run finishing unattended is the right behaviour once the
// scanner is dead; saying nothing about why is not.
func TestARefusedCommandSaysWhichBoundItHit(t *testing.T) {
	var said strings.Builder

	session, err := flowdebug.New(flowdebug.Options{
		In:   strings.NewReader(strings.Repeat("y", flowdebug.MaxCommandBytes+1) + "\n"),
		Emit: func(text string, _ flowdebug.Tone) { said.WriteString(text) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	node := &v1.Node{Id: "held", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	require.NoError(t, session.BeforeStep(t.Context(), node, &v1.Scope{}))

	out := said.String()
	assert.Contains(t, out, "longer than the 65536 bytes one may be",
		"the bound that was hit has to be the one named")
	assert.Contains(t, out, "unattended",
		"and the consequence has to be stated, since the run is now running itself")
	assert.NotContains(t, out, "no more commands",
		"a refused command is not a console that ran out")
}

// TestAConsoleThatRanOutStillSaysSo is the other half: the ordinary end of a
// finite script is not an error, and must not be reported as one.
func TestAConsoleThatRanOutStillSaysSo(t *testing.T) {
	var said strings.Builder

	session, err := flowdebug.New(flowdebug.Options{
		In:   strings.NewReader(""),
		Emit: func(text string, _ flowdebug.Tone) { said.WriteString(text) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	node := &v1.Node{Id: "held", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	require.NoError(t, session.BeforeStep(t.Context(), node, &v1.Scope{}))

	assert.Contains(t, said.String(), "no more commands")
	assert.NotContains(t, said.String(), "could not be read")
}

// TestTheScriptIsBoundedByBytesAsWellAsCount (Codex, #1109): a hundred
// thousand commands each just under MaxCommandBytes satisfies both advertised
// bounds and is six gigabytes of retained recording. Neither bound implies the
// other, and the one nobody wrote down is the one an attacker picks.
//
// Driven at a smaller scale than the real bound — the claim is that the byte
// budget stops the recording and says so, not how many megabytes it takes.
func TestTheScriptIsBoundedByBytesAsWellAsCount(t *testing.T) {
	// Commands the session accepts and records, each large but well under the
	// per-command bound, and enough of them to pass MaxScriptBytes long before
	// MaxScriptCommands.
	const each = 32 << 10

	var script strings.Builder
	for range (flowdebug.MaxScriptBytes / each) + 8 {
		script.WriteString("inspect '" + strings.Repeat("y", each) + "'\n")
	}
	script.WriteString("continue\n")

	session, err := flowdebug.New(flowdebug.Options{
		In:   strings.NewReader(script.String()),
		Emit: func(string, flowdebug.Tone) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	node := &v1.Node{Id: "held", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	require.NoError(t, session.BeforeStep(t.Context(), node, &v1.Scope{}))

	recorded := 0
	for _, line := range session.Script() {
		recorded += len(line)
	}

	assert.LessOrEqual(t, recorded, flowdebug.MaxScriptBytes,
		"the recording kept more than its byte budget")
	assert.Less(t, len(session.Script()), flowdebug.MaxScriptCommands,
		"the count bound is nowhere near reached, which is the point: it does not imply this one")
	assert.True(t, session.ScriptTruncated(),
		"a partial replay script has to admit it is partial, or it reproduces a different run")
}

// TestACommandOfExactlyTheBoundIsAccepted: MaxCommandBytes is advertised as
// what one command may be, and every other bound on this surface accepts a
// command of exactly that size — the scanner's own buffer has to hold the line
// terminator too, so a buffer sized at the bound refused the bound (Codex,
// #1109).
func TestACommandOfExactlyTheBoundIsAccepted(t *testing.T) {
	var said strings.Builder

	// `inspect '<literal>'` sized so the whole line is exactly the bound.
	const wrapper = "inspect 'y'"

	command := "inspect '" + strings.Repeat("y", flowdebug.MaxCommandBytes-len(wrapper)+1) + "'"
	require.Len(t, command, flowdebug.MaxCommandBytes, "the fixture must sit exactly on the bound")

	session, err := flowdebug.New(flowdebug.Options{
		In:   strings.NewReader(command + "\ncontinue\n"),
		Emit: func(text string, _ flowdebug.Tone) { said.WriteString(text) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	node := &v1.Node{Id: "held", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	require.NoError(t, session.BeforeStep(t.Context(), node, &v1.Scope{}))

	assert.NotContains(t, said.String(), "longer than the",
		"a command of exactly the advertised bound was refused by the reader")
	assert.Equal(t, []string{"inspect " + command[len("inspect "):], "continue"}, session.Script(),
		"it should have been accepted, answered, and recorded like any other")
}

// A conditional breakpoint is what makes this debugger usable at the workload
// shape it exists for. `break` on a step inside a `for_each` stops on every
// iteration, and over MCP a script is bounded at a hundred commands — so
// without a condition an agent cannot reach iteration 5,000 of a large loop at
// all, and a human types `continue` until they give up.

// loopingRun runs a `for_each` over n items under a session fed by script,
// with one step in the body, and returns what the console saw.
//
// The binding is what the whole feature is about, so the fixture is a real
// loop rather than a scope built in Go: `runForEach` binds the iterator with
// WithLocal and hands *that* scope to the debugger, and a test constructing
// the scope directly would prove the condition evaluates and say nothing about
// whether the name it needs is there (CLAUDE.md).
func loopingRun(t *testing.T, n int, script string) (out string, ran []string) {
	t.Helper()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader(script),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	seen := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, seen))
	ctx = v1.NewContextWithDebugger(ctx, session)

	items := make([]any, 0, n)
	for i := range n {
		items = append(items, i)
	}

	looping := &v1.Workflow{Name: "looping", Steps: []*v1.Node{{
		Id: "each",
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList(items...),
			Iterator: "n",
			Body:     []*v1.Node{markStep("body")},
		}},
	}}}

	_, err = v1.Run(ctx, looping)
	require.NoError(t, err)

	return console.String(), seen.ids
}

// TestAConditionalBreakpointStopsAtTheIterationItNames is the traversal, not
// the step: twenty iterations, one stop, at the one the condition names.
func TestAConditionalBreakpointStopsAtTheIterationItNames(t *testing.T) {
	t.Parallel()

	// `continue` twice: once to leave the first-step stop, once to leave the
	// conditional stop and let the loop finish.
	out, ran := loopingRun(t, 20, "break body if n == 7\ncontinue\ncontinue\n")

	assert.Len(t, ran, 20, "every iteration still runs; a condition decides stopping, not running")
	assert.Equal(t, 1, strings.Count(out, "break at body"),
		"stopped once, at the iteration the condition named — not on all twenty")
	assert.Contains(t, out, "breakpoint at body if n == 7")
}

// TestAConditionThatIsNeverTrueNeverStops is the negative direction, and the
// one a test asserting only "it stopped" would miss.
func TestAConditionThatIsNeverTrueNeverStops(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 5, "break body if n == 99\ncontinue\n")

	assert.Len(t, ran, 5)
	assert.NotContains(t, out, "break at body",
		"a condition no iteration satisfies is a breakpoint that never fires")
}

// TestABreakpointConditionThatErrorsDoesNotHoldTheRun.
//
// This assertion is the reverse of the one it replaces, and the reversal is
// the point. The first design stopped on an errored condition, reading fail
// closed as "a debugger that cannot decide must not let the run past". Two
// findings showed that reading costs more than it buys: a condition is
// legitimately unanswerable at a same-named step in a sibling loop, so
// stopping parks the run where the author was not looking, on a workflow that
// is entirely legal.
//
// What made stopping seem necessary was the fear of a breakpoint that looks
// armed and never fires. That failure is the *silence*, not the not-stopping,
// and the notice removes it: an author sees which arrivals declined and why.
//
// `n.missing` parses and fails at evaluation, which is exactly the shape that
// cannot be caught when `break` accepts it.
func TestABreakpointConditionThatErrorsDoesNotHoldTheRun(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 3, "break body if n.missing\ncontinue\n")

	assert.Len(t, ran, 3)
	assert.NotContains(t, out, "break at body",
		"an unanswerable condition does not hold the run")
	assert.Contains(t, out, "could not be evaluated here",
		"but it says so, which is what keeps a never-firing breakpoint from being silent")
	assert.Equal(t, 1, strings.Count(out, "could not be evaluated here"),
		"once per breakpoint, not once per iteration")
}

// TestABreakpointWithAMalformedConditionIsRefusedWhenItIsTyped.
//
// Compiled when `break` accepts it, not at each arrival — the shape this
// repository uses for a rule (auth.SecretAccessPolicy.Compile, netpolicy's
// rule compiler) rather than the shape `inspect` uses for a question. A
// breakpoint accepted broken is worse than one refused: it reads as armed.
func TestABreakpointWithAMalformedConditionIsRefusedWhenItIsTyped(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 3, "break body if n ===\nbreakpoints\ncontinue\n")

	assert.Len(t, ran, 3)
	assert.Contains(t, out, "parse condition", "the refusal names what is wrong, at the moment it is typed")
	assert.NotContains(t, out, "break at body",
		"and nothing is set: a refused breakpoint must not half-exist")
	assert.Contains(t, out, "no breakpoints",
		"which `breakpoints` confirms rather than leaving to inference")
}

// TestABareIfIsRefusedRatherThanArmingEveryIteration (Codex, #1116).
//
// `break body if` with nothing after it used to install an *unconditional*
// breakpoint: an empty condition and an absent one were the same value, so the
// guard that compiles a condition simply skipped. The result is the worst
// reading of a typo — a stop on every iteration, which is exactly what typing
// a condition is meant to avoid, arrived at by a command that looked accepted.
func TestABareIfIsRefusedRatherThanArmingEveryIteration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, script string }{
		{name: "if with nothing after it", script: "break body if\nbreakpoints\ncontinue\n"},
		{name: "if with only spaces after it", script: "break body if   \nbreakpoints\ncontinue\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, ran := loopingRun(t, 4, tc.script)

			assert.Len(t, ran, 4)
			assert.Contains(t, out, "`if` needs an expression",
				"the refusal names what is missing")
			assert.Contains(t, out, "no breakpoints",
				"and nothing is set — an unconditional breakpoint here would stop on every iteration")
			assert.NotContains(t, out, "break at body",
				"which is the behaviour the typo used to produce")
		})
	}
}

// TestANearMissKeywordIsRefusedRatherThanDiscarded (Codex, #1116).
//
// `break body iff n == 7` used to install an unconditional breakpoint: a tail
// that was not `if` was discarded rather than rejected, so the condition the
// author wrote was silently not applied and the run stopped on every
// iteration. Same failure as a bare `if`, one typo over — which is why the
// rule is now that a tail is either nothing or a condition, rather than a
// list of the spellings noticed so far.
func TestANearMissKeywordIsRefusedRatherThanDiscarded(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, script string }{
		{name: "a misspelled keyword", script: "break body iff n == 7\nbreakpoints\ncontinue\n"},
		{name: "a condition with no keyword", script: "break body n == 7\nbreakpoints\ncontinue\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, ran := loopingRun(t, 4, tc.script)

			assert.Len(t, ran, 4)
			assert.Contains(t, out, "expected `if` after the step id",
				"the refusal names the grammar rather than only rejecting")
			assert.Contains(t, out, "no breakpoints",
				"and nothing is set, because an unconditional breakpoint here stops on every iteration")
			assert.NotContains(t, out, "break at body")
		})
	}
}

// TestAConditionThatCannotCompileIsRefusedWhenItIsTyped (Codex, #1116).
//
// Parsing is syntax only, so `1 + true` and `missing_function(n)` both parse.
// An accepted condition that cannot compile then fails at *every* arrival —
// and because an errored condition stops, that is a stop at every iteration:
// the exact behaviour a condition is typed to escape, reached by a typo the
// prompt reported as accepted.
//
// The check declares the names the expression mentions rather than the names
// in scope, which is what keeps a condition about a future binding legal. The
// last case is the one that pins it: `n` does not exist when `break` is typed,
// only once the loop body runs.
func TestAConditionThatCannotCompileIsRefusedWhenItIsTyped(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, condition, want string }{
		{name: "an operator over types that cannot combine", condition: "1 + true", want: "condition:"},
		{name: "a function nothing declares", condition: "missing_function(n)", want: "condition:"},
		{name: "an expression that is not a boolean", condition: "n + 1", want: "must be a boolean"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, ran := loopingRun(t, 4, "break body if "+tc.condition+"\nbreakpoints\ncontinue\n")

			assert.Len(t, ran, 4)
			assert.Contains(t, out, tc.want, "the refusal says what is wrong with the condition")
			assert.Contains(t, out, "no breakpoints", "and nothing is set")
			assert.NotContains(t, out, "break at body",
				"an accepted-but-uncompilable condition would stop at every iteration")
		})
	}

	// The other direction, and the reason the check declares what the
	// expression mentions rather than what is in scope: this is typed before
	// the loop has run, so `n` is bound nowhere yet, and it must still be
	// accepted and still fire at the iteration it names.
	t.Run("a condition about a binding that does not exist yet", func(t *testing.T) {
		t.Parallel()

		out, ran := loopingRun(t, 6, "break body if n == 3\ncontinue\ncontinue\n")

		assert.Len(t, ran, 6)
		assert.Contains(t, out, "breakpoint at body if n == 3", "accepted, though `n` is bound nowhere yet")
		assert.Equal(t, 1, strings.Count(out, "break at body"), "and it fires once, at the iteration it names")
	})
}

// TestAConditionMayCallTheProfilesNamespacedFunctions (Codex, #1116).
//
// The accept-time check declares every identifier the condition mentions as a
// dynamic variable, and the call target of `math.abs(n)` is the identifier
// `math` — so the check declares a variable with the same name as a namespace
// the profile provides. The concern is exact: a `math` variable could shadow
// the `math.` function namespace and make a valid condition unwritable.
//
// It does not, for two reasons worth pinning rather than trusting. cel-go
// resolves a qualified function name in preference to a same-named variable,
// so the check passes; and the declarations exist only for the *check* — the
// expression is evaluated through the profile's own environment, which has the
// libraries and none of these variables.
//
// Both halves are load-bearing and neither is visible from the code, so this
// walks every namespace the profile actually declares. A future change to the
// checking environment that broke either one would otherwise make profile
// functions silently unusable in a condition, which is a false diagnostic —
// the failure this repository ranks worst.
func TestAConditionMayCallTheProfilesNamespacedFunctions(t *testing.T) {
	t.Parallel()

	for _, condition := range []string{
		"math.abs(n - 3) == 0",
		"math.ceil(1.2) == 2.0 && n == 3",
		"lists.range(2).size() == 2 && n == 3",
		`json.encode(n) == "3"`,
		"cel.bind(doubled, n * 2, doubled == 6)",
	} {
		t.Run(condition, func(t *testing.T) {
			t.Parallel()

			out, ran := loopingRun(t, 6, "break body if "+condition+"\ncontinue\ncontinue\n")

			require.Len(t, ran, 6)
			assert.NotContains(t, out, "condition:",
				"a namespaced function the profile declares is not a type error")
			assert.NotContains(t, out, "cannot be trusted",
				"and it evaluates, rather than erroring at each arrival")
			assert.Equal(t, 1, strings.Count(out, "break at body"),
				"stopping once, at the iteration the condition picks out")
		})
	}
}

// TestAConditionFiresOnlyInTheDomainThatCanAnswerIt (Codex, #1116).
//
// A step id names a *visibility domain*, not a step: two sibling loops may
// each declare a body step called `page`, which `flowfile` has a whole test
// file about getting right elsewhere. Breakpoints are keyed by id, so
// `break page if total == 3` is a breakpoint at both — and in the first loop
// `total` is bound nowhere, so asking the condition there errors, and the
// stop-on-error rule then held the run in the loop the author was not
// debugging. Three stops where one was meant.
//
// A condition that cannot be evaluated at an occurrence does not hold the run
// there. That is not a question whose answer is no — it is not a question
// about that occurrence at all, and the notice says so rather than leaving the
// author to infer it.
func TestAConditionFiresOnlyInTheDomainThatCanAnswerIt(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("break page if total == 3\ncontinue\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	seen := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, seen))
	ctx = v1.NewContextWithDebugger(ctx, session)

	// Two sibling loops, each with a body step called `page`. Only the second
	// binds `total`, so only its iterations can answer the condition.
	siblings := &v1.Workflow{Name: "siblings", Steps: []*v1.Node{
		{Id: "first", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(1, 2), Iterator: "n",
			Body: []*v1.Node{markStep("page")},
		}}},
		{Id: "second", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(3, 4), Iterator: "total",
			Body: []*v1.Node{markStep("page")},
		}}},
	}}

	_, err = v1.Run(ctx, siblings)
	require.NoError(t, err)

	out := console.String()

	assert.Equal(t, 1, strings.Count(out, "break at page"),
		"once, in the loop that can answer the condition — not in the sibling that cannot")

	// And it is not silent about the occurrences it declined to ask at, so a
	// mistyped name still reports rather than looking like an answer of no.
	assert.Contains(t, out, "could not be evaluated here")
	assert.Equal(t, 1, strings.Count(out, "could not be evaluated here"),
		"once per breakpoint, not once per iteration — the case this exists for is a loop")
}

// TestAReceiverVariableIsStillARequiredName (Codex, #1116).
//
// `total.startsWith("3")` reads `total` from the scope; `math.abs(n)` does
// not read `math` from anywhere. The two are the same shape — an identifier in
// call-target position — and classifying by shape got the second right and the
// first wrong, which put the unbound-name guard back to sleep for exactly the
// conditions most likely to use it.
//
// The checker knows the difference, so it is asked instead: a reference
// carrying overloads is a function, one carrying only a name is a variable.
func TestAReceiverVariableIsStillARequiredName(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader(`break page if total.startsWith("3")` + "\ncontinue\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	seen := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, seen))
	ctx = v1.NewContextWithDebugger(ctx, session)

	siblings := &v1.Workflow{Name: "siblings", Steps: []*v1.Node{
		{Id: "first", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(1, 2), Iterator: "n",
			Body: []*v1.Node{markStep("page")},
		}}},
		{Id: "second", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList("3", "4"), Iterator: "total",
			Body: []*v1.Node{markStep("page")},
		}}},
	}}

	_, err = v1.Run(ctx, siblings)
	require.NoError(t, err)

	out := console.String()

	assert.Equal(t, 1, strings.Count(out, "break at page"),
		"a receiver has to resolve like any other name, so the first loop is declined rather than stopped in")
	assert.Contains(t, out, "could not be evaluated here")
}

// TestReplacingABreakpointRestoresItsNotice (Codex, #1116).
//
// The unbound notice prints once per breakpoint, which is what keeps a loop
// from printing it per iteration. A replacement is a different question and
// has to get its own chance to say it could not be asked — otherwise the
// second condition is skipped in silence after the prompt reported it set,
// which is the silent-never-fires failure the notice exists to prevent.
func TestReplacingABreakpointRestoresItsNotice(t *testing.T) {
	t.Parallel()

	// The first condition has to actually *fire* its notice before the
	// replacement, or the stale state this is about never exists. The run is
	// held before the first step, so: set the first condition, `step` into an
	// iteration where it is declined and the notice prints, replace it there,
	// then let the rest of the loop run.
	out, ran := loopingRun(t, 4,
		"break body if absent_one == 1\nstep\nbreak body if absent_two == 2\ncontinue\n")

	assert.Len(t, ran, 4)
	assert.Equal(t, 2, strings.Count(out, "could not be evaluated here"),
		"the first condition reports before it is replaced, and the replacement reports too — "+
			"one notice each rather than the first's carried over")
	assert.Contains(t, out, "absent_two",
		"and the replacement's own failure is what the second notice names")
}

// TestAMacroBindingDoesNotHideAnOuterNameOfTheSameSpelling (Codex, #1116).
//
// `n == 3 && [1].exists(n, n == 1)` binds an `n` inside the macro and reads a
// different `n` outside it. Excluding macro bindings by *name* dropped both,
// so the condition required nothing, the unbound guard was bypassed, and the
// sibling-loop stop this guard exists for came back — three stops, and not
// even a notice, because nothing was thought to be missing.
//
// Free-ness is a property of a reference, not of a spelling, so it is decided
// per expression node and joined to the checker's reference map by id.
func TestAMacroBindingDoesNotHideAnOuterNameOfTheSameSpelling(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("break page if n == 3 && [1].exists(n, n == 1)\ncontinue\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	seen := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, seen))
	ctx = v1.NewContextWithDebugger(ctx, session)

	// Only the second loop binds `n`. The first binds a differently-named
	// iterator, so the condition cannot be asked there.
	siblings := &v1.Workflow{Name: "siblings", Steps: []*v1.Node{
		{Id: "first", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList("a", "b"), Iterator: "other",
			Body: []*v1.Node{markStep("page")},
		}}},
		{Id: "second", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(3, 4), Iterator: "n",
			Body: []*v1.Node{markStep("page")},
		}}},
	}}

	_, err = v1.Run(ctx, siblings)
	require.NoError(t, err)

	out := console.String()

	assert.Equal(t, 1, strings.Count(out, "break at page"),
		"the outer `n` is a required name despite the macro binding one too")
	assert.Contains(t, out, "could not be evaluated here",
		"and the loop that cannot answer says so")
}

// TestATwoVariableMacroBindsBothOfItsVariables (Codex, #1116).
//
// `exists(i, v, …)` binds two names. Only the first was treated as bound, so
// `v` looked like a name the step had to provide — and since no step provides
// a macro's local, the condition was declined at every arrival and a true
// condition never fired. The quiet direction of the same guard whose loud
// direction was stopping in the wrong loop.
func TestATwoVariableMacroBindsBothOfItsVariables(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 6,
		"break body if n == 3 && [1, 2].exists(i, v, v == 2)\ncontinue\ncontinue\n")

	assert.Len(t, ran, 6)
	assert.Equal(t, 1, strings.Count(out, "break at body"),
		"the macro's second variable is the macro's, not a name the step must bind")
	assert.NotContains(t, out, "could not be evaluated here",
		"so nothing is declined for a name the expression provides itself")
}

// TestAConditionShortCircuitsThroughAnUnboundName (Codex, #1116).
//
// `n == 3 || fallback == 4` is true whenever `n` is 3, because CEL
// short-circuits and never looks at `fallback`. A preflight requiring every
// name a condition mentions declined it at every arrival, so a true condition
// never fired.
//
// This is half of what settled the design. Which references a condition
// actually needs depends on the *values*, so it is a question only the
// evaluator can answer, and any answer computed before evaluation is wrong in
// one direction or the other.
func TestAConditionShortCircuitsThroughAnUnboundName(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 6, "break body if n == 3 || fallback == 4\ncontinue\ncontinue\n")

	assert.Len(t, ran, 6)
	assert.Equal(t, 1, strings.Count(out, "break at body"),
		"the left side is true at n == 3, and CEL never reaches the unbound name — "+
			"which a preflight over both names made impossible")

	// The iterations where `n` is not 3 genuinely cannot answer: CEL has to
	// reach the right-hand side there, and `fallback` is bound nowhere. Those
	// decline, and say so once. Both halves are correct and only the evaluator
	// can tell them apart, which is the whole argument for asking it.
	assert.Equal(t, 1, strings.Count(out, "could not be evaluated here"),
		"the arrivals that had to reach the unbound name decline, once between them")
}

// TestAConditionOnAMemberIsDeclinedWhereTheMemberIsMissing (Codex, #1116).
//
// The other half. `steps.setup.ok` reads the root `steps`, which resolves in
// every scope — so a preflight over roots passed, and evaluation then failed
// on the member anyway, holding the run at the first `page` for a condition
// written about the second. Checking a root's binding never established that
// the selected output exists.
//
// Letting the evaluator decide covers roots and members with one rule, because
// it is the same rule: could this occurrence answer the question.
func TestAConditionOnAMemberIsDeclinedWhereTheMemberIsMissing(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("break page if steps.setup.ok == \"setup\"\ncontinue\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	seen := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, seen))
	ctx = v1.NewContextWithDebugger(ctx, session)

	// `setup` runs between the two loops, so its outputs exist for the second
	// loop's `page` and not for the first's.
	staged := &v1.Workflow{Name: "staged", Steps: []*v1.Node{
		{Id: "first", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(1, 2), Iterator: "n",
			Body: []*v1.Node{markStep("page")},
		}}},
		markStep("setup"),
		{Id: "second", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(3, 4), Iterator: "n",
			Body: []*v1.Node{markStep("page")},
		}}},
	}}

	_, err = v1.Run(ctx, staged)
	require.NoError(t, err)

	out := console.String()

	assert.Equal(t, 2, strings.Count(out, "break at page"),
		"both iterations of the second loop can answer; neither of the first's can")
	assert.Contains(t, out, "could not be evaluated here",
		"and the arrivals that could not answer say so")
}

// TestAConditionMayCompareRuntimeTypes (Codex, #1116).
//
// The accept-time check declares every identifier a condition mentions as a
// dynamic variable, and `type(n) == int` mentions `int` — a type constant CEL
// already provides. The concern is exact: redeclaring it could shadow the type
// and make a valid condition unwritable, which is a false diagnostic, the
// failure this repository ranks worst.
//
// It does not, and both halves are pinned because neither is visible from the
// code. Extending an environment with a variable named for a type constant is
// not a conflict, so the condition is accepted and fires; and the check is
// still doing its job alongside it, which is the half a test asserting only
// "it was accepted" would miss — an Extend failure is swallowed on purpose
// (blaming an author for this build's problem is worse than checking nothing),
// so a conflict would have turned the type check silently off rather than
// loudly wrong.
func TestAConditionMayCompareRuntimeTypes(t *testing.T) {
	t.Parallel()

	for _, condition := range []string{
		"type(n) == int",
		"type(n) == int && n == 3",
		`type("x") == string && n == 3`,
	} {
		t.Run(condition, func(t *testing.T) {
			t.Parallel()

			out, ran := loopingRun(t, 6, "break body if "+condition+"\ncontinue\ncontinue\n")

			require.Len(t, ran, 6)
			assert.NotContains(t, out, "condition:",
				"a type constant CEL provides is not a type error")
			assert.NotContains(t, out, "could not be evaluated here",
				"and it evaluates rather than declining at every arrival")
		})
	}

	// The half that matters more: the check is still effective. A type error
	// in the *same* condition as a type constant is still refused, so the
	// declaration cannot have quietly turned checking off.
	out, ran := loopingRun(t, 3, "break body if type(n) == int && (1 + true)\nbreakpoints\ncontinue\n")

	assert.Len(t, ran, 3)
	assert.Contains(t, out, "no matching overload",
		"the type check still runs alongside the type constant")
	assert.Contains(t, out, "no breakpoints", "and nothing is set")
}

// TestCompleteIsTheTabKeyMadeIntoACommand.
//
// The completion this package builds was reachable only through a terminal's
// tab key: `SetCompleter` is called on a console, and a console exists only
// where both streams are terminals. So a scripted session — which is the whole
// shape of the MCP front — could not ask what may be written, though the
// answer is a pure function of a scope the session already holds.
//
// That is the same capability-on-one-surface gap this package has now been
// given a second front for twice, and the fix is the same: a question that
// belongs in the command stream goes in the command stream, beside `inspect`.
func TestCompleteIsTheTabKeyMadeIntoACommand(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "step\ncomplete inspect steps.\ncomplete break \ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	// Scoped to the answer's own lines rather than the transcript, because the
	// run goes on to print `deploy` in its account a moment later — asserting
	// over the whole output would be asking about the wrong thing.
	offered := completionLines(out, "a step that has run")

	assert.Contains(t, offered, "build",
		"the paused run's own outputs, which is what makes this worth asking")
	assert.NotContains(t, offered, "deploy",
		"and not the step that has not run yet, which is the difference from completing over a file")

	assert.Contains(t, out, "a step this run may reach",
		"`break ` completes over the step inventory, exactly as a tab press does")
}

// TestCompleteIsDiscoverable.
//
// A verb `dispatch` understands but the table does not list is a verb nobody
// finds: `help` renders from the table, and so does the completion of verbs
// themselves. Removing the entry leaves the command working and undiscoverable
// — which is the same capability-you-cannot-reach failure this verb was added
// to fix, one level up.
func TestCompleteIsDiscoverable(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "help\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "complete <partial-command>",
		"`help` renders from the table, so a verb missing from it is one nobody is told about")

	// And the verb completer offers it, which is how somebody at a prompt
	// finds it without reading help at all.
	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"step", "continue"},
		[][]string{nil, {"comp"}})

	require.Len(t, console.answers, 1)
	assert.Equal(t, []string{"complete "}, texts(console.answers[0]),
		"offered with the space that separates it from its argument, like every other verb that takes one")
}

// completionLines are the offered names on the lines carrying detail, joined.
func completionLines(out, detail string) string {
	var names []string
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, detail) {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimPrefix(strings.TrimSpace(line), "debug> "), " ")
		names = append(names, name)
	}

	return strings.Join(names, " ")
}

// TestCompleteSaysSoWhenThereIsNothingToOffer.
//
// An empty answer and a cut list are different answers, and a script cannot
// see a blank line the way a person sees an empty popup.
func TestCompleteSaysSoWhenThereIsNothingToOffer(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "step\ncomplete inspect zzz_nothing_starts_with_this\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "nothing to complete there",
		"said out loud, because a script reading a transcript cannot see an empty popup")
}
