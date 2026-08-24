package flowdebug_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

	session.Autopsy(t.Context(), scope, []string{`expect.check[0]: check failed: steps.build.ok == 'shipped'`})

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
			session.Autopsy(t.Context(), &v1.Scope{}, nil)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("autopsy did not leave on %q", strings.TrimSpace(verb))
		}
	}
}
