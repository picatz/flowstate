package flowdebug_test

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Completion is asked *at a breakpoint*, over the scope the run is actually
// held in, so every test here drives a real local run and asks its questions
// from inside a [flowdebug.Console] — which is where a terminal would ask them.
// Building a scope in Go and completing over it would prove the completer works
// and say nothing about whether a paused run reaches it (CLAUDE.md).

// asking is a console that answers each prompt with the next scripted line,
// after asking the session to complete whatever the script says to complete at
// that stop.
type asking struct {
	session *flowdebug.Session

	// steps are the lines to answer prompts with, in order.
	steps []string

	// ask is what to complete at each stop, indexed as steps is: a list per
	// prompt, because the questions have to be asked *while the run is held*
	// and one stop is usually where several of them belong.
	ask [][]string

	// answers are what came back, in the order they were asked.
	answers []flowdebug.Completion
}

// Prompt implements [flowdebug.Console].
func (a *asking) Prompt() (string, error) {
	if len(a.steps) == 0 {
		return "", io.EOF
	}

	for _, question := range a.ask[0] {
		a.answers = append(a.answers, a.session.Complete(question, len(question)))
	}
	line := a.steps[0]
	a.steps, a.ask = a.steps[1:], a.ask[1:]

	return line, nil
}

// texts is what an answer offers, in order.
func texts(answer flowdebug.Completion) []string {
	out := make([]string, 0, len(answer.Candidates))
	for _, candidate := range answer.Candidates {
		out = append(out, candidate.Text)
	}

	return out
}

// completingWorkflow is the run every test here debugs: three steps, a
// workflow-level var, and a declared input, so that all three roots and a
// binding are live at once.
func completingWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:           "completed",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "version", Type: v1.InputDeclaration_TYPE_STRING, Default: v1.NewLiteral("1.2.3")}},
		Vars:           map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
		Steps: []*v1.Node{
			markStep("build"),
			markStep("test"),
			markStep("deploy"),
		},
	}
}

// completingRun runs that workflow under a session whose console asks the
// questions in ask and answers with the lines in steps.
func completingRun(t *testing.T, opts flowdebug.Options, steps []string, ask [][]string) (*asking, string) {
	t.Helper()

	require.Len(t, ask, len(steps), "one question slot per prompt")

	var out strings.Builder
	console := &asking{steps: steps, ask: ask}
	opts.Console = console
	opts.Out = &out

	session, err := flowdebug.New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, err = v1.RunWithInputs(ctx, completingWorkflow(), map[string]*v1.Value{"version": v1.NewLiteral("1.2.3")})
	require.NoError(t, err)

	return console, out.String()
}

// TestCompletionReachesThePausedRunsOwnOutputs is the headline claim, and the
// reason this exists rather than pointing the editor's completer at a file: at
// the third step, `steps.` offers the two that have run and not the one that
// has not, and `steps.build.` offers the output that step actually produced.
func TestCompletionReachesThePausedRunsOwnOutputs(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"step", "step", "continue"},
		[][]string{nil, nil, {"inspect steps.", "inspect steps.build."}})

	require.Len(t, console.answers, 2)

	assert.Equal(t, []string{"build", "test"}, texts(console.answers[0]),
		"the steps that have run, and not `deploy`, which the scope cannot name yet")
	assert.Equal(t, []string{"ok"}, texts(console.answers[1]),
		"and the output name this run actually produced")
}

// TestCompletionOffersEveryRootTheRunHas walks the four things a bare
// expression may name, at one stop, because an author at a breakpoint asking
// "what can I write" is asking about all of them at once.
func TestCompletionOffersEveryRootTheRunHas(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"step", "continue"},
		[][]string{nil, {"inspect ", "inspect vars.", "inspect inputs."}})

	require.Len(t, console.answers, 3)
	offered := texts(console.answers[0])

	assert.Equal(t, []string{"steps.", "vars.", "inputs."}, offered[:3],
		"every root this run has, each with the dot that continues it")
	assert.Contains(t, offered, "join", "and the profile's own functions")

	assert.Equal(t, []string{"region"}, texts(console.answers[1]), "`vars.`")
	assert.Equal(t, []string{"version"}, texts(console.answers[2]), "`inputs.`")
}

// TestALoopsBindingIsOfferedBare covers the fourth thing an expression may
// name, and the one an editor gets from the file while a run gets it from the
// scope it is actually standing in.
func TestALoopsBindingIsOfferedBare(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	console := &asking{steps: []string{"step", "continue"}, ask: [][]string{nil, {"inspect "}}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)

	looping := &v1.Workflow{Name: "looping", Steps: []*v1.Node{{
		Id: "each",
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList("a", "b"),
			Iterator: "letter",
			Body:     []*v1.Node{markStep("inner")},
		}},
	}}}

	_, err = v1.Run(ctx, looping)
	require.NoError(t, err)

	require.Len(t, console.answers, 1)
	assert.Contains(t, texts(console.answers[0]), "letter",
		"the name the loop bound, written bare because that is how the grammar binds it")
	assert.NotContains(t, texts(console.answers[0]), "letter.",
		"and bare rather than rooted: it is not a namespace, and its element type is not known")
}

// TestTheVerbsCompleteAndTheAutopsysAreFewer.
//
// The second half is the one worth the test: the movement verbs are gone once
// the run is over, and offering them would be teaching a command whose whole
// effect is to leave.
func TestTheVerbsCompleteAndTheAutopsysAreFewer(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"continue"},
		[][]string{{"b"}})

	require.Len(t, console.answers, 1)
	assert.Equal(t, []string{"break ", "breakpoints"}, texts(console.answers[0]),
		"both verbs starting with `b`, and the one taking an argument written with its space")

	// The autopsy's own prompt, over a finished run. Asked from inside it, as
	// every other question here is: the answer depends on the session knowing
	// which of its two prompts it is drawing.
	var out strings.Builder
	autopsyConsole := &asking{steps: []string{"quit"}, ask: [][]string{{""}}}
	session, err := flowdebug.New(flowdebug.Options{Console: autopsyConsole, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	autopsyConsole.session = session

	session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), nil, []string{"a failure"})

	require.Len(t, autopsyConsole.answers, 1)
	offered := texts(autopsyConsole.answers[0])
	assert.Contains(t, offered, "inspect ")
	assert.Contains(t, offered, "scope")
	assert.NotContains(t, offered, "step", "there is no run left to step")
	assert.NotContains(t, offered, "continue", "nor to continue")
	assert.NotContains(t, offered, "until ", "nor to run until anything")
}

// TestBreakCompletesOverStepsTheRunHasNotReached is what makes the completion
// useful rather than merely present: a breakpoint is set for somewhere the run
// has not been, so an answer drawn only from the scope would be empty at
// exactly the moment somebody wants one.
func TestBreakCompletesOverStepsTheRunHasNotReached(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{Steps: []string{"build", "test", "deploy"}},
		[]string{"continue"},
		[][]string{{"break de"}})

	require.Len(t, console.answers, 1)
	assert.Equal(t, []string{"deploy"}, texts(console.answers[0]),
		"a step this run will reach and has not")
	assert.Equal(t, "de", console.answers[0].Prefix)
}

// TestBreakCompletesOverStepsSeenWhenNobodyNamedAny is the same question for an
// embedder holding only the [v1.Debugger] seam, which is never handed the
// workflow.
func TestBreakCompletesOverStepsSeenWhenNobodyNamedAny(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"step", "step", "continue"},
		[][]string{nil, {"break "}, nil})

	require.Len(t, console.answers, 1)
	assert.Equal(t, []string{"build", "test"}, texts(console.answers[0]),
		"the ids this session has watched go past, and not the one it has not reached")
}

// TestDeleteCompletesOverBreakpointsAndNotOverSteps is the negative direction
// of the pair: `delete` names something the session holds, and offering every
// step there would be offering a hundred names of which none is a breakpoint.
func TestDeleteCompletesOverBreakpointsAndNotOverSteps(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{Steps: []string{"build", "test", "deploy"}, Breakpoints: []string{"deploy"}},
		[]string{"continue"},
		[][]string{{"delete "}})

	require.Len(t, console.answers, 1)
	assert.Equal(t, []string{"deploy"}, texts(console.answers[0]))
}

// TestNothingIsOfferedForAVerbWithNoArgument, and nothing for a verb this
// session has never heard of — the same silence, because guessing at either is
// how a prompt starts writing text nobody asked for.
func TestNothingIsOfferedForAVerbWithNoArgument(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"continue"},
		[][]string{{"scope ", "setp "}})

	require.Len(t, console.answers, 2)
	assert.Empty(t, console.answers[0].Candidates)
	assert.Empty(t, console.answers[1].Candidates,
		"and a misspelled verb has no argument to complete")
}

// TestTheVerbIsReadTheWayTheSessionReadsIt: the completer and [Session.dispatch]
// must agree about where the verb ends, or a line with a leading space
// completes as nothing and then runs perfectly.
func TestTheVerbIsReadTheWayTheSessionReadsIt(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{Steps: []string{"deploy"}},
		[]string{"continue"},
		[][]string{{"   break de", "break\tde", "  br"}})

	require.Len(t, console.answers, 3)
	assert.Equal(t, []string{"deploy"}, texts(console.answers[0]), "a leading space")
	assert.Equal(t, []string{"deploy"}, texts(console.answers[1]), "a tab between the verb and its argument")
	assert.Equal(t, []string{"break ", "breakpoints"}, texts(console.answers[2]), "and a verb still being typed")
}

// TestCompletionIsBounded.
//
// A workflow with more steps than the bound is a workflow somebody wrote, so
// the quantity is not this session's to choose. Asserting the bound was
// *reached* as well as not exceeded, because `<=` is also satisfied by an
// answer that gave up (CLAUDE.md).
func TestCompletionIsBounded(t *testing.T) {
	t.Parallel()

	many := make([]string, 0, celcomplete.MaxCandidates*2)
	for i := range celcomplete.MaxCandidates * 2 {
		many = append(many, fmt.Sprintf("step%04d", i))
	}

	console, _ := completingRun(t,
		flowdebug.Options{Steps: many},
		[]string{"continue"},
		[][]string{{"break step"}})

	require.Len(t, console.answers, 1)
	assert.Len(t, console.answers[0].Candidates, celcomplete.MaxCandidates)
	assert.True(t, console.answers[0].Truncated)
}

// TestCompletionAnswersOverAStreamWithNoConsoleAtAll is the degradation this
// whole feature is constrained by, asked where it can be answered without a
// terminal: a session reading a redirected script is the same session, holding
// the same run at the same boundary, and [Session.Complete] answers there too.
//
// That is what lets a surface with no keyboard ask the same question — the MCP
// tool, an editor's debug adapter — and it is the property a prompt that only
// worked on a TTY would have taken with it.
func TestCompletionAnswersOverAStreamWithNoConsoleAtAll(t *testing.T) {
	t.Parallel()

	commands, typing := io.Pipe()
	t.Cleanup(func() { _ = typing.Close() })

	prompted := &promptWatcher{seen: make(chan struct{})}

	session, err := flowdebug.New(flowdebug.Options{In: commands, Out: prompted})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"build": {NamedValues: map[string]*v1.Value{"artifact": v1.NewLiteral("a.tar")}},
		},
	})

	held := make(chan error, 1)
	go func() {
		held <- session.BeforeStep(t.Context(), markStep("deploy"), scope)
	}()

	select {
	case <-prompted.seen:
	case err := <-held:
		t.Fatalf("the boundary was not held: %v", err)
	}

	assert.Equal(t, []string{"build"}, texts(session.Complete("inspect steps.", len("inspect steps."))),
		"the paused run's own scope, over a stream, with no terminal anywhere")

	_, err = typing.Write([]byte("continue\n"))
	require.NoError(t, err)
	require.NoError(t, <-held)

	// And nothing once the run has moved on. Completion is a property of the
	// prompt: a session still answering about the last boundary would be
	// describing a position the run has left, which is the "confident and
	// wrong" failure CLAUDE.md names in a different setting.
	assert.Empty(t, session.Complete("inspect steps.", len("inspect steps.")).Candidates)
}

// promptWatcher closes seen the first time the session draws its prompt, so a
// test can ask a question while the run is genuinely held rather than hoping.
type promptWatcher struct {
	mu      sync.Mutex
	written strings.Builder
	seen    chan struct{}
	once    sync.Once
}

// Write implements [io.Writer].
func (w *promptWatcher) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.written.Write(p)
	drawn := strings.Contains(w.written.String(), flowdebug.Prompt)
	w.mu.Unlock()

	if drawn {
		w.once.Do(func() { close(w.seen) })
	}

	return len(p), nil
}

// TestTheAutopsyCompletesTheBindingsInspectActuallyResolves (Codex, #1114).
//
// The autopsy's `run` and `vars` arrive as bare bindings that win over
// everything in v1.Scope.ActivationWith. Held as locals they were opaque past a
// dot, so `run.` offered nothing — and worse, `vars.` still answered from the
// workflow's ambient vars, teaching a name that resolves to something else. A
// completer that disagrees with the evaluator beside it is the two-surfaces
// problem CLAUDE.md legislates against, not a missing feature.
func TestTheAutopsyCompletesTheBindingsInspectActuallyResolves(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	console := &asking{steps: []string{"quit"}, ask: [][]string{{"inspect run.", "inspect vars."}}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	// A workflow-level var, and an autopsy binding of the same root name
	// carrying something else — which is what actually resolves.
	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.AmbientVars = map[string]*v1.Value{"from_the_workflow": v1.NewValue("x")}

	extra := map[string]ref.Val{
		"run":  v1.TypeAdapter.NativeToValue(map[string]any{"failed": true, "error": "boom"}),
		"vars": v1.TypeAdapter.NativeToValue(map[string]any{"from_the_case": "y"}),
	}

	session.Autopsy(t.Context(), scope, extra, []string{"a failure"})

	require.Len(t, console.answers, 2)

	assert.Equal(t, []string{"error", "failed"}, texts(console.answers[0]),
		"`run.` has to reach the bindings the failing check was judged under")

	assert.Equal(t, []string{"from_the_case"}, texts(console.answers[1]),
		"and `vars.` has to be the autopsy's, which is what inspect resolves — "+
			"offering the workflow's would teach a name that answers differently")
}

// TestATabPressCostsNoMoreThanTheAnswerItProduces (Codex, #1114).
//
// MaxCandidates bounds the *answer*, and the first cut let a task's own output
// map decide how much work happened before that bound applied — every key
// sorted and built into a candidate, then cut to 512. The map is the
// workload's: a plugin's return, or a stubbed `returns:` in a document
// submitted to flowstate_debug. So it is the resource the far side controls,
// and bounding a proxy for it is not bounding it (CLAUDE.md).
func TestATabPressCostsNoMoreThanTheAnswerItProduces(t *testing.T) {
	t.Parallel()

	// One step whose outputs run far past the answer bound.
	named := make(map[string]*v1.Value, 4*celcomplete.MaxCandidates)
	for i := range 4 * celcomplete.MaxCandidates {
		named[fmt.Sprintf("output_%05d", i)] = v1.NewValue("v")
	}

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Outputs = &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{"big": {NamedValues: named}},
	}

	var out strings.Builder

	console := &asking{
		steps: []string{"quit"},
		ask:   [][]string{{"inspect steps.big.", "inspect steps.big.output_00007"}},
	}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})

	require.Len(t, console.answers, 2)

	whole := console.answers[0]
	assert.LessOrEqual(t, len(whole.Candidates), celcomplete.MaxCandidates)
	assert.True(t, whole.Truncated,
		"a prefix must never be presented as the whole of what a name offers")

	// Bounded during collection, so what survives is the first names in order
	// rather than an arbitrary subset — a completer whose answer changed
	// between two identical tab presses would be worse than a slow one.
	require.NotEmpty(t, whole.Candidates)
	assert.Equal(t, "output_00000", whole.Candidates[0].Text)

	// The half that only Candidate.Truncated can carry, and the reason it
	// exists. Here the *answer* is one candidate — far inside MaxCandidates —
	// so celcomplete's own bound has nothing to report. The member list it was
	// drawn from was still a prefix, and saying otherwise would tell an author
	// they had seen everything `steps.big.` offers.
	narrow := console.answers[1]
	assert.Len(t, narrow.Candidates, 1)
	assert.True(t, narrow.Truncated,
		"an answer drawn from a truncated member list is truncated, however few it holds")
}
