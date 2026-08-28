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
		flowdebug.Options{Steps: declared("completed", "build", "test", "deploy")},
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
		flowdebug.Options{Steps: declared("completed", "build", "test", "deploy"), Breakpoints: []string{"deploy"}},
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
		flowdebug.Options{Steps: declared("completed", "deploy")},
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
		flowdebug.Options{Steps: declared("completed", many...)},
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

	// And the name an author actually types is reachable, however far past the
	// bound it sorts — the prefix reaches the collection rather than following
	// it (Codex, #1114). This used to answer nothing while claiming there was
	// more, which is the worst of both: the name withheld and the notice given
	// for names that were never being asked about.
	narrow := console.answers[1]
	assert.Equal(t, []string{"output_00007"}, texts(narrow),
		"a uniquely-narrowing prefix has to reach its name")
	assert.False(t, narrow.Truncated,
		"and one match out of one is complete: `and more` here would be about "+
			"names that do not match what was typed")
}

// TestATruncatedMemberListIsTruncatedEvenAtAFullAnswer is the half that only a
// member level's own flag can carry.
//
// A prefix matching more names than the bound holds leaves the source returning
// exactly [celcomplete.MaxCandidates] of them — at which point celcomplete's
// own cap never sees a candidate it has to refuse, and reports the answer
// complete. The source is the only thing that knows it cut.
func TestATruncatedMemberListIsTruncatedEvenAtAFullAnswer(t *testing.T) {
	t.Parallel()

	named := make(map[string]*v1.Value, 2*celcomplete.MaxCandidates)
	for i := range 2 * celcomplete.MaxCandidates {
		named[fmt.Sprintf("output_%05d", i)] = v1.NewLiteral("v")
	}

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Outputs = &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{"big": {NamedValues: named}},
	}

	var out strings.Builder

	// `output_0` matches every one of them, so the cut happens inside the
	// source and the answer arrives exactly full.
	console := &asking{steps: []string{"quit"}, ask: [][]string{{"inspect steps.big.output_0"}}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})

	require.Len(t, console.answers, 1)

	assert.Len(t, console.answers[0].Candidates, celcomplete.MaxCandidates,
		"exactly full, which is what stops celcomplete's own cap from firing")
	assert.True(t, console.answers[0].Truncated,
		"so the member level has to say it cut, or a full answer reads as the whole of it")
}

// TestTheStepsRootSaysWhenTheRunHasMoreSteps (Codex, #1114): the quieter half
// of the truncation problem.
//
// Each step propagated whether its own outputs were cut, and the collection of
// *step ids* threw the same flag away — so a run past MaxCandidates steps
// offered 512 with no "and more", and an author who could not find a step
// would conclude it never ran. A completer may be incomplete; it may not be
// misleading about it.
func TestTheStepsRootSaysWhenTheRunHasMoreSteps(t *testing.T) {
	t.Parallel()

	steps := make(map[string]*v1.Node_Outputs, 2*celcomplete.MaxCandidates)
	for i := range 2 * celcomplete.MaxCandidates {
		steps[fmt.Sprintf("step_%05d", i)] = &v1.Node_Outputs{}
	}

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Outputs = &v1.Workflow_StepOutputs{StepValues: steps}

	var out strings.Builder

	console := &asking{
		steps: []string{"quit"},
		ask:   [][]string{{"inspect steps.", "inspect steps.step_00007"}},
	}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})

	require.Len(t, console.answers, 2)

	// Listing the lot: the root cuts at the bound and has to say so. The
	// answer arrives exactly full, so celcomplete's own cap never refuses a
	// candidate and cannot be what reports this.
	whole := console.answers[0]
	assert.Len(t, whole.Candidates, celcomplete.MaxCandidates)
	assert.True(t, whole.Truncated,
		"the run has more steps than the root could hold, and saying otherwise "+
			"tells an author a step they cannot find never ran")

	// Naming one of them: complete, and complete *honestly*. The prefix now
	// reaches the root's own collection, so this answers the step asked for
	// rather than reporting `and more` about 1023 steps nobody asked about.
	named := console.answers[1]
	assert.Equal(t, []string{"step_00007"}, texts(named),
		"the step an author names is the step they get")
	assert.False(t, named.Truncated, "and it is the whole of what they asked for")
}

// TestAnAutopsyBindingWithManyKeysIsStableAndSaysItIsShort (Codex, #1114) is
// the third site of the same truncation class, and the one whose second symptom
// was worse than the first.
//
// It took the first MaxCandidates keys *in iteration order* and sorted those.
// Go randomises map iteration, so two identical tab presses could offer two
// different subsets — and neither said it was short. A completer that is
// incomplete is tolerable; one that is incomplete *differently each time* is
// not something an author can reason against at all.
func TestAnAutopsyBindingWithManyKeysIsStableAndSaysItIsShort(t *testing.T) {
	t.Parallel()

	// An autopsy `vars` with far more keys than the bound.
	vars := make(map[string]any, 4*celcomplete.MaxCandidates)
	for i := range 4 * celcomplete.MaxCandidates {
		vars[fmt.Sprintf("var_%05d", i)] = i
	}
	extra := map[string]ref.Val{"vars": v1.TypeAdapter.NativeToValue(vars)}

	ask := func() flowdebug.Completion {
		t.Helper()

		var out strings.Builder

		console := &asking{steps: []string{"quit"}, ask: [][]string{{"inspect vars."}}}
		session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })
		console.session = session

		session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), extra, []string{"a failure"})

		require.Len(t, console.answers, 1)

		return console.answers[0]
	}

	first := ask()

	assert.Len(t, first.Candidates, celcomplete.MaxCandidates)
	assert.True(t, first.Truncated,
		"a binding with four times the bound must not present its prefix as the whole of itself")
	assert.Equal(t, "var_00000", first.Candidates[0].Text,
		"and what is kept is the first names in order, not the first the iterator happened to reach")

	// The property the old shape could not hold. Repeated because Go's map
	// iteration order is what varied it, so a single comparison could pass by
	// luck.
	for range 8 {
		assert.Equal(t, texts(first), texts(ask()),
			"two identical tab presses answered differently")
	}
}

// TestAStepPastTheBoundIsStillReachableByTypingIt is the regression an existing
// test caught while this was being written, kept as a claim of its own because
// the wrong version looks right.
//
// Bounding the id collection and filtering afterwards is the obvious shape and
// it silently loses answers: the alphabetically-first MaxCandidates ids of a
// large run need not contain any of the matches for what was actually typed, so
// a step sitting past the cut becomes uncompletable — the completer reporting
// nothing for a name that exists and would have run.
//
// Filtering before the bound costs the same walk and answers correctly.
func TestAStepPastTheBoundIsStillReachableByTypingIt(t *testing.T) {
	t.Parallel()

	// Far more steps than the bound, and one whose id sorts past every one of
	// them — so it is exactly the name a bound-then-filter completer loses.
	steps := make(map[string]*v1.Node_Outputs, 4*celcomplete.MaxCandidates)
	for i := range 4 * celcomplete.MaxCandidates {
		steps[fmt.Sprintf("step_%05d", i)] = &v1.Node_Outputs{}
	}
	steps["zzz_the_one_you_want"] = &v1.Node_Outputs{}

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Outputs = &v1.Workflow_StepOutputs{StepValues: steps}

	var out strings.Builder

	console := &asking{steps: []string{"quit"}, ask: [][]string{{"break zzz"}}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})

	require.Len(t, console.answers, 1)

	assert.Equal(t, []string{"zzz_the_one_you_want"}, texts(console.answers[0]),
		"a step past the alphabetical bound has to be reachable by typing its name")
	assert.False(t, console.answers[0].Truncated,
		"and one match out of one is not a truncated answer")
}

// TestASeenStepPastTheCacheLeavesTheAnswerSayingItIsShort covers the one
// truncation the answer's own cap cannot see.
//
// [flowdebug.Options.Steps] is deliberately omitted, which is the embedder's
// case: a caller handed the [v1.RunObserver] seam and nothing else knows the
// run only by watching it. Past 512 distinct ids the session stops remembering,
// and a prompt whose scope no longer carries those outputs has no other source
// for them — so `break zzz` matches nothing. Answering nothing is unavoidable;
// answering nothing *silently* is the defect, because a complete-looking empty
// list tells an author the step does not exist.
func TestASeenStepPastTheCacheLeavesTheAnswerSayingItIsShort(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	console := &asking{steps: []string{"quit"}, ask: [][]string{{"break zzz"}}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	// The run walks past more ids than the cache holds, and the one an author
	// would go looking for arrives after it is full.
	for i := range celcomplete.MaxCandidates {
		session.StepSkipped(fmt.Sprintf("step_%05d", i))
	}
	session.StepSkipped("zzz_the_one_you_want")

	// A scope carrying none of it: the autopsy of a run whose outputs are not
	// the steps that were skipped.
	session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), nil, []string{"a failure"})

	require.Len(t, console.answers, 1)

	assert.Empty(t, texts(console.answers[0]),
		"the id is genuinely gone, and the test would be about something else if it were not")
	assert.True(t, console.answers[0].Truncated,
		"an empty answer drawn from a cache that dropped ids has to say the list is short")
}

// TestARunLoopingOverOneStepDoesNotCallItselfShort is the other direction of
// the same flag: a full cache that refuses an id it already holds has lost
// nothing, and a session that reported truncation on every repeat would print
// the notice for the rest of the run.
func TestARunLoopingOverOneStepDoesNotCallItselfShort(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	console := &asking{steps: []string{"quit"}, ask: [][]string{{"break step"}}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	for i := range celcomplete.MaxCandidates {
		session.StepSkipped(fmt.Sprintf("step_%05d", i))
	}
	// Full now, and the loop goes round again over ids it already has.
	for i := range celcomplete.MaxCandidates {
		session.StepSkipped(fmt.Sprintf("step_%05d", i))
	}

	session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), nil, []string{"a failure"})

	require.Len(t, console.answers, 1)

	assert.Len(t, texts(console.answers[0]), celcomplete.MaxCandidates,
		"every id the cache holds still matches")
	assert.False(t, console.answers[0].Truncated,
		"a cache that refused only repeats is not short")
}

// TestScopeSaysHowManyNamesItDidNotList (scale audit, #1111 follow-up).
//
// `scope` answers "what can I name right now". At a run of hundreds of steps
// it used to answer with one unbroken comma-separated wall — every step id,
// joined, on a single line: unreadable in a terminal and expensive in an
// agent's context, which is the same cost the completer was bounded for two
// commits earlier while this line was missed.
//
// The bound is only half of it. A list cut at twenty with nothing said tells a
// reader their run has twenty steps, which is the misleading-about-being-
// incomplete failure the truncation notices exist to prevent.
func TestScopeSaysHowManyNamesItDidNotList(t *testing.T) {
	t.Parallel()

	steps := make(map[string]*v1.Node_Outputs, 4*flowdebug.MaxScopeNames)
	for i := range 4 * flowdebug.MaxScopeNames {
		steps[fmt.Sprintf("step_%05d", i)] = &v1.Node_Outputs{}
	}

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Outputs = &v1.Workflow_StepOutputs{StepValues: steps}

	var out strings.Builder

	console := &asking{steps: []string{"scope", "quit"}, ask: [][]string{nil, nil}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})

	printed := out.String()

	assert.Contains(t, printed, "step_00000", "the names it does list are the first in order")
	assert.NotContains(t, printed, fmt.Sprintf("step_%05d", 4*flowdebug.MaxScopeNames-1),
		"and it stops rather than printing every id a large run produced")
	assert.Contains(t, printed, fmt.Sprintf("and %d more", 4*flowdebug.MaxScopeNames-flowdebug.MaxScopeNames),
		"saying how many it did not list, or the reader thinks that was all of them")
}

// TestEachScopeLineNamesItsOwnNamespace (Codex, #1115).
//
// One truncation notice served four lines drawn from three completion sources,
// and it named `inspect steps.` on all of them. That is worse than naming
// nothing: after the cut the notice is the only thing left saying those names
// exist, so pointing it at a command that cannot reach them sends a reader
// somewhere the names are provably absent.
//
// The pairing is easy to get backwards, which is why it is pinned rather than
// read. The line labelled `vars:` holds `Scope.Vars` — a loop's `as:`, a step's
// own `vars:` — which complete *bare*, under no root. The line labelled
// `workflow vars:` holds `Scope.AmbientVars`, and those are what `vars.`
// reaches.
func TestEachScopeLineNamesItsOwnNamespace(t *testing.T) {
	t.Parallel()

	many := func(prefix string) map[string]*v1.Value {
		out := make(map[string]*v1.Value, 2*flowdebug.MaxScopeNames)
		for i := range 2 * flowdebug.MaxScopeNames {
			out[fmt.Sprintf("%s_%03d", prefix, i)] = v1.NewLiteral("v")
		}

		return out
	}

	steps := make(map[string]*v1.Node_Outputs, 2*flowdebug.MaxScopeNames)
	for i := range 2 * flowdebug.MaxScopeNames {
		steps[fmt.Sprintf("step_%03d", i)] = &v1.Node_Outputs{}
	}

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Outputs = &v1.Workflow_StepOutputs{StepValues: steps}
	scope.Vars = many("bare")
	scope.AmbientVars = many("declared")

	var out strings.Builder

	console := &asking{steps: []string{"scope", "quit"}, ask: [][]string{nil, nil}}
	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})

	lines := map[string]string{}
	for _, line := range strings.Split(out.String(), "\n") {
		label, rest, ok := strings.Cut(line, ": ")
		if ok {
			lines[label] = rest
		}
	}

	require.Contains(t, lines, "steps")
	assert.Contains(t, lines["steps"], "`inspect steps.` lists them",
		"step outputs are what `steps.` reaches")

	require.Contains(t, lines, "workflow vars")
	assert.Contains(t, lines["workflow vars"], "`inspect vars.` lists them",
		"the workflow's declared vars are what `vars.` reaches, despite the label")

	require.Contains(t, lines, "vars")
	assert.Contains(t, lines["vars"], "more (tab completes them)",
		"bare bindings belong to no namespace, so the notice names none")
	assert.NotContains(t, lines["vars"], "inspect",
		"and must not send a reader to a command that cannot reach them")
}

// TestCompletionAfterAnIfOffersTheExpressionNotStepIDs (Codex, #1116).
//
// `break` completes step ids, but past an `if` the argument stops being a step
// id and becomes an expression. Left as ids, `break body if de<tab>` offered —
// and, with one match, *inserted* — a step called `deploy` into the middle of a
// condition. A completion that writes a name the expression cannot mean is
// worse than one that offers nothing.
func TestCompletionAfterAnIfOffersTheExpressionNotStepIDs(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"step", "continue"},
		[][]string{nil, {"break bu", "break build if steps.", "break build if bu"}})

	require.Len(t, console.answers, 3)

	assert.Equal(t, []string{"build"}, texts(console.answers[0]),
		"before an `if` the argument is a step id, and that has not changed")

	// The discriminator: `steps.` is a name only the expression completer can
	// answer — no step id begins with it — so an answer here proves the
	// routing rather than merely being consistent with it.
	assert.Equal(t, []string{"build"}, texts(console.answers[1]),
		"past the `if` the expression completer answers, over the paused run's own scope")

	assert.NotContains(t, texts(console.answers[2]), "build",
		"and a bare word past the `if` is no longer read as a step id, so the "+
			"step that shares its prefix is not written into the condition")
}

// TestCompletingAfterASpaceInAConditionDoesNotEatTheWordBeforeIt
// (Codex, #1116).
//
// The console replaces the bytes immediately before the cursor with the
// candidate, and it is told how many by the answer's prefix. So a prefix that
// disagrees with what is actually before the cursor does not merely offer the
// wrong names — it *corrupts the line*.
//
// `break body if inp <tab>` did exactly that. The condition was handed to the
// completer trimmed, so the answer reported the prefix `inp` while the three
// bytes before the cursor were `np `; accepting a candidate wrote `iinputs.`.
//
// Whitespace before a cursor is not nothing: it is what says the current word
// is empty and everything is on offer.
func TestCompletingAfterASpaceInAConditionDoesNotEatTheWordBeforeIt(t *testing.T) {
	t.Parallel()

	console, _ := completingRun(t,
		flowdebug.Options{},
		[]string{"step", "continue"},
		[][]string{nil, {"break build if inp", "break build if inp ", "break build if "}})

	require.Len(t, console.answers, 3)

	assert.Equal(t, "inp", console.answers[0].Prefix,
		"a partial word is the prefix, and that is what the console will replace")

	assert.Empty(t, console.answers[1].Prefix,
		"but past a space the word is empty, and claiming three characters here "+
			"makes the console cut into the word before the space")
	assert.Contains(t, texts(console.answers[1]), "inputs.",
		"and everything is on offer, as after any other space")

	assert.Empty(t, console.answers[2].Prefix,
		"the same immediately after `if `, where there is no word at all yet")
	assert.Contains(t, texts(console.answers[2]), "steps.")
}
