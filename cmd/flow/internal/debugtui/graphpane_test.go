package debugtui

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// ---- programs and runs to draw ----

func task(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "http"}}}
}

func loopOf(id string, body ...*v1.Node) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Loop{Loop: &v1.Loop{Body: body}}}
}

func parallelOf(id string, branches ...[]*v1.Node) *v1.Node {
	p := &v1.Parallel{}
	for _, steps := range branches {
		p.Branches = append(p.Branches, &v1.Parallel_Branch{Steps: steps})
	}

	return &v1.Node{Id: id, Kind: &v1.Node_Parallel{Parallel: p}}
}

func switchOf(id string, cases [][]*v1.Node, otherwise []*v1.Node) *v1.Node {
	sw := &v1.Switch{}
	for _, steps := range cases {
		sw.Cases = append(sw.Cases, &v1.Switch_Case{Steps: steps})
	}
	if otherwise != nil {
		sw.Default = &v1.Switch_Default{Steps: otherwise}
	}

	return &v1.Node{Id: id, Kind: &v1.Node_Switch{Switch: sw}}
}

func callOf(id string, callee *v1.Workflow) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}
}

// flowProgram is a small workflow with a parallel group and a loop.
func flowProgram() *v1.Workflow {
	return &v1.Workflow{Name: "release", Steps: []*v1.Node{
		task("fetch"),
		task("validate"),
		parallelOf("checks", []*v1.Node{task("notify")}, []*v1.Node{task("audit")}),
		loopOf("pages", task("page"), task("store")),
		task("charge"),
	}}
}

func obs(kind v1.DebugObservationKind, address string) *v1.DebugObservation {
	return &v1.DebugObservation{Kind: kind, Address: address}
}

const (
	seenDone      = v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED
	seenSkipped   = v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED
	seenFailed    = v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED
	seenTolerated = v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TOLERATED
	seenWaiting   = v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_WAITING
)

// inLoop holds the run before `store` in the second pass of `pages`, with the
// steps before it having done the things a run does.
func inLoop(f *fakeTarget) *fakeTarget {
	f.program = []string{"fetch", "validate", "checks", "pages", "charge"}
	f.at = 3
	f.occurrence = &v1.DebugOccurrence{
		Address:  "pages[1]/store",
		Site:     &v1.DebugSite{Workflow: "release", Path: []string{"pages", "store"}, Kind: "task"},
		Segments: []*v1.DebugSegment{{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, StepId: "pages", Index: 1}},
	}
	f.observations = []*v1.DebugObservation{
		obs(seenDone, "fetch"),
		obs(seenSkipped, "validate"),
		obs(seenDone, "checks#0/notify"),
		obs(seenTolerated, "checks#1/audit"),
		obs(seenDone, "checks"),
		obs(seenDone, "pages[0]/page"),
		obs(seenDone, "pages[0]/store"),
		obs(seenDone, "pages[1]/page"),
	}

	return f
}

// flowFrameOf reads a frame of the target with the program given.
func flowFrameOf(t *testing.T, f *fakeTarget, program *v1.Workflow) flowdebug.Frame {
	t.Helper()

	frame, err := flowdebug.ReadFrame(t.Context(), f, flowdebug.FrameOptions{Program: program, StepRows: stepRowsAsked})
	require.NoError(t, err)

	return frame
}

// flowScreenOf is a screen over a frame of the program, with the flow focused.
func flowScreenOf(t *testing.T, f *fakeTarget, program *v1.Workflow, size tui.Size) Screen {
	t.Helper()

	s := screenOf(t, f, false, size)
	s.Frame = flowFrameOf(t, f, program)
	s.Flow = NewFlow()
	s.Focus, s.Pane = paneFlow, paneFlow

	return s
}

// flowModel is a model over the fake with the program given.
func flowModel(t *testing.T, f *fakeTarget, program *v1.Workflow, mods ...func(*Config)) Model {
	t.Helper()

	return started(t, f, append([]func(*Config){func(c *Config) { c.Frame.Program = program }}, mods...)...)
}

// flowText is the flow pane alone, as text.
func flowText(s Screen, st Style, w, h int) string {
	return FlowView(s.Flow, s.Frame, s.Loaded, opts(st, w, h, true))
}

// ---- the picture ----

func TestTheFlowIsDrawnAsALadderInEveryVariant(t *testing.T) {
	for _, v := range styles {
		t.Run(v.name, func(t *testing.T) {
			s := flowScreenOf(t, inLoop(newFake()), flowProgram(), tui.Size{W: 120, H: 36})
			var b strings.Builder
			for _, width := range []int{32, 46, 64} {
				text := flowText(s, v.style, width, 15)
				b.WriteString(fmt.Sprintf("=== %d wide\n%s\n", width, text))
				for _, line := range lines(text) {
					assert.LessOrEqual(t, lipgloss.Width(line), width)
				}
				assert.Len(t, lines(text), 15)
			}
			tuitest.Golden(t, b.String())
		})
	}
}

func TestEveryStateIsDrawnWithItsOwnMarkAndWord(t *testing.T) {
	t.Parallel()

	program := &v1.Workflow{Steps: []*v1.Node{
		task("held"), task("running"), task("waiting"), task("done"), task("tolerated"), task("failed"), task("skipped"), task("pending"),
	}}
	f := newFake()
	f.program = []string{"held"}
	f.at = 0
	f.occurrence = &v1.DebugOccurrence{Address: "held", Site: &v1.DebugSite{Path: []string{"held"}}}
	f.observations = []*v1.DebugObservation{
		obs(seenWaiting, "waiting"), obs(seenDone, "done"), obs(seenTolerated, "tolerated"), obs(seenFailed, "failed"), obs(seenSkipped, "skipped"),
	}
	frame := flowFrameOf(t, f, program)
	// The only state not reached by an observation: a group around the held step.
	frame.Overlay.States["running"] = flowdebug.NodeRunning

	s := flowScreenOf(t, f, program, tui.Size{W: 120, H: 36})
	s.Frame = frame
	line := func(id string, st Style) string {
		for _, l := range lines(flowText(s, st, 60, 14)) {
			if strings.Contains(l, " "+id+" ") || strings.HasSuffix(strings.TrimRight(l, " "), " "+id) {
				return l
			}
		}
		require.Failf(t, "not drawn", "%s", id)

		return ""
	}

	want := map[string]struct{ mark, word string }{
		"held":      {styled.Symbols.Running, "held"},
		"running":   {styled.Symbols.Running, "running"},
		"waiting":   {styled.Symbols.Waiting, "waiting"},
		"done":      {styled.Symbols.Success, ""},
		"tolerated": {styled.Symbols.Warning, "tolerated"},
		"failed":    {styled.Symbols.Failure, ""},
		"skipped":   {styled.Symbols.Skipped, "skipped"},
		"pending":   {styled.Symbols.Waiting, ""},
	}
	for id, w := range want {
		got := line(id, plain)
		assert.Contains(t, got, w.mark, id)
		if w.word != "" {
			assert.Contains(t, got, w.word, id)
		}
	}
	// Told apart by words and marks, never by colour alone: in ASCII a pending and
	// a waiting step share a mark, and the word is what differs.
	asciiPlain := styleFor(colorprofile.NoTTY, false)
	assert.Equal(t, line("pending", asciiPlain)[:6], line("waiting", asciiPlain)[:6], "the premise: the two share a mark in ASCII")
	assert.NotContains(t, line("pending", asciiPlain), "waiting")
	assert.Contains(t, line("waiting", asciiPlain), "waiting")
}

func TestTheOverlayStatesComeFromTheStopNotFromTheStructure(t *testing.T) {
	t.Parallel()

	f := inLoop(newFake())
	program := flowProgram()
	s := flowScreenOf(t, f, program, tui.Size{W: 120, H: 36})
	first := flowText(s, plain, 50, 16)
	require.Contains(t, first, "store")

	ladder := s.Flow.ladder
	f.at = 4
	f.occurrence = nil
	f.observations = append(f.observations, obs(seenDone, "pages[1]/store"), obs(seenDone, "pages"))
	s.Frame = flowFrameOf(t, f, program)
	second := flowText(s, plain, 50, 16)

	assert.NotEqual(t, first, second, "the picture did not change with the stop")
	assert.Same(t, ladder, s.Flow.ladder, "the structure was rebuilt for a stop")
	assert.Equal(t, "charge", s.Frame.Overlay.Held)

	// A different program is a different structure.
	s.Frame.Program = &v1.Workflow{Steps: []*v1.Node{task("only")}}
	flowText(s, plain, 50, 16)
	assert.NotSame(t, ladder, s.Flow.ladder)
}

func TestAGroupIsBoxedAndItsBranchesAreLabelled(t *testing.T) {
	t.Parallel()

	program := &v1.Workflow{Steps: []*v1.Node{
		parallelOf("checks", []*v1.Node{task("notify")}, []*v1.Node{task("audit")}),
		switchOf("route", [][]*v1.Node{{task("a")}, {task("b")}}, []*v1.Node{task("c")}),
		callOf("fan_out", &v1.Workflow{Name: "child", Steps: []*v1.Node{task("greet")}}),
	}}
	f := newFake()
	s := flowScreenOf(t, f, program, tui.Size{W: 120, H: 36})

	for _, v := range []struct {
		style      Style
		open, rail string
		closing    string
	}{
		{plain, "┌", "│", "└"},
		{ascii, "+", "|", "+"},
	} {
		text := flowText(s, v.style, 60, 20)
		for _, want := range []string{"branch 1", "branch 2", "case 1", "case 2", "default", "greet", v.open, v.rail, v.closing} {
			assert.Contains(t, text, want, "%q", want)
		}
	}

	l := s.Flow.ladderOf(s.Frame)
	for _, address := range []string{"checks", "checks/notify", "checks/audit", "route/a", "route/c", "fan_out/greet"} {
		assert.Contains(t, l.byAddr, address, "the static address a run's observations name")
	}
	assert.Equal(t, 9, l.steps)
}

func TestNoProgramIsOneHonestLineAndNeverBlank(t *testing.T) {
	t.Parallel()

	s := flowScreenOf(t, newFake(), nil, tui.Size{W: 120, H: 36})
	text := flowText(s, plain, 40, 10)
	assert.Contains(t, text, NoProgramNote)
	assert.Equal(t, 1, strings.Count(text, "no program"), "said once")

	for _, size := range tuitest.Sizes {
		if size.W < MinWidth || size.H < MinHeight {
			continue
		}
		s.Size = size
		full, _ := s.Draw(plain)
		assert.Contains(t, full, "no program", "%v: the flow pane was blank", size)
	}

	// The model says the same, and `u` and `B` refuse rather than guess a step.
	fake := newFake()
	m := send(started(t, fake), tuitest.Key("u"))
	assert.Contains(t, m.screen.Toast.Text(), NoProgramNote)
	m = send(m, tuitest.Key("B"))
	assert.Empty(t, fake.resumes)
	assert.Empty(t, fake.replaced)
}

func TestAPartialOverlayIsLabelledNotPending(t *testing.T) {
	t.Parallel()

	f := inLoop(newFake())
	f.observations = f.observations[len(f.observations)-1:]
	s := flowScreenOf(t, f, flowProgram(), tui.Size{W: 120, H: 36})
	complete := flowText(s, plain, 60, 14)
	assert.NotContains(t, complete, "?", "a complete overlay drew an unknown")

	s.Frame.Partial = true
	partial := flowText(s, plain, 60, 14)
	assert.Contains(t, partial, "earlier steps not shown")
	assert.Contains(t, partial, "? fetch", "a step the kept observations cannot speak for was drawn as one that has not run")
	assert.Contains(t, partial, "not shown")
	for _, l := range lines(partial) {
		if strings.Contains(l, "charge") {
			assert.NotContains(t, l, "?", "a step after the held one cannot have run")
		}
	}
}

func TestTheOverlayKeysJoinTheGraphAddresses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for name, text := range map[string]string{"main.yaml": joinFlowfile, "child.yaml": joinChild} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600))
	}
	workflow, _, err := flowfile.ParseFile(filepath.Join(dir, "main.yaml"))
	require.NoError(t, err)

	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &strings.Builder{}, Workflow: workflow})
	require.NoError(t, err)
	go func() {
		ctx := v1.NewContextWithDebugger(t.Context(), session)
		ctx = v1.NewContextWithRunObserver(ctx, session)
		_, _ = v1.RunWithInputs(ctx, workflow, nil)
		session.Finished(nil)
	}()
	t.Cleanup(func() { _ = session.Close() })

	flow := NewFlow()
	seen := map[string]bool{}
	var after uint64
	for range 80 {
		snapshot, err := session.WaitSnapshot(t.Context(), after)
		require.NoError(t, err)
		after = snapshot.GetRevision()
		if snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
			if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED {
				break
			}

			continue
		}

		frame, err := flowdebug.ReadFrame(t.Context(), session, flowdebug.FrameOptions{Program: workflow, Source: session})
		require.NoError(t, err)
		l := flow.ladderOf(frame)
		require.Contains(t, l.byAddr, frame.Overlay.Held, "the held step is not a node of the graph")
		for address := range frame.Overlay.States {
			assert.Contains(t, l.byAddr, address, "an observation names a step the graph does not draw")
			seen[address] = true
		}

		receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
			RequestId: fmt.Sprintf("r-%d", after), ExpectedRevision: after, Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
		})
		require.NoError(t, err)
		after = max(after, receipt.GetRevision()-1)
	}

	// Not vacuous: the run reached a loop body, a parallel branch and a callee.
	for _, address := range []string{"each/touch", "checks/left", "nested/greet", "start"} {
		assert.True(t, seen[address], "the run never named %s, so its join was not tested", address)
	}
}

const joinFlowfile = `edition: v2026.4
name: join
vars:
  items: ${[1, 2]}
steps:
  - id: start
    log:
      message: begin
  - id: each
    for_each:
      items: ${vars.items}
      as: item
      steps:
        - id: touch
          log:
            message: ${"item %d".format([item])}
  - id: checks
    parallel:
      - steps:
          - id: left
            log:
              message: left
      - steps:
          - id: right
            log:
              message: right
  - id: nested
    call: ./child.yaml
    with:
      who: world
  - id: done
    log:
      message: done
`

const joinChild = `edition: v2026.4
name: child
inputs:
  who:
    type: string
steps:
  - id: greet
    log:
      message: ${"hello " + inputs.who}
`

// ---- bounds and text ----

func TestAProgramPastTheBoundSaysHowManyAreNotDrawn(t *testing.T) {
	t.Parallel()

	const extra = 137
	program := &v1.Workflow{}
	for i := range MaxFlowNodes + extra {
		program.Steps = append(program.Steps, task(fmt.Sprintf("s%04d", i)))
	}
	s := flowScreenOf(t, newFake(), program, tui.Size{W: 120, H: 36})

	l := s.Flow.ladderOf(s.Frame)
	assert.Equal(t, MaxFlowNodes, l.steps)
	assert.Equal(t, extra, l.skipped)
	assert.Len(t, l.rows, MaxFlowNodes)

	text := flowText(s, plain, 70, 12)
	assert.Contains(t, text, fmt.Sprintf("%d more not drawn", extra))
	assert.Equal(t, 2, strings.Count(text, fmt.Sprintf("%d more not drawn", extra)), "said in the heading and under the ladder")
	assert.Contains(t, text, "s0000")
	assert.NotContains(t, text, fmt.Sprintf("s%04d", MaxFlowNodes), "a step past the bound was drawn")

	// A program inside the bound says nothing of the kind.
	small := flowScreenOf(t, newFake(), flowProgram(), tui.Size{W: 120, H: 36})
	assert.NotContains(t, flowText(small, plain, 70, 12), "not drawn")
}

func TestAHugeNestingIsBoundedToo(t *testing.T) {
	t.Parallel()

	// A callee nested past the depth the pane follows is counted, not walked.
	inner := &v1.Workflow{Name: "leaf", Steps: []*v1.Node{task("leaf")}}
	for i := range maxFlowCallDepth + 3 {
		inner = &v1.Workflow{Name: fmt.Sprintf("w%d", i), Steps: []*v1.Node{callOf(fmt.Sprintf("c%d", i), inner)}}
	}
	l := buildLadder(inner)
	assert.Equal(t, maxFlowCallDepth+1, l.steps)
	assert.Positive(t, l.cut)
	assert.NotContains(t, l.byAddr, strings.Repeat("c0/", 1)+"leaf")
}

func TestControlCharactersInAProgramNeverReachTheScreen(t *testing.T) {
	t.Parallel()

	evil := "ev\x1b[31mil\nx\u0085y"
	program := &v1.Workflow{Steps: []*v1.Node{
		{Id: evil, Kind: &v1.Node_Task{Task: &v1.Task{Name: "t\x1b]0;owned\x07"}}},
		parallelOf("g\rrp", []*v1.Node{task("in\x00ner")}),
	}}
	f := newFake()
	f.observations = []*v1.DebugObservation{obs(seenDone, evil)}
	s := flowScreenOf(t, f, program, tui.Size{W: 120, H: 36})

	for _, v := range styles {
		text, _ := s.Draw(v.style)
		stripped := text
		if v.name != "plain" {
			stripped = ansiFree(text)
		}
		for _, bad := range []string{"\x1b[31mil", "\x1b]0", "\x07", "\r", "\x00", "\u0085"} {
			assert.NotContains(t, stripped, bad, "%s: %q reached the screen", v.name, bad)
		}
		assert.Len(t, lines(text), s.Size.H, "%s: a newline in a name added a row", v.name)
	}
	assert.Contains(t, flowText(s, plain, 70, 8), `\x1b[31mil\nx`, "the escaped name is what is drawn")
}

// ansiFree removes the styling sequences the theme writes, leaving anything else
// that is a control character in place for the assertion to find.
func ansiFree(text string) string {
	var b strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] == 0x1b && i+1 < len(text) && text[i+1] == '[' {
			j := i + 2
			for j < len(text) && (text[j] < 0x40 || text[j] > 0x7e) {
				j++
			}
			// Only a colour/style sequence is the theme's.
			if j < len(text) && text[j] == 'm' {
				i = j

				continue
			}
		}
		b.WriteByte(text[i])
	}

	return b.String()
}

func TestAWithheldStepIdIsRedactedAndNeverTypedIntoACommand(t *testing.T) {
	t.Parallel()

	const secret = "tok-7f3a91-sentinel"
	program := &v1.Workflow{Steps: []*v1.Node{task("fetch"), task(secret)}}
	f := newFake()
	f.program = []string{"fetch", secret}
	f.at = 0
	m := flowModel(t, f, program, func(c *Config) {
		c.Frame.Inventory = nil
	})
	// The session's redactor is what a local frame carries.
	m.screen.Frame.Redact = func(text string) string { return strings.ReplaceAll(text, secret, "[redacted]") }

	text := view(m)
	assert.Contains(t, text, "[redacted]")
	tuitest.NoSecret(t, text, secret)

	// Selecting it and asking for it by key refuses, and says why, and the line
	// that would have echoed the name to the console is never sent.
	m.screen.Flow.Selected = secret
	m = send(m, tuitest.Key("u"))
	assert.Contains(t, m.screen.Toast.Text(), "withheld")
	m = send(m, tuitest.Key("B"))
	assert.Empty(t, f.resumes)
	assert.Empty(t, f.replaced)
	tuitest.NoSecret(t, view(m), secret)
	tuitest.NoSecret(t, strings.Join(m.screen.Console.Lines(), "\n"), secret)
}

// ---- keys ----

func TestTheHeldStepIsKeptInViewAndTheViewIsNotTakenBackFromAScroll(t *testing.T) {
	t.Parallel()

	program := &v1.Workflow{}
	f := newFake()
	f.program = nil
	for i := range 40 {
		id := fmt.Sprintf("s%02d", i)
		program.Steps = append(program.Steps, task(id))
		f.program = append(f.program, id)
	}
	f.at = 0
	m := flowModel(t, f, program, func(c *Config) { c.Size = tui.Size{W: 100, H: 22}; c.Frame.Inventory = nil })
	heldRow := regexp.MustCompile(`\bs\d\d\b.* held\b`)
	heldLine := func(m Model) string {
		for _, l := range lines(view(m)) {
			if heldRow.MatchString(l) {
				return l
			}
		}

		return ""
	}
	require.Contains(t, heldLine(m), "s00")

	// The run moves a long way: the held step is on screen wherever it goes.
	for range 25 {
		m = send(m, tuitest.Key("s"))
	}
	require.Contains(t, heldLine(m), "s25", "the held step left the screen")
	assert.False(t, m.screen.Flow.Scrolled)

	// The person scrolls away. The run then moves on its own (another client, a
	// watch): the view stays where it was put.
	x, y := find(t, m, flowPrefix+"s25")
	for range 12 {
		m = send(m, tuitest.Wheel(x, y, true))
	}
	require.True(t, m.screen.Flow.Scrolled)
	assert.Empty(t, heldLine(m), "scrolling did not take the view from the run")
	scrolled := flowText(m.screen, plain, 40, 14)
	assert.Contains(t, scrolled, "s00")

	f.advance()
	m = send(m, frameFrom(t, m, f))
	assert.Equal(t, "s26", m.screen.Frame.Overlay.Held)
	assert.Equal(t, scrolled, flowText(m.screen, plain, 40, 14), "a new stop fought the person's scroll")

	// Asking the run to move gives the view back to it.
	m = send(m, tuitest.Key("s"))
	assert.False(t, m.screen.Flow.Scrolled)
	assert.Contains(t, heldLine(m), "s27")

	// And a wheel at the ends of the ladder stays on it.
	for range 100 {
		m = send(m, tuitest.Wheel(x, y, false))
	}
	assert.Contains(t, view(m), "s39")
	assert.LessOrEqual(t, m.screen.Flow.Top, len(m.screen.Flow.ladder.rows))
}

// frameFrom is the message a finished read of the target would be.
func frameFrom(t *testing.T, m Model, f *fakeTarget) frameMsg {
	t.Helper()

	opts := m.cfg.Frame
	opts.StepRows = stepRowsAsked
	frame, err := flowdebug.ReadFrame(t.Context(), f, opts)
	require.NoError(t, err)

	return frameMsg{seq: m.readSeq, frame: frame}
}

func TestArrowsMoveTheSelectionAndLeftAndRightFoldAGroup(t *testing.T) {
	t.Parallel()

	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram())
	require.Equal(t, paneFlow, m.screen.Focus, "with a program the flow takes the keys")
	require.Equal(t, "pages/store", m.screen.Flow.Selected, "the held step is selected")

	selected := func() string { return m.screen.Flow.Selected }
	m = send(m, tuitest.Key("home"))
	assert.Equal(t, "fetch", selected())
	m = send(m, tuitest.Key("up"))
	assert.Equal(t, "fetch", selected(), "up at the top moved off the ladder")
	m = send(m, tuitest.Key("down"), tuitest.Key("down"))
	assert.Equal(t, "checks", selected())
	m = send(m, tuitest.Key("down"))
	assert.Equal(t, "checks/notify", selected(), "the labels between are not selectable")
	m = send(m, tuitest.Key("end"))
	assert.Equal(t, "charge", selected())
	m = send(m, tuitest.Key("pgup"))
	assert.Equal(t, "fetch", selected())

	// Fold the parallel group: its members go, it says how many, and the box with them.
	m = send(m, tuitest.Key("down"), tuitest.Key("down"), tuitest.Key("left"))
	assert.True(t, m.screen.Flow.Folded["checks"])
	folded := view(m)
	assert.NotContains(t, folded, "notify")
	assert.NotContains(t, folded, "audit")
	assert.Contains(t, folded, "(+2)")
	assert.Contains(t, folded, "charge", "folding hid what is after the group")

	// Left on an already-closed group, or on a step that is not in one, changes nothing.
	m = send(m, tuitest.Key("left"))
	assert.Equal(t, "checks", selected())
	assert.True(t, m.screen.Flow.Folded["checks"])

	m = send(m, tuitest.Key("right"))
	assert.False(t, m.screen.Flow.Folded["checks"])
	assert.Contains(t, view(m), "notify")

	// Left on a member goes to its group; left again folds it.
	m = send(m, tuitest.Key("down"), tuitest.Key("left"))
	assert.Equal(t, "checks", selected())
	m = send(m, tuitest.Key("left"))
	assert.True(t, m.screen.Flow.Folded["checks"])

	// A closed group around the held step is where the held mark is drawn, so the
	// run is never lost behind a fold.
	m = send(m, tuitest.Key("end"), tuitest.Key("up"), tuitest.Key("up"), tuitest.Key("up"))
	require.Equal(t, "pages", selected())
	m = send(m, tuitest.Key("left"))
	assert.True(t, m.screen.Flow.Folded["pages"])
	var pagesLine string
	for _, l := range lines(view(m)) {
		if strings.Contains(l, "pages") {
			pagesLine = l
		}
	}
	assert.Contains(t, pagesLine, "running")
	assert.Contains(t, pagesLine, "(+2)")
}

func TestUAndEnterRunUntilTheSelectedStep(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"u", "enter"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			f := inLoop(newFake())
			m := flowModel(t, f, flowProgram())
			m = send(m, tuitest.Key("home"), tuitest.Key("down"), tuitest.Key("down"), tuitest.Key("down"))
			require.Equal(t, "checks/notify", m.screen.Flow.Selected)

			m = send(m, tuitest.Key(key))

			require.Len(t, f.resumes, 1)
			assert.Equal(t, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, f.resumes[0].GetAction())
			assert.Equal(t, "checks/notify", f.resumes[0].GetUntil(), "the static address of the node, which the target resolves")
			assert.Contains(t, strings.Join(m.screen.Console.Lines(), "\n"), Prompt+"until checks/notify")
		})
	}

	// `u` works from another pane, since the selection is the flow's, and with
	// nothing selected it is the held step.
	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram())
	m.screen.Flow.Selected = ""
	m = send(m, tuitest.Key("tab"), tuitest.Key("u"))
	require.Len(t, f.resumes, 1)
	assert.Equal(t, "pages/store", f.resumes[0].GetUntil())
}

func TestAFrontWithoutUntilHasNoUKeyAndNoBreakKeyWithoutBreak(t *testing.T) {
	t.Parallel()

	keys, err := NewKeymap([]flowdebug.Verb{{Name: "step"}})
	require.NoError(t, err)
	for _, key := range []string{"u", "B"} {
		_, ok := keys.Match(key)
		assert.False(t, ok, key)
	}

	keys, err = NewKeymap(flowdebug.DriverVerbs())
	require.NoError(t, err)
	for key, name := range map[string]string{"u": bindUntil, "B": bindBreak} {
		binding, ok := keys.Match(key)
		require.True(t, ok, key)
		assert.Equal(t, name, binding.Name)
	}

	// Right-click and double-click reach the same commands without a key, so they
	// ask the front too.
	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram(), func(c *Config) { c.Verbs = []flowdebug.Verb{{Name: "step"}} })
	x, y := find(t, m, flowPrefix+"fetch")
	m = send(m, tuitest.RightClick(x, y))
	assert.Contains(t, m.screen.Toast.Text(), "does not answer break")
	assert.Empty(t, f.replaced)
}

func TestBTogglesABreakpointOnTheSelectedStep(t *testing.T) {
	t.Parallel()

	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram())
	m = send(m, tuitest.Key("home"), tuitest.Key("down"), tuitest.Key("down"), tuitest.Key("down"))
	require.Equal(t, "checks/notify", m.screen.Flow.Selected)
	marked := func(m Model, address string) bool {
		_, hits := m.screen.Draw(m.cfg.Style)
		x, y := find(t, m, flowPrefix+address)
		hit, _ := hits.At(x, y)
		cells := []rune(lines(view(m))[y])

		return strings.Contains(string(cells[hit.Rect.X:hit.Rect.X+hit.Rect.W]), plain.Symbols.Bullet)
	}
	require.False(t, marked(m, "checks/notify"))

	m = send(m, tuitest.Key("B"))
	require.Len(t, f.replaced, 1)
	require.Len(t, f.replaced[0].GetBreakpoints(), 1)
	assert.Equal(t, "checks/notify", f.replaced[0].GetBreakpoints()[0].GetStep())
	assert.True(t, marked(m, "checks/notify"), "the breakpoint is not on the node")
	assert.False(t, marked(m, "fetch"), "a breakpoint marked a node it is not on")

	m = send(m, tuitest.Key("B"))
	require.Len(t, f.replaced, 2)
	assert.Empty(t, f.replaced[1].GetBreakpoints(), "the second press did not clear it")
	assert.False(t, marked(m, "checks/notify"))

	// A bare-id breakpoint covers the node, and B removes that one.
	f.breakpoints = []*v1.DebugBreakpointState{{Id: "notify", Verified: true, Definition: &v1.DebugBreakpoint{Id: "notify", Step: "notify"}}}
	m = send(m, tuitest.Key("down"), tuitest.Key("up"))
	m = send(m, frameFrom(t, m, f))
	require.True(t, marked(m, "checks/notify"))
	// An unarmed one and a logpoint are not in force.
	f.breakpoints = []*v1.DebugBreakpointState{
		{Id: "fetch", Verified: false, Definition: &v1.DebugBreakpoint{Id: "fetch", Step: "fetch"}},
		{Id: "log validate", Verified: true, Definition: &v1.DebugBreakpoint{Id: "log validate", Step: "validate", LogMessage: "x"}},
	}
	m = send(m, frameFrom(t, m, f))
	assert.False(t, marked(m, "fetch"))
	assert.False(t, marked(m, "validate"))
}

func TestARightClickTogglesABreakpointOnTheNode(t *testing.T) {
	t.Parallel()

	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram())
	x, y := find(t, m, flowPrefix+"validate")
	m = send(m, tuitest.RightClick(x, y))
	require.Len(t, f.replaced, 1)
	assert.Equal(t, "validate", f.replaced[0].GetBreakpoints()[0].GetStep())
	assert.Empty(t, f.resumes)

	// A right click on nothing does nothing.
	before := len(f.replaced)
	_ = send(m, tuitest.RightClick(0, 0))
	assert.Len(t, f.replaced, before)
}

// ---- the mouse ----

func TestAClickSelectsANodeAndTwoClicksRunUntilIt(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	clock := func(c *Config) { c.Now = func() time.Time { return now } }

	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram(), clock)
	x, y := find(t, m, flowPrefix+"validate")

	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "validate", m.screen.Flow.Selected)
	assert.Equal(t, paneFlow, m.screen.Focus)
	assert.Empty(t, f.resumes, "one click ran the program")

	// A second click in the window is a double click.
	now = now.Add(150 * time.Millisecond)
	m = send(m, tuitest.Click(x, y))
	require.Len(t, f.resumes, 1)
	assert.Equal(t, v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, f.resumes[0].GetAction())
	assert.Equal(t, "validate", f.resumes[0].GetUntil())

	// A third click is the first of the next pair, not a triple.
	x, y = find(t, m, flowPrefix+"validate")
	now = now.Add(100 * time.Millisecond)
	m = send(m, tuitest.Click(x, y))
	assert.Len(t, f.resumes, 1)
}

func TestTwoClicksThatAreNotADoubleClickOnlySelect(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	clock := func(c *Config) { c.Now = func() time.Time { return now } }

	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram(), clock)
	vx, vy := find(t, m, flowPrefix+"validate")
	fx, fy := find(t, m, flowPrefix+"fetch")

	// Too slow.
	m = send(m, tuitest.Click(vx, vy))
	now = now.Add(2 * time.Second)
	m = send(m, tuitest.Click(vx, vy))
	// Two different nodes.
	now = now.Add(10 * time.Millisecond)
	m = send(m, tuitest.Click(fx, fy))
	now = now.Add(10 * time.Millisecond)
	m = send(m, tuitest.Click(vx, vy))
	// The clock going backwards is not a window.
	now = now.Add(-time.Hour)
	m = send(m, tuitest.Click(vx, vy))
	assert.Empty(t, f.resumes)
	assert.Equal(t, "validate", m.screen.Flow.Selected)

	// A screen given no clock sees no double clicks, and says nothing about it.
	noClock := flowModel(t, inLoop(newFake()), flowProgram())
	nf := noClock.cfg.Target.(*fakeTarget)
	x, y := find(t, noClock, flowPrefix+"validate")
	noClock = send(noClock, tuitest.Click(x, y), tuitest.Click(x, y), tuitest.Click(x, y))
	assert.Empty(t, nf.resumes)
	assert.Equal(t, "validate", noClock.screen.Flow.Selected)

	// A click on the ladder's own lines that are not nodes only focuses the pane.
	m = send(m, tuitest.Click(0, 0))
	assert.Empty(t, f.resumes)
}

func TestAClickOnAGroupsMarkFoldsIt(t *testing.T) {
	t.Parallel()

	f := inLoop(newFake())
	m := flowModel(t, f, flowProgram())
	x, y := find(t, m, foldPrefix+"checks")
	m = send(m, tuitest.Click(x, y))
	assert.True(t, m.screen.Flow.Folded["checks"])
	assert.NotContains(t, view(m), "notify")

	x, y = find(t, m, foldPrefix+"checks")
	m = send(m, tuitest.Click(x, y))
	assert.False(t, m.screen.Flow.Folded["checks"])
	assert.Contains(t, view(m), "notify")
	assert.Empty(t, f.resumes)
}

// ---- the screen ----

func TestTheFlowFitsEverySizeInEveryState(t *testing.T) {
	t.Parallel()

	states := map[string]func(*Screen){
		"held in a loop": func(*Screen) {},
		"folded":         func(s *Screen) { s.Flow.Folded["checks"], s.Flow.Folded["pages"] = true, true },
		"scrolled":       func(s *Screen) { s.Flow.Scrolled, s.Flow.Top = true, 4 },
		"selected":       func(s *Screen) { s.Flow.Selected = "checks/audit" },
		"partial":        func(s *Screen) { s.Frame.Partial = true },
		"no program":     func(s *Screen) { s.Frame.Program = nil },
		"unloaded":       func(s *Screen) { s.Loaded = false },
		"flow tab":       func(s *Screen) { s.Pane = paneFlow },
		"steps tab":      func(s *Screen) { s.Pane = paneSteps },
		"help":           func(s *Screen) { s.Help = true },
	}
	for name, mod := range states {
		for _, size := range tuitest.Sizes {
			for _, v := range styles {
				t.Run(name+"/"+size.String()+"/"+v.name, func(t *testing.T) {
					s := flowScreenOf(t, inLoop(newFake()), flowProgram(), size)
					mod(&s)
					text, hits := s.Draw(v.style)
					tuitest.Fits(t, text, size)
					assert.Len(t, lines(text), size.H)
					if size.W < MinWidth || size.H < MinHeight {
						assert.Zero(t, hits.Len())

						return
					}
					if name == "help" {
						return
					}
					// The heading is there, or the pane is a tab.
					shown := false
					for y := range size.H {
						for x := range size.W {
							if hit, ok := hits.At(x, y); ok && (hit.ID == panePrefix+paneFlow || (hit.Kind == pane.KindTab && hit.ID == paneFlow)) {
								shown = true
							}
						}
					}
					assert.True(t, shown, "no flow pane or tab at %v", size)
				})
			}
		}
	}
}

func TestEveryHitTheFlowRegistersIsOnTheScreenAndOnItsOwnLine(t *testing.T) {
	t.Parallel()

	for _, size := range []tui.Size{{W: 80, H: 24}, {W: 100, H: 30}, {W: 120, H: 36}, {W: 200, H: 60}} {
		s := flowScreenOf(t, inLoop(newFake()), flowProgram(), size)
		text, hits := s.Draw(plain)
		rows := lines(text)
		nodes := 0
		for y := range size.H {
			for x := range size.W {
				hit, ok := hits.At(x, y)
				if !ok || hit.Kind != pane.KindNode || !strings.HasPrefix(hit.ID, flowPrefix) {
					continue
				}
				nodes++
				id := strings.TrimPrefix(hit.ID, flowPrefix)
				assert.Contains(t, rows[y], id[strings.LastIndex(id, "/")+1:], "%v: the hit for %s is on another line", size, id)
			}
		}
		assert.Positive(t, nodes, "%v: nothing on the flow could be clicked", size)
	}
}

// TestNothingInTheFlowReadsAClock holds the flow pane to the rule the rest of the
// screen keeps: the only clock it sees is the one it was given.
func TestNothingInTheFlowReadsAClock(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile("graphpane.go")
	require.NoError(t, err)
	for _, banned := range []string{"time.Now(", "time.Since(", "time.Tick(", "time.After("} {
		assert.NotContains(t, string(source), banned)
	}
}

// TestTheScreenWithAFlowGolden pins the whole screen with a program: the flow is
// the left column wide, and above the steps where the terminal is narrower.
func TestTheScreenWithAFlowGolden(t *testing.T) {
	for _, size := range []tui.Size{{W: 80, H: 24}, {W: 120, H: 36}} {
		t.Run(size.String(), func(t *testing.T) {
			s := flowScreenOf(t, inLoop(newFake()), flowProgram(), size)
			s.Console = consoleWith("until pages/store", "debug> step")
			text, _ := s.Draw(plain)
			tuitest.Golden(t, text)
			tuitest.Fits(t, text, size)
		})
	}
}

// TestAProgramThatIsNotTheRunsIsNeverDrawn: a file whose digest is not the one
// the run reports would draw steps the run does not have and aim `until` and
// `break` at them, so the frame carries no program and the pane says so.
func TestAProgramThatIsNotTheRunsIsNeverDrawn(t *testing.T) {
	t.Parallel()

	read := func(t *testing.T, fake *fakeTarget) flowdebug.Frame {
		t.Helper()
		m := flowModel(t, fake, flowProgram())
		msg, ok := m.read(m.readSeq)().(frameMsg)
		require.True(t, ok)
		require.NoError(t, msg.err)

		return msg.frame
	}

	t.Run("a digest that matches draws the program", func(t *testing.T) {
		t.Parallel()

		fake := inLoop(newFake())
		fake.irDigest = v1.WorkflowIRDigest(flowProgram())
		assert.NotNil(t, read(t, fake).Program)
	})
	t.Run("a run that reports none cannot contradict it", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, read(t, inLoop(newFake())).Program)
	})
	t.Run("a digest that differs draws no program and commands refuse", func(t *testing.T) {
		t.Parallel()

		fake := inLoop(newFake())
		fake.irDigest = "sha256:somebody-elses"
		frame := read(t, fake)
		assert.Nil(t, frame.Program)

		m := send(flowModel(t, fake, flowProgram()), tuitest.Key("u"))
		assert.Contains(t, m.screen.Toast.Text(), NoProgramNote)
		assert.Empty(t, fake.resumes, "until was aimed at a step of a program the run is not running")
	})
}
