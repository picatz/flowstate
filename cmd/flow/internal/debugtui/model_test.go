package debugtui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// held is the index in the step window of the step the run is held at.
func held(m Model) int { return m.screen.Frame.Steps.Held }

func TestTheFirstFrameIsReadFromTheTarget(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	require.True(t, m.screen.Loaded)
	assert.Equal(t, uint64(2), m.screen.Frame.Snapshot.GetRevision())
	assert.Equal(t, 1, held(m), "build is the second step")
	assert.Contains(t, view(m), "release-1")
	assert.Contains(t, view(m), "rev 2")
	assert.Contains(t, view(m), "inputs", "the scope was not drawn")
}

func TestStepMovesTheHeldRow(t *testing.T) {
	t.Parallel()

	for key, action := range map[string]v1.DebugResumeAction{
		"s":     v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
		"space": v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
		"n":     v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
		"f":     v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT,
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			m := started(t, fake)
			before := held(m)

			m = send(m, tuitest.Key(key))

			require.Len(t, fake.resumes, 1)
			assert.Equal(t, action, fake.resumes[0].GetAction(), "the key sent a different verb")
			assert.Equal(t, before+1, held(m), "the held row did not move to the next step")
			assert.Equal(t, uint64(3), m.screen.Frame.Snapshot.GetRevision(), "the frame was not read again")
			assert.Empty(t, m.screen.Busy)
			assert.Contains(t, strings.Join(m.screen.Console.Lines(), "\n"), Prompt+resumeVerb(action),
				"the console did not record the line the key stood for")
		})
	}
}

func resumeVerb(action v1.DebugResumeAction) string {
	return map[v1.DebugResumeAction]string{
		v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN:   "step",
		v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER: "next",
		v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT:  "finish",
	}[action]
}

func TestContinueRunsToTheEnd(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := send(started(t, fake), tuitest.Key("c"))
	require.Len(t, fake.resumes, 1)
	assert.Equal(t, v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE, fake.resumes[0].GetAction())
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, m.screen.Frame.Snapshot.GetState())
	assert.Contains(t, view(m), "not held", "a finished run was drawn as held")
}

func TestARefusedVerbToastsTheRefusalAndChangesNothing(t *testing.T) {
	t.Parallel()

	fake := newFake()
	fake.refuse = map[v1.DebugResumeAction]string{v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER: "the run is not at a boundary"}
	m := started(t, fake)
	before := m.screen.Frame.Snapshot.GetRevision()

	m = send(m, tuitest.Key("n"))

	assert.Contains(t, m.screen.Toast.Text(), "the run is not at a boundary")
	assert.Contains(t, view(m), "the run is not at a boundary", "the refusal is not on the screen")
	assert.Equal(t, before, m.screen.Frame.Snapshot.GetRevision())
	assert.Equal(t, 1, held(m), "a refused step moved the held row")
	assert.False(t, m.Done())

	// The toast lasts until the next key, and no longer.
	m = send(m, tuitest.Key("?"))
	assert.False(t, m.screen.Toast.Active())
}

func TestAVerbTheTargetCannotDoIsToastedNotSwallowed(t *testing.T) {
	t.Parallel()

	m := send(started(t, newFake()), tuitest.Key("b"))
	assert.Contains(t, m.screen.Toast.Text(), "cannot step back", "the refusal a plain session gives was lost")
	assert.Equal(t, 1, held(m))
}

func TestOnlyOneCommandRunsAtATime(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := started(t, fake)

	// The first key starts a command whose answer has not arrived.
	next, cmd := m.Update(tuitest.Key("s"))
	m = next.(Model)
	require.NotNil(t, cmd)
	assert.Equal(t, "step", m.screen.Busy)

	next, again := m.Update(tuitest.Key("n"))
	m = next.(Model)
	assert.Nil(t, again, "a second command was started while the first was running")
	assert.Contains(t, m.screen.Toast.Text(), `still running "step"`)
	assert.Contains(t, view(m), "working: step")

	m = send(m, cmd())
	assert.Empty(t, m.screen.Busy)
	assert.Len(t, fake.resumes, 1)
}

func TestTabCyclesTheFocusRing(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	var order []string
	for range 5 {
		order = append(order, m.screen.Focus)
		m = send(m, tuitest.Key("tab"))
	}
	assert.Equal(t, []string{"steps", "scope", "console", "flow", "source"}, order)
	assert.Equal(t, "steps", m.screen.Focus)

	m = send(m, tuitest.Key("shift+tab"))
	assert.Equal(t, "source", m.screen.Focus, "shift+tab did not go back")
}

func TestAClickOnATreeRowTogglesIt(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	require.False(t, m.screen.Tree.Open("g:inputs"))

	x, y := find(t, m, scopePrefix+"g:inputs")
	m = send(m, tuitest.Click(x, y))
	assert.True(t, m.screen.Tree.Open("g:inputs"), "the click did not open the row")
	assert.Equal(t, "scope", m.screen.Focus, "the click did not focus the pane it was in")
	assert.Equal(t, "g:inputs", m.screen.Tree.Selected())
	assert.Contains(t, view(m), "version", "the children are not drawn")

	m = send(m, tuitest.Click(x, y))
	assert.False(t, m.screen.Tree.Open("g:inputs"), "a second click did not close it")

	// A leaf is selected and not toggled.
	m = send(m, tuitest.Click(x, y))
	x, y = find(t, m, scopePrefix+"inputs.region")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "inputs.region", m.screen.Tree.Selected())
	assert.Contains(t, view(m), "eu-west-1", "the inspector does not show the selected row")
}

func TestAClickOutsideAnyHitIsIgnored(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key("tab"))
	before := view(m)
	focus, scroll := m.screen.Focus, m.screen.StepScroll

	_, hits := m.screen.Draw(m.cfg.Style)
	var outside [][2]int
	for y := range m.screen.Size.H {
		for x := range m.screen.Size.W {
			if _, ok := hits.At(x, y); !ok {
				outside = append(outside, [2]int{x, y})
			}
		}
	}
	require.NotEmpty(t, outside, "every cell is a hit, so there is nothing to ignore")

	// Off the screen entirely, and on cells nothing registered.
	outside = append(outside, [2]int{-1, 3}, [2]int{500, 500})
	for _, at := range outside {
		m = send(m, tuitest.Click(at[0], at[1]), tuitest.RightClick(at[0], at[1]), tuitest.Wheel(at[0], at[1], false))
	}

	assert.Equal(t, before, view(m), "a click on nothing changed the screen")
	assert.Equal(t, focus, m.screen.Focus)
	assert.Equal(t, scroll, m.screen.StepScroll)
	assert.False(t, m.screen.Tree.Open("g:inputs"))
}

func TestAClickOnAPaneHeadingFocusesIt(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	x, y := find(t, m, panePrefix+"scope")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "scope", m.screen.Focus)

	x, y = find(t, m, paneConsole)
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "console", m.screen.Focus)

	// The inspector is read-only and takes no focus.
	x, y = find(t, m, panePrefix+"inspector")
	m = send(m, tuitest.Click(x, y))
	assert.Equal(t, "console", m.screen.Focus)
}

func TestTheWheelScrollsThePaneUnderThePointer(t *testing.T) {
	t.Parallel()

	fake := newFake().withBig(150)
	fake.program = nil
	for i := range 60 {
		fake.program = append(fake.program, fmt.Sprintf("step%02d", i))
	}
	fake.at = 30
	m := started(t, fake, func(c *Config) { c.Size = tui.Size{W: 100, H: 24} })

	// The scope tree: open the big group, then wheel over it.
	x, y := find(t, m, scopePrefix+"g:big")
	m = send(m, tuitest.Click(x, y))
	require.True(t, m.screen.Tree.Open("g:big"))
	require.Zero(t, m.screen.Tree.Top())

	m = send(m, tuitest.Wheel(x, y, false))
	assert.Equal(t, wheelRows, m.screen.Tree.Top(), "the wheel did not scroll the tree")
	m = send(m, tuitest.Wheel(x, y, true))
	assert.Zero(t, m.screen.Tree.Top())
	m = send(m, tuitest.Wheel(x, y, true))
	assert.Zero(t, m.screen.Tree.Top(), "scrolled above the first row")

	// The steps pane has its own scroll, and a wheel over it moves that and not the tree's.
	sx, sy := find(t, m, panePrefix+"steps")
	before := view(m)
	m = send(m, tuitest.Wheel(sx, sy+2, false))
	assert.Equal(t, wheelRows, m.screen.StepScroll)
	assert.NotEqual(t, before, view(m))
	assert.Zero(t, m.screen.Tree.Top())

	for range 100 {
		m = send(m, tuitest.Wheel(sx, sy+2, false))
	}
	_, hi := StepScrollRange(m.screen.Frame, m.stepRows(), m.cfg.Style)
	assert.Equal(t, hi, m.screen.StepScroll, "scrolled past the last step")
}

func TestKeysMoveThroughTheScope(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key("tab"))
	require.Equal(t, "scope", m.screen.Focus)
	assert.Equal(t, "g:inputs", m.screen.Tree.Selected())

	m = send(m, tuitest.Key("enter"))
	assert.True(t, m.screen.Tree.Open("g:inputs"))
	m = send(m, tuitest.Key("j"), tuitest.Key("down"))
	assert.Equal(t, "inputs.region", m.screen.Tree.Selected())
	m = send(m, tuitest.Key("h"))
	assert.Equal(t, "g:inputs", m.screen.Tree.Selected(), "left on a leaf goes to its parent")
	m = send(m, tuitest.Key("h"))
	assert.False(t, m.screen.Tree.Open("g:inputs"))
	m = send(m, tuitest.Key("l"))
	assert.True(t, m.screen.Tree.Open("g:inputs"))
	m = send(m, tuitest.Key("G"))
	assert.Equal(t, "g:run", m.screen.Tree.Selected())
	m = send(m, tuitest.Key("home"))
	assert.Equal(t, "g:inputs", m.screen.Tree.Selected())

	// Navigation keys do nothing to the run.
	assert.Empty(t, m.cfg.Target.(*fakeTarget).resumes)
}

func TestAMorePageIsLoadedByAClickAndOnlyForTheStopItWasAskedAt(t *testing.T) {
	t.Parallel()

	fake := newFake().withBig(300)
	m := started(t, fake)
	x, y := find(t, m, scopePrefix+"g:big")
	m = send(m, tuitest.Click(x, y))

	node, _ := m.screen.Tree.Node("g:big")
	resolved := flowdebug.MaxFrameValues - 4 // the other groups spent four of the read's budget
	require.Len(t, node.Children, resolved, "a read resolves at most %d values", flowdebug.MaxFrameValues)
	require.Equal(t, 300, node.Total)

	m = send(m, tuitest.Key("G"))
	mx, my := find(t, m, scopePrefix+"more:g:big")
	require.NotZero(t, my)
	m = send(m, tuitest.Click(mx, my))

	node, _ = m.screen.Tree.Node("g:big")
	assert.Len(t, node.Children, resolved+pageSize, "one page was not appended")
	last := fake.inspects[len(fake.inspects)-1]
	assert.Equal(t, "@scope:big", last.GetExpression())
	assert.EqualValues(t, resolved, last.GetOffset())
	assert.Equal(t, fake.revision(), last.GetRevision(), "the page was not asked at the stop it is for")

	// A page that arrives after the run moved is refused as stale by the target and
	// appended to nothing.
	fake.advance()
	before := len(node.Children)
	m = send(m, pageCmdOf(m, pane.Request{Parent: "g:big", Offset: before})())
	node, _ = m.screen.Tree.Node("g:big")
	assert.Len(t, node.Children, before)
}

// pageCmdOf is the command a "… more" click would start.
func pageCmdOf(m Model, req pane.Request) tea.Cmd { return m.pageCmd(req) }

func TestAHelpOverlayListsOnlyWhatTheFrontAnswers(t *testing.T) {
	t.Parallel()

	// Tall enough to show the whole overlay, which grows with each key the table earns.
	m := started(t, newFake(), func(c *Config) { c.Size = tui.Size{W: 120, H: 50} })
	m = send(m, tuitest.Key("?"))
	require.True(t, m.screen.Help)
	help := view(m)
	for _, want := range []string{"step", "next", "continue", "back", "until <step>", "break", "inspect", "Type in the console"} {
		assert.Contains(t, help, want)
	}
	assert.NotContains(t, help, "\n  quit", "a verb the driver front refuses is taught")
	assert.NotContains(t, help, "info")

	// While it is open, keys are not commands.
	fake := m.cfg.Target.(*fakeTarget)
	m = send(m, tuitest.Key("s"))
	assert.Empty(t, fake.resumes)
	m = send(m, tuitest.Key("esc"))
	assert.False(t, m.screen.Help)

	// A front that does not answer a verb has no key for it and does not teach it.
	var verbs []flowdebug.Verb
	for _, verb := range flowdebug.DriverVerbs() {
		if verb.Name != "back" && verb.Name != "reverse-continue" && verb.Name != "pause" {
			verbs = append(verbs, verb)
		}
	}
	fewer := started(t, newFake(), func(c *Config) { c.Verbs = verbs })
	fewer = send(fewer, tuitest.Key("?"))
	text := view(fewer)
	assert.NotContains(t, text, "back")
	assert.NotContains(t, text, "reverse")
	assert.NotContains(t, text, "pause")
	assert.Contains(t, text, "continue")

	fewer = send(fewer, tuitest.Key("esc"), tuitest.Key("b"), tuitest.Key("r"), tuitest.Key("p"))
	assert.Empty(t, fewer.cfg.Target.(*fakeTarget).resumes)
	assert.Empty(t, fewer.screen.Console.Lines(), "a key with no verb behind it sent something")
	assert.False(t, fewer.screen.Toast.Active())
}

func TestAClickClosesTheHelp(t *testing.T) {
	t.Parallel()

	m := send(started(t, newFake()), tuitest.Key("?"))
	m = send(m, tuitest.Click(5, 5))
	assert.False(t, m.screen.Help)
}

func TestTheConsoleRunsTypedLinesThroughTheDriver(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := started(t, fake)
	m = send(m, tuitest.Key(":"))
	require.Equal(t, "console", m.screen.Focus)
	assert.Empty(t, m.screen.Console.Text, "the key that opened the console was typed into it")

	m = send(m, append(tuitest.Keys("inspect inputs.version"), tuitest.Key("enter"))...)
	assert.Contains(t, strings.Join(m.screen.Console.Lines(), "\n"), `"2026.9.0"`, "the answer is not in the transcript")
	assert.Empty(t, m.screen.Console.Text)
	assert.Equal(t, "console", m.screen.Focus)

	// A typed verb is the same call as its key.
	m = send(m, append(tuitest.Keys("next"), tuitest.Key("enter"))...)
	require.Len(t, fake.resumes, 1)
	assert.Equal(t, v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER, fake.resumes[0].GetAction())

	// A line the driver does not know is an error the person can read, and the
	// session stays open.
	m = send(m, append(tuitest.Keys("frobnicate"), tuitest.Key("enter"))...)
	assert.Contains(t, m.screen.Toast.Text(), "unknown command")
	assert.False(t, m.Done())

	// Up recalls, and esc leaves the console for the pane.
	m = send(m, tuitest.Key("up"))
	assert.Equal(t, "frobnicate", m.screen.Console.Text)
	m = send(m, tuitest.Key("ctrl+u"), tuitest.Key("esc"))
	assert.Equal(t, "steps", m.screen.Focus)
}

func TestTheInspectKeyOpensTheConsoleOnTheSelectedName(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key("tab"), tuitest.Key("i"))
	assert.Equal(t, "scope", m.screen.Focus, "a group has no expression to inspect")
	assert.Contains(t, m.screen.Toast.Text(), "select a name")

	m = send(m, tuitest.Key("enter"), tuitest.Key("j"), tuitest.Key("i"))
	assert.Equal(t, "console", m.screen.Focus)
	assert.Equal(t, "inspect inputs.version", m.screen.Console.Text)
}

func TestTabCompletesInTheConsole(t *testing.T) {
	t.Parallel()

	m := started(t, newFake())
	m = send(m, tuitest.Key(":"))
	m = send(m, tuitest.Keys("unt")...)
	m = send(m, tuitest.Key("tab"))
	assert.Equal(t, "until ", m.screen.Console.Text, "the single offer was not applied")
	assert.Empty(t, m.screen.Busy)

	// Several offers are applied as far as they agree, and offered in the menu.
	m.screen.Console.Clear()
	m = send(m, tuitest.Keys("co")...)
	m = send(m, tuitest.Key("tab"))
	assert.Equal(t, "co", m.screen.Console.Text)
	menu, open := m.screen.Console.Menu()
	require.True(t, open)
	offered := make([]string, len(menu.Candidates))
	for i, c := range menu.Candidates {
		offered[i] = c.Text
	}
	assert.Equal(t, []string{"continue", "complete "}, offered)
}

func TestApplyingACompletion(t *testing.T) {
	t.Parallel()

	one := flowdebug.Completion{Prefix: "inp", Candidates: []flowdebug.Candidate{{Text: "inputs", Continues: true}}}
	line, offers := applyCompletion("inspect inp", one)
	assert.Equal(t, "inspect inputs", line, "a name that continues is not followed by a space")
	assert.Empty(t, offers)

	many := flowdebug.Completion{Prefix: "st", Candidates: []flowdebug.Candidate{{Text: "step"}, {Text: "steps"}, {Text: "status"}}}
	line, offers = applyCompletion("st", many)
	assert.Equal(t, "st", line, "the offers share nothing beyond what is typed")
	assert.Equal(t, []string{"step", "steps", "status"}, offers)

	many.Candidates = many.Candidates[:2]
	line, _ = applyCompletion("st", many)
	assert.Equal(t, "step", line)

	line, offers = applyCompletion("different", one)
	assert.Equal(t, "different", line, "a completion for another line was applied")
	assert.Empty(t, offers)

	line, _ = applyCompletion("x", flowdebug.Completion{})
	assert.Equal(t, "x", line)
}

func TestHowTheScreenEnds(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		keys    []string
		outcome Outcome
		resumes int
		done    bool
	}{
		"q detaches the run":                 {keys: []string{"q"}, outcome: OutcomeDetach, resumes: 1, done: true},
		"a typed quit is the same":           {keys: []string{":", "quit", "enter"}, outcome: OutcomeDetach, resumes: 1, done: true},
		"a typed disconnect leaves attached": {keys: []string{":", "disconnect", "enter"}, outcome: OutcomeDisconnect, done: true},
		"ctrl+c interrupts at once":          {keys: []string{"ctrl+c"}, outcome: OutcomeInterrupt, done: true},
		"ctrl+c in the console too":          {keys: []string{":", "ctrl+c"}, outcome: OutcomeInterrupt, done: true},
		"ctrl+d leaves":                      {keys: []string{"ctrl+d"}, outcome: OutcomeLeave, done: true},
		"ctrl+d in an empty console leaves":  {keys: []string{":", "ctrl+d"}, outcome: OutcomeLeave, done: true},
		"ctrl+d in a typed line does not":    {keys: []string{":", "s", "ctrl+d"}, done: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			m := started(t, fake)
			for _, key := range test.keys {
				if len(key) > 1 && !strings.ContainsAny(key, "+") && key != "enter" {
					m = send(m, tuitest.Keys(key)...)

					continue
				}
				m = send(m, tuitest.Key(key))
			}
			assert.Equal(t, test.done, m.Done())
			if test.done {
				assert.Equal(t, test.outcome, m.Outcome())
			}
			require.Len(t, fake.resumes, test.resumes)
			if test.resumes > 0 {
				assert.Equal(t, v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH, fake.resumes[0].GetAction())
			}
		})
	}
}

func TestADetachTheRunRefusesDoesNotEndTheScreen(t *testing.T) {
	t.Parallel()

	fake := newFake()
	fake.refuse = map[v1.DebugResumeAction]string{v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH: "another controller holds the run"}
	m := send(started(t, fake), tuitest.Key("q"))
	assert.False(t, m.Done(), "the screen ended on a run it did not release")
	assert.Contains(t, m.screen.Toast.Text(), "another controller holds the run")
}

func TestQuittingAnEndedRunSendsNothing(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := send(started(t, fake), tuitest.Key("c"))
	require.Len(t, fake.resumes, 1)

	m = send(m, tuitest.Key("q"))
	assert.True(t, m.Done())
	assert.Equal(t, OutcomeEnded, m.Outcome())
	assert.Len(t, fake.resumes, 1, "a detach was sent to a run that is over")
}

func TestAcceptedLinesAreReported(t *testing.T) {
	t.Parallel()

	var got []string
	fake := newFake()
	fake.refuse = map[v1.DebugResumeAction]string{v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER: "no"}
	m := started(t, fake, func(c *Config) { c.Accepted = func(line string) { got = append(got, line) } })
	send(m, tuitest.Key("s"), tuitest.Key("n"), tuitest.Key(":"))
	assert.Equal(t, []string{"step"}, got, "a refused line was recorded as done")
}

func TestAnOldReadDoesNotReplaceANewerOne(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := started(t, fake)
	newer := m.screen.Frame

	next, _ := m.Update(frameMsg{seq: m.readSeq - 1, frame: frameOf(t, newFake(), true)})
	m = next.(Model)
	assert.Equal(t, newer.Snapshot.GetRevision(), m.screen.Frame.Snapshot.GetRevision())

	next, _ = m.Update(frameMsg{seq: m.readSeq, err: errors.New("the server went away")})
	m = next.(Model)
	assert.Contains(t, m.screen.Toast.Text(), "the server went away")
	assert.True(t, m.screen.Loaded, "a failed read blanked the last good frame")
	assert.Equal(t, newer.Snapshot.GetRevision(), m.screen.Frame.Snapshot.GetRevision())
}

// ---- following the run ----

// waiting runs a wait command on a goroutine and returns what it answers.
func waiting(cmd tea.Cmd) <-chan tea.Msg {
	out := make(chan tea.Msg, 1)
	go func() { out <- cmd() }()

	return out
}

func TestTheScreenFollowsTheTargetsRevisionsWithoutAClock(t *testing.T) {
	fake := newFake()
	m := modelFor(t, fake, func(c *Config) { c.Watch = true })

	// The first frame arms the wait.
	next, wait := m.Update(m.Init()())
	m = next.(Model)
	require.NotNil(t, wait)
	require.True(t, m.watching)

	pending := waiting(wait)
	select {
	case <-pending:
		t.Fatal("the wait returned before the run moved")
	default:
	}

	// The run moves on its own, with no key pressed.
	fake.advance()
	msg := <-pending
	next, cmd := m.Update(msg)
	m = next.(Model)
	require.NotNil(t, cmd)

	// It reads the new frame and waits again.
	next, again := m.Update(framesOf(t, cmd))
	m = next.(Model)
	assert.Nil(t, again, "the wait was already armed alongside the read")
	assert.Equal(t, uint64(3), m.screen.Frame.Snapshot.GetRevision(), "the new stop was not read")
	assert.Contains(t, view(m), "rev 3")
	assert.True(t, m.watching)
}

// framesOf runs the commands of a batch concurrently and returns the first
// message that is a frame: the wait the batch also holds stays blocked, as it
// does in a program, until the test moves the run.
func framesOf(t *testing.T, cmd tea.Cmd) tea.Msg {
	t.Helper()

	batch, ok := cmd().(tea.BatchMsg)
	require.True(t, ok, "the model did not ask for a read and a wait")
	out := make(chan tea.Msg, len(batch))
	for _, c := range batch {
		go func() { out <- c() }()
	}
	for range batch {
		msg := <-out
		if _, isFrame := msg.(frameMsg); isFrame {
			return msg
		}
	}
	require.Fail(t, "no frame was read")

	return nil
}

func TestAWaitThatStopsAdvancingIsGivenUp(t *testing.T) {
	t.Parallel()

	m := modelFor(t, newFake(), func(c *Config) { c.Watch = true })
	next, _ := m.Update(m.Init()())
	m = next.(Model)
	require.True(t, m.watching)

	// A target whose wait returns at once with the revision it was asked past.
	same := &v1.DebugSnapshot{Revision: m.frameRev, State: v1.DebugRunState_DEBUG_RUN_STATE_HELD}
	var cmd tea.Cmd
	for i := range stallLimit {
		next, cmd = m.Update(watchMsg{after: m.frameRev, snap: same})
		m = next.(Model)
		if i < stallLimit-1 {
			require.NotNil(t, cmd, "gave up after %d stalls", i+1)
		}
	}
	assert.False(t, m.watching, "a wait that never advances was re-armed forever")
	assert.Nil(t, cmd)
}

func TestALostWaitIsToastedAndStopsFollowing(t *testing.T) {
	t.Parallel()

	m := modelFor(t, newFake(), func(c *Config) { c.Watch = true })
	next, _ := m.Update(m.Init()())
	m = next.(Model)

	next, cmd := m.Update(watchMsg{after: 2, err: errors.New("connection reset")})
	m = next.(Model)
	assert.Nil(t, cmd)
	assert.False(t, m.watching)
	assert.Contains(t, m.screen.Toast.Text(), "connection reset")

	// A wait that merely ran out its own deadline is a quiet run, asked again.
	m.watching = true
	_, cmd = m.Update(watchMsg{after: 2, err: context.DeadlineExceeded})
	assert.NotNil(t, cmd)
}

func TestAnEndedRunIsNotWaitedOn(t *testing.T) {
	t.Parallel()

	m := modelFor(t, newFake(), func(c *Config) { c.Watch = true })
	next, _ := m.Update(m.Init()())
	m = next.(Model)

	done := &v1.DebugSnapshot{Revision: 9, State: v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED}
	next, cmd := m.Update(watchMsg{after: 2, snap: done})
	m = next.(Model)
	assert.False(t, m.watching)
	require.NotNil(t, cmd, "the final state was not read")
	_, isBatch := cmd().(tea.BatchMsg)
	assert.False(t, isBatch, "a wait was armed on a run that is over")
}

func TestTheScreenWithoutWatchNeverWaits(t *testing.T) {
	t.Parallel()

	fake := newFake()
	m := modelFor(t, fake)
	next, cmd := m.Update(m.Init()())
	m = next.(Model)
	assert.Nil(t, cmd)
	assert.False(t, m.watching)
}

func TestAScreenNeedsATargetAndADriver(t *testing.T) {
	t.Parallel()

	_, err := New(t.Context(), Config{})
	require.Error(t, err)
}
