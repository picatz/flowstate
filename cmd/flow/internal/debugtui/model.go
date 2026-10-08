package debugtui

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	tea "charm.land/bubbletea/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Outcome is how the screen ended, which is what the caller releases the run
// by.
type Outcome uint8

const (
	// OutcomeLeave: the person left without a verb (ctrl-D). The caller releases
	// the run, as the end of input does at a prompt.
	OutcomeLeave Outcome = iota
	// OutcomeInterrupt: ctrl-C. The caller ends the session as it does for an
	// interrupted prompt.
	OutcomeInterrupt
	// OutcomeDetach: `detach` (or quit) was sent and the run took it.
	OutcomeDetach
	// OutcomeDisconnect: `disconnect`, which leaves the session attached for a
	// later rejoin.
	OutcomeDisconnect
	// OutcomeEnded: the run is over, and so is the session.
	OutcomeEnded
)

// Config is what a screen is opened over.
type Config struct {
	// Target is read for frames and waited on for the next stop, and Driver
	// carries every command to it. They are the same session.
	Target flowdebug.Target
	Driver *flowdebug.Driver

	// Frame says what a read adds to the target's answers: the program and its
	// step list.
	Frame flowdebug.FrameOptions

	// Verbs is what the front answers; empty means [flowdebug.DriverVerbs].
	Verbs []flowdebug.Verb

	Style Style

	// Size is the terminal's size until the first resize message says otherwise.
	Size tui.Size

	// Watch has the screen wait on the target for the next revision. A test
	// that drives messages by hand leaves it off.
	Watch bool

	// Accepted is called with each line the run took, for a recording.
	Accepted func(line string)

	// Now is the clock a double click is judged by. The screen reads no clock of
	// its own; a screen given none never sees a double click, and a click then
	// only selects.
	Now func() time.Time
}

// Bounds on work the screen starts.
const (
	// readTimeout bounds one frame read: a snapshot, a scope listing and at most
	// [flowdebug.MaxFrameValues] evaluations.
	readTimeout = 15 * time.Second
	// completeTimeout bounds one completion, as the line editor's is.
	completeTimeout = 2 * time.Second
	// stepRowsAsked is the step window a read asks for; the pane shows the part
	// that fits.
	stepRowsAsked = 48
	// stallLimit is how many waits in a row may return without a newer revision
	// before the screen stops waiting.
	stallLimit = 3
	// wheelRows is how far one notch of the wheel scrolls.
	wheelRows = 3
)

// busyCompleting is what the screen is "running" while the driver is asked for
// a completion, which uses the driver as a command does.
const busyCompleting = "completion"

// Messages the screen's commands answer with.
type (
	frameMsg struct {
		seq   uint64
		frame flowdebug.Frame
		err   error
	}
	doneMsg struct {
		line   string
		result *flowdebug.DriveResult
		err    error
	}
	watchMsg struct {
		after uint64
		snap  *v1.DebugSnapshot
		err   error
	}
	pageMsg struct {
		rev   uint64
		req   pane.Request
		nodes []pane.Node
		total int
		err   error
	}
	completeMsg struct {
		line   string
		answer flowdebug.Completion
		err    error
	}
)

// Model is the debugger screen.
type Model struct {
	cfg  Config
	ctx  context.Context
	keys tui.Keymap

	screen Screen
	ring   tui.Ring

	// readSeq numbers frame reads; only the latest is applied, so a slow read
	// cannot overwrite a newer one.
	readSeq uint64

	// reading is a frame read in flight, and dirty that the run moved while it
	// was: one read at a time, one more after it, however fast revisions come.
	reading, dirty bool
	frameRev       uint64

	watching bool
	stalled  int

	quitting bool
	outcome  Outcome
}

// New returns the screen over cfg. ctx bounds every command it starts.
func New(ctx context.Context, cfg Config) (Model, error) {
	if cfg.Target == nil || cfg.Driver == nil {
		return Model{}, errors.New("debugtui: a screen needs a target and a driver")
	}
	if len(cfg.Verbs) == 0 {
		cfg.Verbs = flowdebug.DriverVerbs()
	}
	keys, err := NewKeymap(cfg.Verbs)
	if err != nil {
		return Model{}, err
	}

	ring := tui.NewRing(paneFlow, paneSteps, paneScope, paneConsole)
	focus := paneSteps
	if cfg.Frame.Program != nil {
		// With a program the flow is the first thing to look at; without one it is
		// a sentence, and keys belong to the steps.
		focus = paneFlow
	}
	ring = ring.Set(focus)

	return Model{
		cfg:     cfg,
		ctx:     ctx,
		keys:    keys,
		ring:    ring,
		readSeq: 1,
		reading: true,
		screen: Screen{
			Size: cfg.Size, Tree: pane.NewTree(nil), Console: NewConsole(), Keys: keys, Verbs: cfg.Verbs,
			Focus: ring.Current(), Pane: focus, Diverged: map[uint64]bool{}, Flow: NewFlow(),
		},
	}, nil
}

// Outcome is how the screen ended; meaningful once [Model.Done].
func (m Model) Outcome() Outcome { return m.outcome }

// Done reports that the screen has asked to quit.
func (m Model) Done() bool { return m.quitting }

// Screen is the state the screen draws.
func (m Model) Screen() Screen { return m.screen }

// Init reads the first frame.
func (m Model) Init() tea.Cmd { return m.read(m.readSeq) }

// View draws the screen.
func (m Model) View() tea.View {
	text, _ := m.screen.Draw(m.cfg.Style)
	view := tea.NewView(text)
	view.AltScreen = true
	view.MouseMode = tea.MouseModeCellMotion
	view.WindowTitle = "flow debug"

	return view
}

// Update folds one message in.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.screen.Size = tui.Size{W: msg.Width, H: msg.Height}
		m.revealSelection()

		return m, nil

	case tea.KeyPressMsg:
		return m.key(msg)

	case tea.MouseClickMsg:
		return m.click(tea.Mouse(msg))

	case tea.MouseWheelMsg:
		return m.wheel(tea.Mouse(msg))

	case tea.InterruptMsg:
		return m.leave(OutcomeInterrupt)

	case frameMsg:
		return m.framed(msg)

	case doneMsg:
		return m.done(msg)

	case watchMsg:
		return m.watched(msg)

	case pageMsg:
		return m.paged(msg)

	case completeMsg:
		return m.completed(msg)
	}

	return m, nil
}

// leave ends the screen with an outcome.
func (m Model) leave(outcome Outcome) (tea.Model, tea.Cmd) {
	m.quitting, m.outcome = true, outcome

	return m, tea.Quit
}

// toast shows a refusal or a problem until the next key press.
func (m *Model) toast(tone ui.Tone, text string) {
	m.screen.Toast = m.screen.Toast.Show(tone, text)
}

// ended reports that the run's session is over.
func (m Model) ended() bool {
	switch m.screen.Frame.Snapshot.GetState() {
	case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		return true
	default:
		return false
	}
}

// ---- commands the screen starts ----

// read reads one frame.
func (m Model) read(seq uint64) tea.Cmd {
	ctx, target, opts := m.ctx, m.cfg.Target, m.cfg.Frame
	opts.StepRows = stepRowsAsked

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(ctx, readTimeout)
		defer cancel()
		frame, err := flowdebug.ReadFrame(ctx, target, opts)

		return frameMsg{seq: seq, frame: frame, err: err}
	}
}

// reread asks for a fresh read. One runs at a time: a run whose revisions come
// faster than a read completes costs one more read afterwards, not one each.
func (m *Model) reread() tea.Cmd {
	if m.reading {
		m.dirty = true

		return nil
	}
	m.reading = true
	m.readSeq++

	return m.read(m.readSeq)
}

// wait waits on the target for a revision after the one given.
func (m Model) wait(after uint64) tea.Cmd {
	ctx, target := m.ctx, m.cfg.Target

	return func() tea.Msg {
		snap, err := target.WaitSnapshot(ctx, after)

		return watchMsg{after: after, snap: snap, err: err}
	}
}

// run sends one line through the driver. Only one runs at a time: the driver
// keeps state between lines, and a movement can wait a long while.
func (m *Model) run(line string) tea.Cmd {
	if m.screen.Busy != "" {
		m.toast(ui.ToneWarning, fmt.Sprintf("still running %q; ctrl+c leaves", m.screen.Busy))

		return nil
	}
	m.screen.Busy = line
	if m.moves(line) {
		// The run is about to be somewhere else, and the view goes with it.
		m.screen.Flow.Follow()
	}
	driver, ctx := m.cfg.Driver, m.ctx

	return func() tea.Msg {
		result, err := driver.Do(ctx, line)

		return doneMsg{line: line, result: result, err: err}
	}
}

// ---- messages from commands ----

func (m Model) framed(msg frameMsg) (tea.Model, tea.Cmd) {
	if msg.seq != m.readSeq {
		return m, nil
	}
	m.reading = false
	if msg.err != nil {
		if m.ctx.Err() != nil {
			return m, nil
		}
		m.screen.Problem = ui.EscapeControl(msg.err.Error())
		m.toast(ui.ToneDanger, "cannot read the run: "+msg.err.Error())
		m.dirty = false

		return m, nil
	}

	m.screen.Frame, m.screen.Loaded, m.screen.Problem = msg.frame, true, ""
	m.frameRev = msg.frame.Snapshot.GetRevision()
	m.screen.Tree.SetRoots(ScopeNodes(msg.frame))
	m.screen.Flow.Apply(msg.frame)
	m.revealSelection()

	var cmds []tea.Cmd
	if m.dirty {
		m.dirty = false
		cmds = append(cmds, m.reread())
	}
	if m.cfg.Watch && !m.watching && !m.ended() {
		m.watching = true
		cmds = append(cmds, m.wait(m.frameRev))
	}

	return m, tea.Batch(cmds...)
}

func (m Model) watched(msg watchMsg) (tea.Model, tea.Cmd) {
	if m.ctx.Err() != nil {
		return m, nil
	}
	if msg.err != nil && msg.snap == nil {
		// A wait that ran out its own deadline is a quiet run, not a lost one.
		if errors.Is(msg.err, context.DeadlineExceeded) {
			return m, m.wait(msg.after)
		}
		m.watching = false
		m.toast(ui.ToneWarning, "no longer following the run: "+msg.err.Error())

		return m, nil
	}

	var cmds []tea.Cmd
	if rev := msg.snap.GetRevision(); rev > m.frameRev {
		m.stalled = 0
		cmds = append(cmds, m.reread())
	} else if rev <= msg.after {
		m.stalled++
	}
	switch state := msg.snap.GetState(); {
	case m.stalled >= stallLimit:
		m.watching = false
	case state == v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, state == v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		state == v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, state == v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		m.watching = false
	default:
		cmds = append(cmds, m.wait(max(msg.snap.GetRevision(), msg.after)))
	}

	return m, tea.Batch(cmds...)
}

func (m Model) done(msg doneMsg) (tea.Model, tea.Cmd) {
	m.screen.Busy = ""
	m.screen.Console.Say(Prompt + msg.line)

	if msg.err != nil {
		if m.ctx.Err() != nil {
			return m, nil
		}
		m.screen.Console.Say(msg.err.Error())
		m.toast(ui.ToneDanger, msg.err.Error())

		cmd := m.reread()

		return m, cmd
	}

	result := msg.result
	m.screen.Console.Say(result.Text)

	if receipt := result.Receipt; receipt != nil && !flowdebug.Accepted(receipt) {
		if point, ok := gotoPoint(msg.line); ok && receipt.GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DIVERGED {
			// Nothing moved. The point is marked, and the run's own words say
			// why: it is not deterministic, so there is no going back to it.
			timeline := m.screen.Frame.Snapshot.GetTimeline()
			m.screen.Diverged[uint64(timeline.GetDropped())+uint64(point)] = true
			m.toast(ui.ToneDanger, fmt.Sprintf("the run is not deterministic, so point %d is not reachable: %s", point, strings.TrimSpace(receipt.GetMessage())))

			cmd := m.reread()

			return m, cmd
		}
		m.toast(ui.ToneWarning, "the command was not applied: "+strings.TrimSpace(flowdebug.FormatReceipt(receipt)))

		cmd := m.reread()

		return m, cmd
	}
	if flowdebug.StepsBack(msg.line) && result.Snapshot != nil {
		// The transcript is never rewound: a travel is one more thing that happened.
		before := int(m.screen.Frame.Snapshot.GetTimeline().GetCurrent())
		after := int(result.Snapshot.GetTimeline().GetCurrent())
		m.screen.Console.Say(travelNote(result.Snapshot, before, after, pane.Options{Symbols: m.cfg.Style.Symbols}))
	}
	if state := result.Unarmed; state != nil {
		m.toast(ui.ToneWarning, fmt.Sprintf("the breakpoint %s was not armed: %s", state.GetId(), state.GetMessage()))
	}
	if m.cfg.Accepted != nil && (result.Receipt == nil || flowdebug.Accepted(result.Receipt)) && result.Unarmed == nil {
		m.cfg.Accepted(msg.line)
	}

	if msg.line == "detach" && flowdebug.Accepted(result.Receipt) {
		return m.leave(OutcomeDetach)
	}

	cmd := m.reread()

	return m, cmd
}

func (m Model) paged(msg pageMsg) (tea.Model, tea.Cmd) {
	if msg.err != nil {
		m.toast(ui.ToneWarning, "cannot load more: "+msg.err.Error())

		return m, nil
	}
	if msg.rev != m.frameRev {
		// A page of the stop before this one is not appended to this one's tree.
		return m, nil
	}
	m.screen.Tree.Fill(msg.req.Parent, msg.req.Offset, msg.nodes, msg.total)

	return m, nil
}

func (m Model) completed(msg completeMsg) (tea.Model, tea.Cmd) {
	if m.screen.Busy == busyCompleting {
		m.screen.Busy = ""
	}
	if msg.err != nil || msg.line != m.screen.Console.Text {
		// An answer to a line that has since changed is not applied to the new one.
		return m, nil
	}
	// A candidate too long to be part of a command is dropped before any work is
	// done with it.
	msg.answer.Candidates = slices.DeleteFunc(slices.Clone(msg.answer.Candidates), func(c flowdebug.Candidate) bool {
		return len(c.Text) > flowdebug.MaxCommandBytes
	})
	line, offers := applyCompletion(msg.line, msg.answer)
	// A candidate is the target's text: one with a control character, or one
	// that would outgrow a command, is not put on the line.
	if strings.ContainsFunc(line, unicode.IsControl) || len(line) > flowdebug.MaxCommandBytes {
		line = msg.line
	}
	m.screen.Console.Text = line
	if len(offers) > 0 {
		m.screen.Console.Say(strings.Join(offers, "  "))
	}

	return m, nil
}

// applyCompletion applies a completion to the end of line: a single offer in
// full, several as far as they agree. The offers left to choose between are
// returned for the transcript.
func applyCompletion(line string, c flowdebug.Completion) (string, []string) {
	if len(c.Candidates) == 0 {
		return line, nil
	}
	base := strings.TrimSuffix(line, c.Prefix)
	if base+c.Prefix != line {
		return line, nil
	}

	if len(c.Candidates) == 1 {
		candidate := c.Candidates[0]
		if !candidate.Continues {
			return base + candidate.Text + " ", nil
		}

		return base + candidate.Text, nil
	}

	common := c.Candidates[0].Text
	for _, candidate := range c.Candidates[1:] {
		for !strings.HasPrefix(candidate.Text, common) {
			common = common[:len(common)-1]
		}
	}
	if len(common) > len(c.Prefix) {
		line = base + common
	}
	var offers []string
	for _, candidate := range c.Candidates[:min(len(c.Candidates), 8)] {
		offers = append(offers, candidate.Text)
	}
	if len(c.Candidates) > 8 || c.Truncated {
		offers = append(offers, "…")
	}

	return line, offers
}

// ---- tree paging ----

// pageSize is how many children one page asks for.
const pageSize = 50

// pageCmd loads one page of a node's children from the target, at the revision
// the screen last read: the run having moved since is the target's to say, and
// the page is then not appended to a tree of another stop.
func (m Model) pageCmd(req pane.Request) tea.Cmd {
	expression := req.Parent
	if group, ok := strings.CutPrefix(expression, "g:"); ok {
		expression = "@scope:" + group
	}
	target, ctx, revision := m.cfg.Target, m.ctx, m.frameRev

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(ctx, readTimeout)
		defer cancel()
		answer, err := target.Inspect(ctx, &v1.DebugInspectRequest{
			Revision: revision, Expression: expression, Children: true, Offset: int32(req.Offset), Limit: pageSize,
		})
		if err != nil {
			return pageMsg{rev: revision, req: req, err: err}
		}
		if reason := answer.GetError(); reason != "" {
			return pageMsg{rev: revision, req: req, err: errors.New(reason)}
		}
		nodes := make([]pane.Node, 0, len(answer.GetChildren()))
		for _, child := range answer.GetChildren() {
			value := child.GetValue()
			nodes = append(nodes, pane.Node{
				ID:    value.GetExpression(),
				Label: child.GetName(),
				Value: cutValue(value.GetRendered()),
				Total: int(value.GetChildren()),
			})
		}

		return pageMsg{rev: revision, req: req, nodes: nodes, total: int(answer.GetTotal())}
	}
}

// completeCmd asks the driver what could be written at the end of line.
func (m Model) completeCmd(line string) tea.Cmd {
	driver, ctx := m.cfg.Driver, m.ctx

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(ctx, completeTimeout)
		defer cancel()
		answer, err := driver.Complete(ctx, line)

		return completeMsg{line: line, answer: answer, err: err}
	}
}
