package watch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The live shape of `flow watch`: the same [State] the plain shape folds
// into, redrawn in place instead of appended to.
//
// Two things make this worth the machinery over reprinting. A view that
// updates in place puts the change where the eye already is, rather than
// below where it was looking. And it can show what a printed line cannot:
// elapsed time, which is the difference between a run that is working and a
// watch that has frozen. A still screen with a number moving on it is
// legible; a still screen is not.
//
// What it draws while a run is going is where the run is — the step, and
// the path into it — beside what Temporal is retrying and how long this has
// been watched. The first two come from the server on every poll and are
// folded in by [State.Absorb]. The elapsed number is the one thing here the
// run does not supply, and it earns its line on the frames where nothing
// else moves: a still screen with a number climbing on it is a run on a long
// step, and a still screen is a program that may have died.
//
// What it deliberately does not do is own the outcome. The model reaches a
// terminal status and quits; the exit code, the outputs, and the failure
// message are all decided by cmd/flow, from the state the model carries out
// with it — see [Run].

// Model draws a run as it goes.
//
// The state is a pointer to the shared machine rather than a copy of its
// fields, which is the one place this departs from the value-model
// convention bubbletea invites. The alternative is a second implementation
// of "what changed and when do we stop", and the whole reason both shapes
// are correct is that there is only one.
type Model struct {
	surface *ui.UI
	deps    Deps
	poller  Poller
	state   *State

	// ctx is held so a poll in flight is cancelled with the program rather
	// than after it. tea.WithContext stops the event loop; it cannot reach
	// into an RPC this model started, and ctrl+c that waits out a request is
	// ctrl+c that looks broken.
	ctx context.Context

	interval time.Duration

	// first and latest are poll times, so elapsed is measured from the
	// messages that drive the model rather than from a clock it reads. That
	// is what makes a test able to state exactly what the view should say.
	first, latest time.Time

	// width and height are the terminal's, from bubbletea rather than from
	// the initial detection, once the first resize arrives.
	width, height int

	// quit records that the person asked to stop, so the outcome is not
	// reported as the run's.
	quit bool
}

// PollMsg is the clock: its time is what elapsed is measured against.
type PollMsg struct{ At time.Time }

// StateMsg is one answer from the server, or one refusal.
type StateMsg struct {
	// At is when the answer was observed, so the outage allowance is
	// measured against the clock rather than against a number of polls.
	At time.Time

	Response *v1.GetResponse
	Err      error
}

// NewModel builds the live view.
func NewModel(
	ctx context.Context,
	surface *ui.UI,
	deps Deps,
	poller Poller,
	interval time.Duration,
	workflowID string,
	known *v1.GetResponse,
	options ...Option,
) Model {
	return Model{
		surface: surface,
		deps:    deps,

		poller:   poller,
		state:    NewState(deps, workflowID, known, options...),
		ctx:      ctx,
		interval: interval,
		width:    surface.ErrCaps.Width,
		height:   surface.ErrCaps.Height,
	}
}

// Run draws a run until it finishes or the person quits, and returns the
// model it ended with.
//
// [Model.Quit] is true when the program ended without an outcome to report:
// the person pressed q/esc/ctrl+c, or the context was cancelled from outside
// (ctrl+c on the process, or a caller with its own reason to stop). Nothing
// about the run went wrong in that case, so nothing here is a failure — the
// caller decides what, if anything, it owes its own caller about a walk that
// stopped before the run did; see cmd/flow's watchEnding.
func Run(
	ctx context.Context,
	surface *ui.UI,
	deps Deps,
	poller Poller,
	interval time.Duration,
	workflowID string,
	known *v1.GetResponse,
	options ...Option,
) (Model, error) {
	model := NewModel(ctx, surface, deps, poller, interval, workflowID, known, options...)

	// Drawn to stderr, so stdout carries the outputs and nothing else — which
	// is what lets one invocation show a live view and pipe its answer. The
	// colour profile is the one detected for that stream, rather than left to
	// bubbletea to detect a second time and possibly differently.
	final, runErr := tea.NewProgram(model,
		tea.WithContext(ctx),
		tea.WithOutput(surface.Err),
		tea.WithColorProfile(surface.ErrCaps.Profile),
	).Run()
	if runErr != nil {
		// A cancelled context is the person pressing ctrl+c. It ends the
		// program by design, so reporting it as a failure to watch would
		// turn an intentional stop into a non-zero exit a pipeline acts on.
		if errors.Is(runErr, context.Canceled) ||
			errors.Is(runErr, tea.ErrProgramKilled) ||
			errors.Is(runErr, tea.ErrInterrupted) {
			return Model{quit: true}, nil
		}

		return Model{}, fmt.Errorf("watching %q: %w", workflowID, runErr)
	}

	done, ok := final.(Model)
	if !ok {
		return Model{}, fmt.Errorf("watching %q ended with a model of type %T, which is a bug", workflowID, final)
	}

	return done, nil
}

// State is the run this model has drawn, as of its last folded message.
func (m Model) State() *State { return m.state }

// Quit reports that the person asked to stop watching, so the walk's ending
// is not the run's outcome.
func (m Model) Quit() bool { return m.quit }

// Init asks immediately rather than after one interval, so the first frame
// says something about the run instead of saying nothing for a second.
func (m Model) Init() tea.Cmd {
	return m.pollAfter(0)
}

// pollAfter schedules the next request.
func (m Model) pollAfter(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(at time.Time) tea.Msg { return PollMsg{At: at} })
}

// Fetch performs one request, returning the [StateMsg] the answer folds
// into. Exported so a test can drive [Model.Update] with the message a real
// poll would have produced, without a program to run one.
func (m Model) Fetch() tea.Cmd {
	return func() tea.Msg {
		response, err := m.poller.Poll(m.ctx)

		// After the call, not before: the allowance measures how long the
		// server has been observed unable to answer, and a request that took
		// thirty seconds to fail was thirty seconds during which nothing was
		// known.
		return StateMsg{At: time.Now(), Response: response, Err: err}
	}
}

// Update folds one message in.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		// Recorded as reported. The clamp and the fallback live in
		// viewWidth, where there is one of each rather than one per place a
		// size arrives from.
		m.width, m.height = msg.Width, msg.Height

		return m, nil

	case tea.KeyPressMsg:
		return m.Key(msg)

	case PollMsg:
		if m.first.IsZero() {
			m.first = msg.At
		}
		m.latest = msg.At

		return m, m.Fetch()

	case StateMsg:
		// A poll cut short by ctrl+c fails with a cancelled context, and
		// connect reports that as a refusal like any other. Absorbing it
		// would record the server as having stopped answering and exit
		// non-zero on a run that is fine — so the interruption is recognised
		// as one, here, for the same reason and in the same place the plain
		// shape recognises it.
		if m.ctx.Err() != nil {
			m.quit = true

			return m, tea.Quit
		}

		// Whether the walk ended badly travels on the state, not out through
		// the program: a bubbletea error means the *rendering* failed, and a
		// server that stopped answering is not that. [Run] reads the reason
		// back off the model it gets returned.
		if m.state.Absorb(msg.At, msg.Response, msg.Err).Done {
			return m, tea.Quit
		}

		return m, m.pollAfter(m.interval)
	}

	return m, nil
}

// Key handles the keyboard.
//
// Three spellings of stop and nothing else. A watch has no state to
// navigate — the step list is short and it is already all on screen — so
// every additional binding would be a thing to discover that does nothing.
// `q` because it is what every full-screen terminal program uses, `esc`
// because it is what people who do not know that press, and `ctrl+c` because
// a program that ignores it is a program people have to kill.
//
// Exported for the same reason [Model.Fetch] is: a test drives the keyboard
// the way a real terminal would, through [Model.Update], but a test
// asserting *only* that a key produces no command has no message loop to
// read one back from.
func (m Model) Key(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "esc", "ctrl+c":
		m.quit = true

		return m, tea.Quit
	}

	return m, nil
}

// View draws the run.
//
// Everything here survives its own removal: the status is a word before it
// is a colour, a step is named before it is marked, and the elapsed time is
// prose. Strip every escape sequence and the screen still says what is
// happening — which is what a screen reader receives, and what a
// `script(1)` capture of a CI job contains.
//
// Nothing on screen may be wider than the terminal, and there are two ways
// to arrange that. Identifiers and marks are *trimmed*: an id too long to
// fit is looked up rather than read, a reader who wants the whole of one has
// `flow get`, and what matters here is that the line below it does not
// move. Prose is *wrapped*, by [Model.note] — see there for why that
// exception is the important one.
func (m Model) View() tea.View {
	theme := m.surface.ErrTheme
	symbols := m.surface.ErrCaps.Symbols()
	state := m.state

	var b strings.Builder

	// The status pill and the id it belongs to, on the line the eye lands on
	// first.
	fmt.Fprintf(&b, "%s %s\n",
		theme.Pill(m.deps.StatusTone(state.status), m.deps.StatusLabel(state.status)),
		theme.Strong.Render(state.workflowID))

	// The run id is what somebody needs in order to ask about this attempt
	// later, and it is not what they are watching for — so it is present and
	// muted.
	if state.runID != "" {
		fmt.Fprintf(&b, "%s\n", theme.Muted.Render("run "+state.runID))
	}

	// Where the run is, on a line of its own rather than beside the id.
	//
	// Beside it, a workflow id long enough to fill the terminal would take
	// the position off the screen with it — and the position is the line
	// that moves, which makes it the one the eye is here for. Built from the
	// same joined path `flow get` prints, so a loop reads
	// `deploy > each > upload` on both.
	if state.position != "" {
		fmt.Fprintf(&b, "%s\n", theme.Muted.Render("on "+state.position))
	}

	fmt.Fprintf(&b, "%s\n", theme.Muted.Render(m.elapsedLine()))

	// Why a run that is not moving is not moving. An attempt count climbing
	// under an unchanging RUNNING is the signature of stuck rather than
	// slow, and it is marked as a warning because that is what it is: the
	// run is still going, and something in it keeps failing.
	for _, pending := range state.pending {
		fmt.Fprintf(&b, "%s\n", m.note(theme.Warning, symbols.Warning, pending))
	}

	if lines := m.stepLines(theme, symbols); lines != "" {
		fmt.Fprintf(&b, "\n%s\n", lines)
	}

	if state.lastError != nil {
		// The reason nothing is moving, said rather than left to be inferred
		// from a screen that has stopped changing.
		fmt.Fprintf(&b, "\n%s\n", m.note(theme.Warning, symbols.Warning, state.lastError.Error()))
	}

	if state.failure != "" {
		fmt.Fprintf(&b, "\n%s\n", m.note(theme.Danger, symbols.Failure, state.failure))
	}

	fmt.Fprintf(&b, "\n%s\n", theme.Muted.Render("q stops watching, not the run"))

	// Trimmed once, over the whole screen, rather than per line as it is
	// built.
	//
	// That is not only shorter: a line is a pill plus an id plus the space
	// between them, and trimming the parts to the full width puts the line
	// over it by however wide the other parts are. Measuring what is
	// actually going to be displayed is the only version of this with no
	// arithmetic to get wrong. ansi.Truncate, underneath, measures display
	// width rather than bytes, so the escape sequences cost nothing against
	// the margin — and the wrapped notes are already inside it, which makes
	// this a no-op on them rather than a second opinion.
	return tea.NewView(ui.Trim(b.String(), m.ViewWidth()))
}

// note renders a marked block of prose, wrapped to the terminal.
//
// Wrapped rather than trimmed, which is the one place this view reflows on
// purpose. A failure message is the reason somebody is looking at the
// screen, and truncating it at the right margin is how "connection refused"
// becomes "no Flowstate server answered at localhost:9233 (set --address or
// FLOWSTATE_ADD". Both notes sit below everything except the footer, so a
// message that grows by a line moves a line nobody is reading.
func (m Model) note(style lipgloss.Style, mark, text string) string {
	// The mark is outside the wrap and the text is indented under it, so a
	// second line lines up with the first rather than under the mark.
	body := style.Width(m.ViewWidth() - 2).Render(text)

	lines := strings.Split(body, "\n")
	for i, line := range lines {
		if i == 0 {
			lines[i] = style.Render(mark) + " " + line

			continue
		}
		lines[i] = "  " + line
	}

	return strings.Join(lines, "\n")
}

// ViewWidth is the columns this view may use.
//
// Two sizes reach the model — the one detected for the stream and the one
// bubbletea reports on a resize — and both are recorded as given. The bound
// is applied here, at the single point of use, rather than at each of them:
// one clamp and one fallback, which is what stops a resize from quietly
// escaping a rule the initial size obeyed.
//
// ui.ClampWidth is the same answer every surface that prints uses. A
// repainting view wider than a printed table is two answers to "how wide is
// the text", in one program.
func (m Model) ViewWidth() int {
	return ui.ClampWidth(m.width)
}

// elapsedLine reports how long the watch has been going.
//
// Measured from the poll messages, so it is the time this command has been
// watching rather than the run's own age — which is the honest thing to
// claim, since a watch attached to a run started yesterday knows nothing
// about yesterday. Named accordingly.
func (m Model) elapsedLine() string {
	if m.first.IsZero() {
		return "watching"
	}

	return fmt.Sprintf("watching for %s", m.latest.Sub(m.first).Round(time.Second))
}

// VisibleSteps is how many step lines fit, given the terminal's height and
// what the rest of the view occupies.
//
// The chrome is eight-ish lines plus whatever the run itself added above the
// list — the position, and a sentence per activity being retried. Counted
// rather than assumed, because those lines appear exactly when a run is in
// trouble, which is when the screen is most worth reading.
//
// A floor of three rather than zero: a terminal too short to show the list
// is better served by a short list that scrolls the header off than by a
// view that silently stops reporting progress at all.
func (m Model) VisibleSteps() int {
	if m.height <= 0 {
		return MaxVisibleSteps
	}

	above := 8 + len(m.state.pending)
	if m.state.position != "" {
		above++
	}

	return max(3, min(MaxVisibleSteps, m.height-above))
}

// maxVisibleSteps caps the list on a terminal tall enough not to need
// capping.
//
// A run with two hundred completed steps has nothing to say in lines one to
// one hundred and eighty that the count does not say better, and the
// interesting end of the list is the recent end.
const MaxVisibleSteps = 12

// stepLines renders the steps that have produced outputs.
//
// A summary rather than progress, and deliberately so now that there is
// progress above it: outputs arrive with the run's result, so this list
// fills in on the final frame while the position line is what moves during
// the run. Two different facts — where the run is, and what it produced —
// which is why they are two blocks rather than one list that means
// different things at different times.
//
// The tail rather than the head, because the useful question is what just
// finished. When the list is cut, the number cut is *stated*: a list that
// silently shows a window reads as a complete list, and a reader counting
// steps would be counting the wrong thing.
func (m Model) stepLines(theme ui.Theme, symbols ui.SymbolSet) string {
	steps := m.state.steps
	if len(steps) == 0 {
		return ""
	}

	var b strings.Builder

	if visible := m.VisibleSteps(); len(steps) > visible {
		fmt.Fprintf(&b, "%s\n", theme.Muted.Render(fmt.Sprintf(
			"%s %d earlier steps", symbols.Ellipsis, len(steps)-visible)))
		steps = steps[len(steps)-visible:]
	}

	for _, id := range steps {
		fmt.Fprintf(&b, "%s %s\n", theme.Success.Render(symbols.Success), id)
	}

	// The count, because the marks above are a window and this is the whole
	// of it.
	fmt.Fprintf(&b, "%s", theme.Muted.Render(fmt.Sprintf("%d step(s) done", len(m.state.steps))))

	return b.String()
}
