// Package debugpane draws the debugger's two panes — where the run is, and
// what it can name — from a paused session's own read-only surface (#928
// slice 1, #1111 item 2).
//
// # Why this is a package and not a printf in the console
//
// The destination this is a slice of is the full REPL, and the thing that
// decides whether a slice is a foundation or a dead end is what a later slice
// would have to throw away. So the drawing is split from everything that knows
// where it is being drawn:
//
//   - [Frame] is one read of a paused session: the position, the bindings with
//     their values already resolved and already redacted, and the step list.
//     [Snapshot] is the only thing here that touches a [flowdebug.Session].
//   - [Render] is a pure function of a [Frame], a theme and a [Layout]. It
//     reads no clock, no terminal and no session.
//
// That split is the whole design claim. `flow debug attach` (slice 3) reaches a
// session over a wire rather than in this process, and a pane built out of
// session method calls would have to be rewritten for it; a pane built out of a
// [Frame] needs a second way to *fill* one. The same is true of a substrate
// change underneath: a bubbletea `View()` returning [Render]'s string is the
// same drawing, so the port is a change to the console and not to the panes.
//
// # No terminal, no panes
//
// Nothing in this package is reached unless a console is attached, and that is
// the console's decision to make rather than this package's — see
// `cmd/flow/debugpanes.go`. `flow test --debug < script.txt`, `flow debug
// replay` and the `flowstate_debug` MCP tool drive the same session core with
// no terminal, and their output does not change by one byte because none of
// them installs a painter. [flowdebug.Console] is the seam and says so.
//
// # Nothing here reads a clock
//
// Deliberately, and it is a property worth keeping rather than an omission. A
// frame is a function of the session and the layout, so the same session at the
// same stop draws the same bytes on every machine — which is what makes a
// golden frame a test rather than a snapshot of one afternoon. The session's
// own `break at` line carries the run's elapsed time already, from the run's
// clock ([flowdebug.Options.Clock]), which is the right place for it: that is
// the run's time and not the wall's.
package debugpane

import (
	"context"
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The bounds. Everything drawn here is sized by what an author's file says —
// a workflow may declare thousands of steps, and a run inside a loop can name
// thousands more — so each of the three resources that grows with it is
// bounded, and each is bounded where it is actually spent.
const (
	// MaxScopeEvaluations bounds how many bindings [Snapshot] resolves to a
	// value.
	//
	// The names are bounded by nothing on purpose — [flowdebug.Session.Scope]
	// returns every name the run can reach, because a value surface narrower
	// than the run is a worse lie than a long list — and this is the surface
	// that turns each of them into a rendered value, which costs one CEL
	// evaluation apiece. That is the same bound and the same reasoning as
	// [flowdap.MaxScopeVariables], set lower because the two spend it on
	// different things: an editor's variables pane is scrolled through
	// hundreds of rows, and a terminal pane shows a couple of dozen and says
	// how many it did not.
	//
	// It is a bound on *work*, not on knowledge: what is not evaluated is
	// counted and reported, never silently missing.
	MaxScopeEvaluations = 200

	// MaxValueRunes bounds one rendered value's width before the layout gets
	// it.
	//
	// [flowdebug.MaxInspectRunes] already bounds what an inspection returns,
	// at four kilobytes — the right size for a line somebody asked for and far
	// too large for a row nobody did. A value this long is cut here so that
	// the layout is never handed a row it has to reason about, and the cut is
	// marked.
	MaxValueRunes = 240

	// MaxPaneRows bounds either pane on a terminal tall enough not to need
	// bounding, and is what an unmeasurable one gets.
	//
	// The panes are printed above the prompt rather than into a region of
	// their own, so every row costs a row of the transcript above them. Twelve
	// is `watch`'s own answer to the same question ([watch.MaxVisibleSteps]),
	// reached the same way: past a dozen rows a list has stopped being a
	// glance and the count says more than the rows do.
	MaxPaneRows = 12

	// MinPaneRows is the floor a short terminal still gets.
	//
	// Three rather than zero, for [watch.Model.VisibleSteps]'s reason: a
	// terminal too short for the panes is better served by panes that scroll
	// the transcript than by a debugger that silently stops saying where it
	// is.
	MinPaneRows = 3
)

// Binding is one name in the paused run's scope, with the value it holds.
//
// The value is a string rather than a [ref.Val] because it has been through
// [flowdebug.Session.Evaluate], which is the redacting door: a caller reaching a
// session through a pane is no more entitled to a secret than one at a
// terminal, and rendering here from anything else would open on a new surface
// the hole that seam exists to close.
// It carries no group, deliberately, though the session hands one out and a
// pane-per-group is an obvious slice-2 shape. Nothing here reads it today, and
// an exported field nothing reads is a promise made before anyone knows what it
// should be — the group is one field away when something needs it.
type Binding struct {
	// Expression is what would be typed to ask for this value again — the
	// group's root and the name, or the bare name where the group has no root.
	// It is what the pane shows, because a pane whose rows cannot be typed
	// back at the prompt is a pane that has to be translated before it is
	// useful.
	Expression string

	// Value is the rendered, redacted answer, or the redacted reason there is
	// none. Either way it is safe to draw: see the type's own doc.
	Value string
}

// Frame is one read of a paused session, and everything the panes are drawn
// from.
//
// A value rather than a session, which is the seam this package is built on:
// see the package doc.
type Frame struct {
	// At is where the session is holding the run, and Paused reports whether
	// it is holding one at all. Both are [flowdebug.Session.Paused]'s answer,
	// kept in the shape it gives them — an autopsy is a real pause with no
	// step to name, so "there is a position" and "the position has a step"
	// have to stay separable.
	At     flowdebug.Position
	Paused bool

	// Bindings are the scope pane's rows, in the order `scope` groups them.
	Bindings []Binding

	// BindingsTotal is how many names the run could reach, which is the number
	// Bindings is a prefix of. Kept separately so the elision can say how many
	// rather than that there were some.
	BindingsTotal int

	// Steps is the window of the run's step list this frame draws, and what
	// each of those has done.
	//
	// A window rather than the list, and taken at [Snapshot] rather than cut
	// at [Render]. The list is the author's file — a workflow may declare
	// thousands of nodes and a pane draws a dozen — so copying every entry at
	// every stop is O(N) per stop and O(N²) across a walk of the run. It is
	// also the shape a wire message wants, which is what keeps slice 3's
	// network-attached session answering the same question the same way.
	Steps []flowdebug.Step

	// StepsBefore and StepsAfter are how many steps the window left out on
	// each side, so an elision can say how many rather than that there were
	// some.
	StepsBefore int
	StepsAfter  int

	// StepsTotal is the length of the whole list the window is onto.
	StepsTotal int

	// Held is the index *within Steps* of the step the run is held at, or -1
	// where the window holds none — an autopsy, or a position the step list
	// cannot place.
	//
	// An index rather than a comparison left to [Render], because the
	// comparison is the thing that was wrong: matching rows by bare id marks
	// every row of that name held, and across a `call:` a caller and a callee
	// may both declare `build` (`eval.go:1804-1812`). The session resolves it
	// once, against the workflow the engine says the run is in.
	Held int

	// StepsUnattributed is how many rows of the whole list carry an id another
	// workflow also declares, and whose outcome the session therefore cannot
	// attribute. See [flowdebug.Session.Steps].
	StepsUnattributed int

	// StepsTruncated reports that the session stopped recording what it
	// watched, so a state above may understate what a step actually did.
	StepsTruncated bool
}

// Snapshot reads one frame from a session.
//
// It resolves values as well as names, because a scope pane listing names
// alone is the `scope` command with a box drawn round it. Every value goes
// through [flowdebug.Session.Evaluate] — the redacting door — and a failed
// evaluation becomes the row's value rather than dropping the row, so the pane
// cannot come to disagree with the scope the session reports.
//
// It takes the layout because the *window* is part of the read: asking the
// session for the whole step list and cutting it here would copy an entry per
// declared node at every stop, which is the allocation the window exists to
// avoid. A frame is therefore sized for the terminal it is about to be drawn
// on, and [Render] draws what it is given.
//
// The second return reports whether there was a pause to read at all. A session
// between stops has no scope to answer against, and drawing a frame of its last
// one would report a position the run has left.
func Snapshot(ctx context.Context, session *flowdebug.Session, layout Layout) (Frame, bool) {
	at, paused := session.Paused()
	if !paused {
		return Frame{}, false
	}

	frame := Frame{At: at, Paused: true, Held: -1}

	// Where the run is in the list, and how long the list is — both without
	// copying it, so the window can be chosen before anything is allocated.
	// The workflow is what makes the position resolvable across a `call:`; the
	// session answers -1 rather than guessing when it cannot tell (see
	// [flowdebug.Session.StepPosition]).
	index, total := session.StepPosition(at.Workflow, positionStep(at))
	first, last := window(total, index, paneRows(layout.Height))

	list := session.Steps(first, last-first)

	frame.Steps = list.Steps
	frame.StepsBefore = list.Offset
	frame.StepsAfter = list.Total - list.Offset - len(list.Steps)
	frame.StepsTotal = list.Total
	frame.StepsUnattributed = list.Unattributed
	frame.StepsTruncated = list.Truncated

	if index >= list.Offset && index < list.Offset+len(list.Steps) {
		frame.Held = index - list.Offset
	}

	groups, err := session.Scope()
	if err != nil {
		// The run left the pause between the two reads. A frame of the steps
		// alone is still true — [flowdebug.Session.Steps] is about the workflow
		// rather than about a scope — so it is drawn rather than discarded.
		//
		// A named survivor: no test here drives this branch, because the window
		// it covers is between two of the session's own lock acquisitions and
		// nothing this side of that lock can hold a run inside it. Deleting it
		// does not fail a test — it turns a resumed run into a frame with no
		// scope pane, which is what this returns anyway. What it buys is the
		// step pane, which is still true, and it is written down rather than
		// left as coverage nobody can account for (CLAUDE.md).
		return frame, true
	}

	for _, group := range groups {
		frame.BindingsTotal += len(group.Names)

		for _, name := range group.Names {
			if len(frame.Bindings) >= MaxScopeEvaluations {
				// Counting continues past the bound, deliberately: the total is
				// what makes the elision honest, and stopping the count here
				// would report the bound back as though it were the scope.
				continue
			}

			// The session's own answer for what this group's names hang from,
			// rather than a switch over group names here. See
			// [flowdebug.Names.Root] for the two that already had one.
			expression := name
			if group.Root != "" {
				expression = group.Root + "." + name
			}

			text, _, evalErr := session.Evaluate(ctx, expression)
			if evalErr != nil {
				// The name is real — the run said so — and only its value could
				// not be produced. The error is already redacted by the same
				// seam the value would have been.
				text = "(" + evalErr.Error() + ")"
			}

			frame.Bindings = append(frame.Bindings, Binding{
				Expression: expression,
				Value:      capValue(text),
			})
		}
	}

	return frame, true
}

// capValue shortens one rendered value to [MaxValueRunes], saying that it did.
//
// Runes rather than bytes, because the cut is about how wide a row is and a cut
// mid-rune is a replacement glyph rather than a shorter value. The marker is
// prose rather than [ui.SymbolSet]'s ellipsis: this runs before a symbol set is
// known, and a value that was cut has to say so in a way that survives every
// set.
func capValue(text string) string {
	text = ui.EscapeControl(text)

	runes := []rune(text)
	if len(runes) <= MaxValueRunes {
		return text
	}

	return string(runes[:MaxValueRunes]) + " (cut)"
}

// positionStep is the step a position names, or "" where it names none.
//
// An autopsy is a real pause with no step to be at — the run is over — so it
// windows the front of the list rather than pointing into it.
func positionStep(at flowdebug.Position) string {
	if at.Autopsy {
		return ""
	}

	return at.Step
}

// Layout is the space the panes have.
type Layout struct {
	// Width is the columns to draw within. Zero or less is answered with the
	// package's own fallback rather than with a division by nothing, which is
	// the rule [ui.Capabilities] already states for a stream it cannot
	// measure.
	Width int

	// Height is the terminal's rows, or zero where there are none to count —
	// [ui.Capabilities.Height]'s own convention, kept rather than restated. A
	// stream with no height gets [MaxPaneRows] per pane, because the panes are
	// printed into a transcript rather than into a screen they have to fill.
	Height int
}

// Render draws the two panes.
//
// Pure: no clock, no terminal, no session. Given the same frame, theme,
// symbols and layout it returns the same bytes, which is what makes a golden
// frame a test.
//
// The returned string ends in a newline where anything was drawn, and is empty
// where the frame holds nothing to draw — a session that is not paused, and
// nothing to say about it.
func Render(frame Frame, theme ui.Theme, symbols ui.SymbolSet, layout Layout) string {
	if !frame.Paused {
		return ""
	}

	// Clamped through the one function the rest of the CLI measures with, so a
	// pipe and a very wide terminal are answered here exactly as they are
	// everywhere else: a fallback rather than a division by nothing, and a
	// ceiling rather than a rule spanning three hundred columns.
	width := ui.ClampWidth(layout.Width)

	// The two panes split the rows between them rather than each taking a
	// budget of its own, because they are printed one above the other into one
	// transcript: two panes each holding to a dozen rows is two dozen rows of
	// somebody's screen, whatever the terminal's height said.
	//
	// The step pane's share was already spent, at [Snapshot], which is what
	// keeps a stop from copying the whole list; this is the scope pane's.
	rows := paneRows(layout.Height)

	var b strings.Builder

	// Steps above scope, and the order is a decision. The prompt sits directly
	// below what this returns, and `inspect` is what somebody types there — so
	// the names they are about to type are the ones nearest their cursor, and
	// the position, which the `break at` line above has already told them, is
	// further away.
	steps := stepRows(frame, theme, symbols)
	scope := scopeRows(frame, theme, rows)

	writePane(&b, width, theme, symbols, "steps", steps)
	if len(steps) > 0 && len(scope) > 0 {
		b.WriteString("\n")
	}
	writePane(&b, width, theme, symbols, "scope", scope)

	return b.String()
}

// paneRows is how many rows one pane may draw, given the terminal's height.
//
// Half the height, because there are two of them, less the chrome each pays
// for: a heading, and the account of the step that has just finished sitting
// above them. Bounded above by [MaxPaneRows] on a tall terminal and below by
// [MinPaneRows] on a short one.
func paneRows(height int) int {
	if height <= 0 {
		return MaxPaneRows
	}

	// Six rows of chrome: two headings, the `break at` line, the step's own
	// outcome line, the prompt, and one blank the eye needs between the panes
	// and the prompt. Counted rather than assumed, so a change to what is
	// printed around the panes is a change to this number.
	const chrome = 6

	return max(MinPaneRows, min(MaxPaneRows, (height-chrome)/2))
}

// writePane draws one pane's heading and rows, or nothing where it has none.
func writePane(b *strings.Builder, width int, theme ui.Theme, symbols ui.SymbolSet, label string, rows []string) {
	if len(rows) == 0 {
		return
	}

	// The heading is trimmed like every other line and not merely built to
	// width: [heading] gives up its rule on a terminal narrower than the label
	// and returns the label whole, which on a *very* narrow one is itself over
	// the margin. Trimming here rather than there keeps one rule — a line is cut
	// once, at the end, where it is written — instead of two places that both
	// have to be right.
	fmt.Fprintf(b, "%s\n", ui.Trim(heading(label, width, theme, symbols), width))
	for _, row := range rows {
		fmt.Fprintf(b, "%s\n", ui.Trim(row, width))
	}
}

// heading is a pane's label with a rule filling the rest of the line.
//
// A rule rather than a box, because a box has two more sides than this needs
// and each of them is a column of a terminal somebody is reading a transcript
// in. [ui.SymbolSet.Divider] is the mark, so a stream that cannot carry the
// box-drawing character gets a run of hyphens and the same layout.
func heading(label string, width int, theme ui.Theme, symbols ui.SymbolSet) string {
	// One space either side of the label, and the rule takes what is left. A
	// negative remainder is a terminal narrower than the word, which gets the
	// word and no rule rather than an arithmetic error.
	fill := width - len([]rune(label)) - 2
	if fill < 1 {
		return theme.Header.Render(label)
	}

	return theme.Header.Render(label) + " " + theme.Muted.Render(strings.Repeat(symbols.Divider, fill))
}

// stepRows are the step pane's lines: a window over the run's steps, centred on
// where it is paused.
//
// Centred rather than tailed, which is the one place this deliberately differs
// from `flow watch`'s list. That one shows the *recent* end because a watch is
// about progress and the newest line is the interesting one. A debugger's list
// is about position: what matters is the step the run is held at and what is on
// either side of it, and a tail would put the paused step at the bottom edge
// with nothing after it to say what comes next.
func stepRows(frame Frame, theme ui.Theme, symbols ui.SymbolSet) []string {
	if len(frame.Steps) == 0 {
		return nil
	}

	// Where two rows in the *window* share an id, each is drawn against
	// whatever tells them apart. Only where they share one: a qualifier on
	// every row would be noise on the ordinary workflow that has no call in
	// it, and the pane's rows are meant to be typed back at the prompt, where
	// the name is the id.
	qualify := qualifiers(frame.Steps)

	rows := make([]string, 0, len(frame.Steps)+4)
	if frame.StepsBefore > 0 {
		rows = append(rows, theme.Muted.Render(fmt.Sprintf("  %s %d earlier", symbols.Ellipsis, frame.StepsBefore)))
	}
	for i, step := range frame.Steps {
		rows = append(rows, stepRow(step, i == frame.Held, qualify[i], theme, symbols))
	}
	if frame.StepsAfter > 0 {
		rows = append(rows, theme.Muted.Render(fmt.Sprintf("  %s %d later", symbols.Ellipsis, frame.StepsAfter)))
	}

	// The count, because the rows are a window and this is the whole of it.
	rows = append(rows, theme.Muted.Render(fmt.Sprintf("  %d step(s)", frame.StepsTotal)))

	// Then the two things a row cannot say about itself, each on a line of its
	// own rather than appended to the count. Every line here is trimmed to the
	// margin, and a sentence explaining why a row reads `pending` is worth
	// nothing cut in half — which is what a single long line becomes on an
	// eighty-column terminal.
	if frame.StepsTruncated {
		// A state this session stopped recording reads as `pending`, which a
		// reader has no way to tell from a step the run has not reached.
		rows = append(rows, theme.Muted.Render("  outcomes stopped being recorded; some `pending` are not"))
	}
	if frame.StepsUnattributed > 0 {
		// The same shape for a different reason: an outcome arrives naming a
		// bare id, so where two workflows declare that id nothing can say
		// whose it was. Said out loud, because `pending` on a step that has
		// plainly run is otherwise a pane that looks broken.
		rows = append(rows, theme.Muted.Render(fmt.Sprintf(
			"  %d share an id across a `call:`; outcomes not attributed", frame.StepsUnattributed)))
	}

	return rows
}

// qualifiers is the prefix to draw each row against, by row index, and "" for
// a row that needs none.
//
// Three cases, and the third is why this is a function taking its rows rather
// than a loop inside [stepRows]: only the first is what real fixtures produce,
// and a test has to be able to build the others.
//
//   - An id only one row in the window carries needs nothing. The id is the
//     name, and a qualifier on every row is noise on the ordinary workflow.
//   - Rows sharing an id from differently-named workflows are told apart by
//     the workflow, which is what a reader already has in the file.
//   - Rows sharing an id *and* a workflow name are one callee invoked from two
//     `call:` steps, or two embedded workflows that share a `name:`. The name
//     cannot separate those, so the call step an author wrote does — see
//     [flowdebug.Step.Via]. Where even that is equal the rows are drawn
//     unqualified rather than decorated with something that distinguishes
//     nothing.
func qualifiers(steps []flowdebug.Step) []string {
	rowsFor := make(map[string][]int, len(steps))
	for i, step := range steps {
		rowsFor[step.ID] = append(rowsFor[step.ID], i)
	}

	out := make([]string, len(steps))
	for _, rows := range rowsFor {
		if len(rows) < 2 {
			continue
		}

		names := map[string]struct{}{}
		for _, i := range rows {
			names[steps[i].Workflow] = struct{}{}
		}

		for _, i := range rows {
			switch {
			case len(names) > 1 && steps[i].Workflow != "":
				out[i] = steps[i].Workflow
			case steps[i].Via != "":
				out[i] = steps[i].Via
			}
		}
	}

	return out
}

// window is the half-open range of a list of n items to show, given a budget of
// rows and the index the window is centred on.
//
// A function taking its inputs rather than an expression inside [stepRows],
// because every interesting answer here is one the real data rarely produces: a
// list shorter than the budget, a position at either end, a position the list
// does not contain. Written inline, those are cases no test can drive — which
// is CLAUDE.md's "assert where the answers differ" exactly.
//
// An index of -1 (no position) windows the *front* of the list rather than the
// middle: at an autopsy the run is over and the first steps are where it began,
// which is a better default than the middle of a list nothing is pointing into.
func window(n, at, budget int) (first, last int) {
	if budget >= n {
		return 0, n
	}
	if at < 0 {
		return 0, budget
	}

	// Centred, with the odd row going below the position: a reader looking at
	// where a run is held is looking forward more than back.
	first = at - (budget-1)/2
	first = max(0, min(first, n-budget))

	return first, first + budget
}

// stepRow is one step's line: a gutter saying whether the run is held here, the
// state's mark, the id, and the state in words.
//
// The state is a word as well as a mark, which is [ui.SymbolSet]'s own rule —
// "the status is the word RUNNING, and the mark beside it only helps the eye
// find the row". Removing every colour and every mark from this pane loses
// emphasis and no information.
func stepRow(step flowdebug.Step, held bool, qualifier string, theme ui.Theme, symbols ui.SymbolSet) string {
	gutter := " "
	name := step.ID
	if qualifier != "" {
		// The qualifier first, muted, because the id is still the name: a
		// reader scanning the column is looking for `build`, and the prefix is
		// there to tell two of them apart rather than to be read.
		name = theme.Muted.Render(qualifier+".") + step.ID
	}

	id := name
	if held {
		gutter = symbols.Arrow
		id = theme.Strong.Render(name)
	}

	mark, style := stepMark(step.State, theme, symbols)

	return fmt.Sprintf("%s %s %s %s", gutter, style.Render(mark), id, theme.Muted.Render(step.State.String()))
}

// stepMark is the mark and the role a state reads as.
//
// The mapping is the one `flow test`'s transcript already makes from the same
// three outcomes: a failure the run absorbs is a warning, one it does not is
// danger, and everything else is account. A skipped step and one not yet
// reached are both muted, because neither is an outcome — and they are told
// apart by their marks and by the word, not by colour.
func stepMark(state flowdebug.StepState, theme ui.Theme, symbols ui.SymbolSet) (string, lipgloss.Style) {
	switch state {
	case flowdebug.StepDone:
		return symbols.Success, theme.Success
	case flowdebug.StepTolerated:
		return symbols.Warning, theme.Warning
	case flowdebug.StepFailed:
		return symbols.Failure, theme.Danger
	case flowdebug.StepSkipped:
		return symbols.Skipped, theme.Muted
	case flowdebug.StepRunning:
		return symbols.Running, theme.Info
	default:
		return symbols.Waiting, theme.Muted
	}
}

// scopeRows are the scope pane's lines: what the paused run can name, and what
// each of those holds.
func scopeRows(frame Frame, theme ui.Theme, budget int) []string {
	if len(frame.Bindings) == 0 {
		if frame.BindingsTotal > 0 {
			// Names with no rows is the bound above having been spent entirely
			// on the elision, which a budget of zero cannot happen at — but a
			// pane that reported nothing while the run could name something
			// would be the one wrong answer here.
			return []string{theme.Muted.Render(fmt.Sprintf("  %d name(s), none rendered", frame.BindingsTotal))}
		}

		return nil
	}

	shown := min(budget, len(frame.Bindings))

	// The widest expression among the rows actually drawn, so the values line
	// up in a column. Measured over the window rather than over every binding:
	// one very long name out of five thousand would otherwise indent every
	// visible row off the screen.
	widest := 0
	for _, binding := range frame.Bindings[:shown] {
		widest = max(widest, len([]rune(binding.Expression)))
	}

	rows := make([]string, 0, shown+1)
	for _, binding := range frame.Bindings[:shown] {
		rows = append(rows, fmt.Sprintf("  %s%s  %s",
			theme.Strong.Render(binding.Expression),
			strings.Repeat(" ", widest-len([]rune(binding.Expression))),
			binding.Value))
	}

	// What was left out, and how much of it, in the two places it can be left
	// out from: this pane's rows, and [MaxScopeEvaluations] above them. One
	// sentence covers both, because to a reader they are one fact — the run
	// can name more than this shows — and the remedy for both is the same
	// command.
	if elided := frame.BindingsTotal - shown; elided > 0 {
		rows = append(rows, theme.Muted.Render(fmt.Sprintf(
			"  %d more of %d (`scope` lists the names; `inspect <name>` reads one)",
			elided, frame.BindingsTotal)))
	}

	return rows
}
