package debugtui

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"

	tea "charm.land/bubbletea/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// MaxWatches bounds the expressions the screen re-evaluates at every stop. Each
// is one inspection after every frame read, so the bound is a bound on the work
// a stop costs, and it is small enough to read at a glance.
const MaxWatches = 16

// Watch is an expression the person asked to see at every stop.
//
// A watch is the screen's own: the target never hears of it. What it holds of
// the run is what [flowdebug.Target.Inspect] answered, so a value the target withholds is
// withheld here and nothing in a watch is evaluated by the client.
type Watch struct {
	// Expr is the expression, as typed.
	Expr string

	// Value is the last answer, nil before the first and while it cannot be
	// evaluated.
	Value *v1.DebugValue

	// Err is why it cannot be evaluated, or empty.
	Err string

	// Said reports that the console has already said Err. It is cleared by an
	// answer, so a watch that fails, recovers and fails again says so twice, and
	// one that keeps failing says so once however many stops it fails at.
	Said bool

	// NotHeld reports that the run was not held when it was last read, so there
	// is no value and no error to show.
	NotHeld bool
}

// text is what the watch's row shows.
func (w Watch) text() string {
	switch {
	case w.Err != "":
		return "(" + w.Err + ")"
	case w.NotHeld:
		return "(not held)"
	case w.Value == nil:
		return "(reading)"
	default:
		return valueText(w.Value)
	}
}

// watchNodes is the group the watches are listed under, or nil with none.
func watchNodes(watches []Watch) []pane.Node {
	if len(watches) == 0 {
		return nil
	}
	group := pane.Node{ID: groupWatches, Label: "watches", Value: fmt.Sprintf("{%d}", len(watches)), Total: len(watches)}
	for _, w := range watches {
		group.Children = append(group.Children, pane.Node{
			ID: watchPrefix + w.Expr, Label: w.Expr, Value: w.text(), Total: int(w.Value.GetChildren()),
		})
	}

	return []pane.Node{group}
}

// outcome is what one evaluation of a watch came to.
type outcome uint8

const (
	// outcomeValue: the target answered with a value.
	outcomeValue outcome = iota
	// outcomeError: the target could not evaluate the expression.
	outcomeError
	// outcomeSkipped: the run moved or let go before the answer, which says
	// nothing about the expression. The watch reads as pending, keeping only what
	// the console has said; the read that follows the move evaluates it again.
	outcomeSkipped
	// outcomeNotHeld: the run was not held, so nothing could be asked.
	outcomeNotHeld
)

// watchResult is the answer to one watch in one frame read.
type watchResult struct {
	expr    string
	outcome outcome
	value   *v1.DebugValue
	err     string
}

// evaluateWatches asks the target for each expression at the frame's revision:
// at most [MaxWatches] inspections, none when the run is not held. It is called
// by the command that reads the frame, so a frame and its watches are of one
// stop and at most one evaluation runs at a time.
func evaluateWatches(ctx context.Context, target flowdebug.Target, frame flowdebug.Frame, exprs []string) []watchResult {
	results := make([]watchResult, 0, len(exprs))
	if !frame.Paused {
		for _, expr := range exprs {
			results = append(results, watchResult{expr: expr, outcome: outcomeNotHeld})
		}

		return results
	}

	revision := frame.Snapshot.GetRevision()
	for _, expr := range exprs[:min(len(exprs), MaxWatches)] {
		result := watchResult{expr: expr}
		answer, err := target.Inspect(ctx, &v1.DebugInspectRequest{Revision: revision, Expression: expr})
		switch {
		case errors.Is(err, flowdebug.ErrNotPaused), errors.Is(err, flowdebug.ErrStaleRevision):
			result.outcome = outcomeSkipped
		case err != nil:
			result.outcome, result.err = outcomeError, err.Error()
		case answer.GetError() != "":
			result.outcome, result.err = outcomeError, answer.GetError()
		case answer.GetValue() == nil:
			result.outcome, result.err = outcomeError, "the target answered with no value"
		default:
			result.outcome, result.value = outcomeValue, answer.GetValue()
		}
		results = append(results, result)
	}

	return results
}

// applyWatches folds a read's answers into the watches. An expression that
// cannot be evaluated is said once, in the console, when it starts failing; the
// row says it at every stop after, and an answer clears it without a word.
func (m *Model) applyWatches(results []watchResult) {
	for _, r := range results {
		i := slices.IndexFunc(m.screen.Watches, func(w Watch) bool { return w.Expr == r.expr })
		if i < 0 {
			continue
		}
		w := &m.screen.Watches[i]

		switch r.outcome {
		case outcomeSkipped:
			// The run moved under the read, so what the watch held is of an
			// earlier stop. It reads as pending until the read that follows the
			// move answers; Said stays, so a failing watch is not said twice.
			w.Value, w.Err, w.NotHeld = nil, "", false
		case outcomeNotHeld:
			w.Value, w.Err, w.NotHeld, w.Said = nil, "", true, false
		case outcomeError:
			w.Value, w.NotHeld = nil, false
			w.Err = ui.EscapeControl(cutValue(strings.TrimSpace(r.err)))
			if !w.Said {
				w.Said = true
				m.screen.Console.Say("watch " + w.Expr + ": " + w.Err)
			}
		default:
			w.Value, w.Err, w.NotHeld, w.Said = r.value, "", false, false
		}
	}
}

// watchExprs are the expressions a read evaluates.
func watchExprs(watches []Watch) []string {
	exprs := make([]string, len(watches))
	for i, w := range watches {
		exprs[i] = w.Expr
	}

	return exprs
}

// canWatch reports that the front answers `inspect`, which a watch is.
func (m Model) canWatch() bool {
	return slices.ContainsFunc(m.cfg.Verbs, func(v flowdebug.Verb) bool { return v.Name == "inspect" })
}

// refuse says why a command was not taken, in the console and the line above
// the hint bar.
func (m *Model) refuse(text string) {
	m.screen.Console.Say(text)
	m.toast(ui.ToneWarning, text)
}

// watch starts watching expr and reads the run again, so the new watch has a
// value at this stop. It refuses an expression that is empty, longer than a
// command, not a single line, or already watched, and a seventeenth.
func (m *Model) watch(expr string) tea.Cmd {
	expr = strings.TrimSpace(expr)
	switch {
	case !m.canWatch():
		m.refuse("this front does not answer inspect, so there is nothing to watch with")
	case expr == "":
		m.refuse("watch needs an expression: watch steps.build.size()")
	case len(expr) > flowdebug.MaxCommandBytes:
		m.refuse(fmt.Sprintf("that expression is longer than the %d bytes a command may be", flowdebug.MaxCommandBytes))
	case strings.ContainsFunc(expr, unicode.IsControl):
		m.refuse("a watch is one line without control characters")
	case slices.ContainsFunc(m.screen.Watches, func(w Watch) bool { return w.Expr == expr }):
		m.refuse("already watching " + ui.EscapeControl(cutValue(expr)))
	case len(m.screen.Watches) >= MaxWatches:
		m.refuse(fmt.Sprintf("at most %d watches; unwatch one first", MaxWatches))
	default:
		m.screen.Watches = append(slices.Clone(m.screen.Watches), Watch{Expr: expr})
		m.syncTree()
		m.screen.Tree.Expand(groupWatches)
		m.screen.Tree.Select(watchPrefix + expr)
		m.revealSelection()

		return m.reread()
	}

	return nil
}

// unwatch stops watching the expression at position n (counted from 1, as the
// watches are listed) or spelled as arg.
func (m *Model) unwatch(arg string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		m.refuse("unwatch needs a number or an expression: unwatch 2")

		return
	}

	i := slices.IndexFunc(m.screen.Watches, func(w Watch) bool { return w.Expr == arg })
	if n, err := strconv.ParseUint(arg, 10, 16); err == nil && i < 0 {
		// A number names a position; an expression that is itself a number and
		// is watched was found above.
		if n >= 1 && n <= uint64(len(m.screen.Watches)) {
			i = int(n - 1)
		}
	}
	if i < 0 {
		m.refuse("no such watch: " + ui.EscapeControl(cutValue(arg)))

		return
	}

	m.screen.Watches = slices.Delete(slices.Clone(m.screen.Watches), i, i+1)
	m.syncTree()
}

// syncTree replaces the scope tree's nodes with the frame's scope, the watches
// and the last inspection. What was open and selected stays so where the row
// still exists.
func (m *Model) syncTree() {
	roots := ScopeNodes(m.screen.Frame)
	roots = append(roots, watchNodes(m.screen.Watches)...)
	if r := m.screen.Result; r != nil {
		roots = append(roots, *r)
	}
	m.screen.Tree.SetRoots(roots)
}
