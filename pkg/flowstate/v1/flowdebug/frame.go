package flowdebug

import (
	"cmp"
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Frame is one read of a debug session through its [Target], and everything a
// pane is drawn from.
//
// It is filled only by [ReadFrame], and only from what [Target.Snapshot] and
// [Target.Inspect] answer plus the caller's own program: the redacting door
// every front already walks through. A [v1.DebugValue] is text, never a
// ref.Val, so a Frame holds no secret because no Target answer can. A pane that
// is a pure function of a Frame therefore draws the same bytes whether the
// session is in this process, behind a [Reversible], or on a durable run across
// the network.
type Frame struct {
	// Snapshot is the target's own answer. It is never nil in a Frame
	// [ReadFrame] returned without an error.
	Snapshot *v1.DebugSnapshot

	// Program and SourceMap are the caller's, passed through. Program is nil
	// over the wire without one, and SourceMap is set only by a caller that has
	// verified it (see [Remote.SourceMapVerified]).
	Program   *v1.Workflow
	SourceMap *v1.DebugSourceMap

	// At is where the run is held. Paused reports whether it is held at all,
	// kept apart from At because an autopsy is a real pause with no step.
	At     Position
	Paused bool

	// Steps is the window of the run's step list, or nil where nothing names
	// one: a target reached over the wire with no program. Nil is an answer,
	// not a gap to fill with a guess.
	Steps *StepWindow

	// Scope names what the held run can reach. Each group carries its total and
	// the bindings resolved within the budget, so a group may hold a prefix of
	// its names. Nil where the scope could not be read; ScopeNote says why.
	Scope *v1.DebugScope

	// Values maps a binding's expression to its redacted value, at most
	// [MaxFrameValues] of them.
	Values map[string]*v1.DebugValue

	// ScopeNote is why Scope is absent or incomplete, in words for a reader:
	// inspect was refused, or the run left the stop mid-read. Empty when the
	// scope was read whole, or when the run holds nothing to read.
	ScopeNote string

	// Partial reports that the target dropped observations, so any state
	// derived from them understates what the run did.
	Partial bool
}

// StepWindow is a window onto the run's step list and what each step has done.
type StepWindow struct {
	// Steps is the window, in the order an author wrote them.
	Steps []Step

	// Labels overrides a row's text only where joining its individually safe
	// parts recreated text the pause redactor withholds.
	Labels map[int]string

	// Before and After count the steps the window left out on each side, and
	// Total the whole list.
	Before, After, Total int

	// Held is the index within Steps of the step the run is held at, or -1.
	Held int

	// Unattributed counts rows whose outcome cannot be attributed because
	// another workflow declares the same id.
	Unattributed int

	// Truncated reports that outcomes stopped being recorded.
	Truncated bool
}

// StepSource is the richer step inventory a local [Session] can give a Frame:
// the window, the qualifiers and the held index across a `call:`, none of
// which a [Target] carries. [*Session] implements it, and a source must read
// the same session the [Target] does.
type StepSource interface {
	PausedStepPosition() (position Position, index, total int, paused bool)
	Steps(offset, limit int) StepList
}

var _ StepSource = (*Session)(nil)

// FrameOptions says what [ReadFrame] may add to what the target answers.
type FrameOptions struct {
	// Program and SourceMap are carried into the Frame unread.
	Program   *v1.Workflow
	SourceMap *v1.DebugSourceMap

	// Inventory is the step list a program declares, for a target that cannot
	// name one. States are derived from the snapshot's observations. Empty
	// means the Frame has no step list.
	Inventory []Step

	// Source, when set, wins over Inventory: a local session's own account.
	Source StepSource

	// StepRows is the step window's height; zero asks for a dozen. It is held
	// to [MaxFrameStepRows].
	StepRows int

	// MaxValues bounds how many bindings are resolved; zero or more than
	// [MaxFrameValues] asks for [MaxFrameValues].
	MaxValues int
}

const (
	// MaxFrameValues bounds the scope values one [ReadFrame] resolves. Each is
	// one evaluation under the run's own cost bound; what is not resolved is
	// counted in the group's total, never silently missing.
	MaxFrameValues = 200

	// MaxFrameStepRows bounds the step window.
	MaxFrameStepRows = 64

	defaultFrameStepRows = 12
)

// ReadFrame reads one [Frame] from t.
//
// It costs at most one snapshot, one scope-root listing, and one listing per
// scope group whose limits sum to [MaxFrameValues]. A target that refuses the
// scope still yields a Frame, with the refusal in [Frame.ScopeNote]; only a
// target that cannot give a snapshot returns an error.
func ReadFrame(ctx context.Context, t Target, opts FrameOptions) (Frame, error) {
	snapshot, err := t.Snapshot(ctx)
	if err != nil {
		return Frame{}, err
	}

	frame := Frame{
		Snapshot:  snapshot,
		Program:   opts.Program,
		SourceMap: opts.SourceMap,
		Partial:   snapshot.GetObservationsDropped() > 0 || len(snapshot.GetObservations()) >= MaxObservations,
	}

	// A caller's count is a request, not a trusted size: a negative one would
	// reverse the window and panic the slice built from it.
	rows := max(1, min(cmp.Or(opts.StepRows, defaultFrameStepRows), MaxFrameStepRows))

	switch {
	case opts.Source != nil:
		var index, total int
		frame.At, index, total, frame.Paused = opts.Source.PausedStepPosition()
		if !frame.Paused {
			return frame, nil
		}
		first, last := StepWindowAround(total, index, rows)
		list := opts.Source.Steps(first, last-first)
		window := &StepWindow{
			Steps: list.Steps, Before: list.Offset, After: list.Total - list.Offset - len(list.Steps),
			Total: list.Total, Unattributed: list.Unattributed, Truncated: list.Truncated, Held: -1,
		}
		for i, label := range joinedLabels(list.Steps) {
			if redacted := list.RedactText(label); redacted != label {
				if window.Labels == nil {
					window.Labels = make(map[int]string)
				}
				window.Labels[i] = redacted
			}
		}
		if index >= list.Offset && index < list.Offset+len(list.Steps) {
			window.Held = index - list.Offset
		}
		frame.Steps = window
	default:
		frame.Paused = snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD
		frame.At = positionOfOccurrence(snapshot.GetOccurrence())
		if snapshot.GetReason() == v1.DebugStopReason_DEBUG_STOP_REASON_AUTOPSY {
			// The occurrence is the last step the run executed, not one it is
			// held before: an autopsy has no held row.
			frame.At = Position{Autopsy: true}
		}
		if !frame.Paused {
			return frame, nil
		}
		if len(opts.Inventory) > 0 {
			frame.Steps = inventoryWindow(opts.Inventory, snapshot, frame.At, rows)
		}
	}

	readScope(ctx, t, &frame, min(cmp.Or(opts.MaxValues, MaxFrameValues), MaxFrameValues))

	return frame, nil
}

// joinedLabels is the text each row is drawn as when it carries a qualifier:
// the label a renderer assembles, which redaction has to judge whole.
func joinedLabels(steps []Step) []string {
	rowsFor := make(map[string][]int, len(steps))
	for i, step := range steps {
		rowsFor[step.ID] = append(rowsFor[step.ID], i)
	}

	out := make([]string, len(steps))
	for i, step := range steps {
		out[i] = step.ID
	}
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
				out[i] = steps[i].Workflow + "." + steps[i].ID
			case steps[i].Via != "":
				out[i] = steps[i].Via + "." + steps[i].ID
			}
		}
	}

	return out
}

// positionOfOccurrence is the held position a snapshot's occurrence names.
func positionOfOccurrence(occurrence *v1.DebugOccurrence) Position {
	site := occurrence.GetSite()
	path := site.GetPath()
	position := Position{Kind: site.GetKind(), Workflow: site.GetWorkflow()}
	if len(path) > 0 {
		position.Step = path[len(path)-1]
	}

	return position
}

// inventoryWindow windows a declared step list around the held step, with each
// row's state taken from the snapshot's observations.
//
// An id more than one row declares is left pending and counted, exactly as a
// local session does: an outcome arrives naming a bare id, so nothing can say
// whose it was.
func inventoryWindow(order []Step, snapshot *v1.DebugSnapshot, at Position, rows int) *StepWindow {
	seen := make(map[string]StepState)
	for _, observation := range snapshot.GetObservations() {
		switch observation.GetKind() {
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED:
			seen[observation.GetStepId()] = StepDone
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED:
			seen[observation.GetStepId()] = StepSkipped
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED:
			seen[observation.GetStepId()] = StepFailed
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TOLERATED:
			seen[observation.GetStepId()] = StepTolerated
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_WAITING:
			seen[observation.GetStepId()] = StepRunning
		}
	}

	declared := make(map[string]int, len(order))
	for _, step := range order {
		declared[step.ID]++
	}
	unattributed := 0
	for _, count := range declared {
		if count > 1 {
			unattributed += count
		}
	}

	index := positionIn(order, at.Workflow, at.Step)
	first, last := StepWindowAround(len(order), index, rows)

	// The steps the held one sits inside (a loop, a parallel group, a call) are
	// running too, though the run has not finished them.
	var enclosing map[string]bool
	if path := snapshot.GetOccurrence().GetSite().GetPath(); len(path) > 1 && !at.Autopsy {
		enclosing = make(map[string]bool, len(path)-1)
		for _, id := range path[:len(path)-1] {
			enclosing[id] = true
		}
	}

	// Observations are redacted by the target; an id the program does not
	// declare may be a step whose name redaction changed, so its outcome cannot
	// be attributed. Say the rows may understate rather than draw them pending.
	truncated := false
	for id := range seen {
		if declared[id] == 0 {
			truncated = true
		}
	}
	if !at.Autopsy && at.Step != "" && index < 0 {
		truncated = true
	}
	window := &StepWindow{
		Steps: make([]Step, 0, last-first), Before: first, After: len(order) - last,
		Total: len(order), Unattributed: unattributed, Held: -1, Truncated: truncated,
	}
	for i, step := range order[first:last] {
		step.State = seen[step.ID]
		if declared[step.ID] > 1 {
			step.State = StepPending
		}
		if enclosing[step.ID] && declared[step.ID] == 1 && step.State == StepPending {
			step.State = StepRunning
		}
		if first+i == index {
			window.Held = i
			// A second pass over a loop body is held before a step an earlier
			// pass finished; the earlier outcome is not this arrival's.
			if step.State != StepFailed {
				step.State = StepRunning
			}
		}
		window.Steps = append(window.Steps, step)
	}

	return window
}

// StepWindowAround is the half-open range of a list of n items to show, given a
// budget of rows and the index to centre on.
//
// An index of -1 windows the front of the list rather than the middle: at an
// autopsy the run is over and the first steps are where it began. The odd row
// goes below the position, because a reader of a held run looks forward more
// than back.
func StepWindowAround(n, at, budget int) (first, last int) {
	if budget >= n {
		return 0, n
	}
	if at < 0 {
		return 0, budget
	}

	first = at - (budget-1)/2
	first = max(0, min(first, n-budget))

	return first, first + budget
}

// readScope fills frame.Scope and frame.Values through t.Inspect, within
// budget evaluations.
func readScope(ctx context.Context, t Target, frame *Frame, budget int) {
	if capabilities := frame.Snapshot.GetCapabilities(); capabilities != nil && !capabilities.GetInspect() {
		frame.ScopeNote = "inspect is not permitted for this session"

		return
	}

	revision := frame.Snapshot.GetRevision()
	roots, err := t.Inspect(ctx, &v1.DebugInspectRequest{Revision: revision})
	if err != nil {
		switch {
		case errors.Is(err, ErrNotPaused):
		case errors.Is(err, ErrStaleRevision):
			frame.ScopeNote = "the run moved before its scope could be read"
		case connect.CodeOf(err) == connect.CodePermissionDenied:
			frame.ScopeNote = "inspect is not permitted: " + diagnostic(err.Error())
		default:
			frame.ScopeNote = "scope unavailable: " + diagnostic(err.Error())
		}

		return
	}
	if reason := roots.GetError(); reason != "" {
		frame.ScopeNote = "inspect is not permitted: " + diagnostic(reason)

		return
	}

	frame.Scope = &v1.DebugScope{}
	frame.Values = make(map[string]*v1.DebugValue)
	remaining := budget
	// The groups a session has are a handful; a peer that lists more is not
	// given a listing call each.
	for _, root := range roots.GetChildren()[:min(len(roots.GetChildren()), maxFrameGroups)] {
		group := &v1.DebugScopeGroup{Group: root.GetName(), Total: root.GetValue().GetChildren()}
		frame.Scope.Groups = append(frame.Scope.Groups, group)
		frame.Scope.Total += group.Total
		if remaining <= 0 || group.Total == 0 || frame.ScopeNote != "" {
			continue
		}

		names, err := t.Inspect(ctx, &v1.DebugInspectRequest{
			Revision: revision, Expression: root.GetValue().GetExpression(), Limit: int32(min(remaining, MaxInspectLimit)),
		})
		if err == nil && names.GetError() != "" {
			err = errors.New(names.GetError())
		}
		if err != nil {
			frame.ScopeNote = "scope incomplete: " + diagnostic(err.Error())

			continue
		}
		for _, variable := range names.GetChildren()[:min(len(names.GetChildren()), remaining)] {
			value := variable.GetValue()
			expression := cmp.Or(value.GetExpression(), variable.GetName())
			binding := &v1.DebugBinding{Name: variable.GetName(), Expression: expression}
			if value.GetType() == "error" {
				binding.Answer = &v1.DebugBinding_Error{Error: value.GetRendered()}
			} else {
				binding.Answer = &v1.DebugBinding_Rendered{Rendered: value.GetRendered()}
			}
			group.Bindings = append(group.Bindings, binding)
			frame.Values[expression] = value
			remaining--
		}
	}
}

// maxDiagnosticBytes bounds the text of a target's refusal that a Frame keeps:
// it is drawn on one row, and a peer chooses its length.
const maxDiagnosticBytes = 256

// maxFrameGroups bounds the scope groups one [ReadFrame] lists.
const maxFrameGroups = 32

func diagnostic(text string) string { return textbound.Truncate(text, maxDiagnosticBytes) }
