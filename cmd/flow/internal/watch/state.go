// Package watch is the TUI `flow watch` draws while a run is going, and the
// state machine both the live view and the plain, line-per-change shape fold
// their answers into.
//
// Split out of cmd/flow by #410: the two files here used to sit in
// `package main` at 1,318 lines between them, which is where a bubbletea
// model's inputs stay implicit — a test could only drive it by importing the
// whole CLI. Everything this package needs from cmd/flow that it cannot get
// for itself — how a status maps to a colour, how a run's position and its
// retries render into prose — arrives through [Deps], set once by the
// caller. What stays in cmd/flow is the cobra command, the flags, and the
// transport: building a client, classifying a refusal against `--address`,
// and writing the run document `--output` asked for are all decisions only
// the CLI's own flags can make, and none of them belongs to a state machine
// that a test should be able to drive with a fake poller and nothing else.
//
// # The state machine, folded into by both shapes
//
// [State] is the run as a watch has seen it, and the decision of when to
// stop. One state machine, folded into by the live view and the plain lines
// alike, so "has anything changed", "is this over", and "has the server been
// quiet too long" cannot get two answers — see [State.Absorb].
package watch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// OutageAllowance is how long the server may be unable to answer before a
// watch gives up on it.
//
// Some allowance is not indulgence: the reason to watch rather than to loop
// `flow get` is that a watch lasts as long as the run, and over an hour a
// server restart or a dropped connection is close to certain. A watch that
// dies on the first one sends people back to the shell loop, which retries
// by construction.
//
// Measured as elapsed time, from the clock, and not as a number of attempts.
// Attempts were the first attempt at this and they were wrong twice over: an
// interval of ten seconds gave up after twenty while reporting thirty, and a
// server that accepted a connection and then said nothing produced no
// attempt at all, which left the allowance never starting and the watch
// hanging until somebody killed it. Whenever a bound is stated in one unit
// and enforced in another, the difference is where the peer gets to live.
const OutageAllowance = 30 * time.Second

// Deps are the rendering functions this package borrows from cmd/flow rather
// than owning a second copy of.
//
// All five are pure functions of a status, a progress, or a slice of pending
// work — no flags, no client, no I/O — so passing them in is only naming
// which package keeps the single implementation. `flow get` and `flow watch`
// share exactly these, on purpose: see positionPath's package doc in
// cmd/flow/get.go for why a position rendered twice is how a watch and a
// `flow get` come to disagree about where a run is. Every field is required;
// a nil field panics on first use rather than rendering silently wrong.
type Deps struct {
	// StatusTone maps a status onto the palette's outcome roles.
	StatusTone func(v1.RunResponse_Status) ui.Tone

	// StatusLabel renders a status the way a column or a pill wants it.
	StatusLabel func(v1.RunResponse_Status) string

	// PositionPath renders a run's progress as bare text, with no styling —
	// what [State] compares across polls to decide a run has moved, and what
	// the live view puts on a line of its own.
	PositionPath func(*v1.RunProgress) string

	// RunPosition renders a run's progress as the themed, sentence-shaped
	// form the plain, line-per-change shape appends to its status line.
	RunPosition func(ui.Theme, *v1.RunProgress) string

	// PendingActivityLines renders what Temporal is retrying, one sentence
	// each, against the moment the answer was observed.
	PendingActivityLines func([]*v1.PendingActivity, time.Time) []string

	// PendingWaitLines renders the gates a run is parked on, one sentence
	// each, against the moment the answer was observed.
	PendingWaitLines func(*v1.RunProgress, time.Time) []string
}

// Poller is the run state a follow renders, behind an interface so both
// shapes can be driven without a server.
//
// The parts most likely to be wrong are the ones a fake can exercise: an
// off-by-one in a step list, a terminal status that does not stop the loop, a
// transient error that ends a watch it should have survived. What a fake
// cannot tell us is whether a real `Get` returns what this package thinks,
// which is why cmd/flow's implementation issues the same request `flow get`
// does rather than a second opinion about it.
type Poller interface {
	Poll(ctx context.Context) (*v1.GetResponse, error)
}

// TransientError marks a poll failure worth asking about again.
//
// cmd/flow classifies a refusal against the connect code and wraps it in this
// before handing it to [State.Absorb], which is the only place the
// distinction matters: everything else here treats a poll failure as one
// thing.
type TransientError struct{ error }

// NewTransientError wraps a poll failure as one worth asking about again.
//
// A constructor rather than a bare composite literal, because the wrapped
// error sits in an unexported field even on the exported type — embedding
// the built-in error interface anonymously names the field "error", and a
// field named after a predeclared identifier is still unexported like any
// other lowercase name, so only this package can set it directly.
func NewTransientError(err error) TransientError { return TransientError{err} }

// Unwrap keeps the refusal underneath reachable, so a remedy printed from
// further up the chain — cmd/flow's nextCommandsFor, for one — can still find
// it inside a "gave up after 30s" sentence.
func (e TransientError) Unwrap() error { return e.error }

// State is the run as a watch has seen it, and the decision of when to stop.
//
// One state machine, folded into by both shapes, because "has anything
// changed", "is this over", and "has the server been quiet too long" must
// not get two answers. The live view and the plain lines then differ only in
// how they render it — which is what keeps a bug fixed in one from surviving
// in the other.
type State struct {
	deps Deps

	workflowID string

	runID  string
	status v1.RunResponse_Status

	// steps are the ids of steps that have produced outputs, sorted.
	steps []string

	// position is where the run has got to, as bare text: the top-level step
	// and the path into it, joined by deps.PositionPath. Empty where the
	// server answered nothing, which is not the same as the beginning.
	position string

	// pending is what Temporal is retrying, already rendered.
	//
	// Rendered at absorb time rather than at draw time because one of the
	// sentences carries a countdown, and the moment to measure it against is
	// the moment the answer was observed rather than whichever redraw
	// happens to display it.
	pending []string

	// waits are the gates the run is parked on, already rendered, for the
	// same reason pending is.
	waits []string

	// pendingKeys is the part of pending that means something has changed:
	// the attempt count and the last failure, one string per activity.
	// Deliberately not the countdown — see [State.Absorb].
	pendingKeys []string

	// waitKeys is the stable identity of the held gates, for the same
	// purpose pendingKeys serves for retries.
	waitKeys []string

	// failure is a failed run's message, empty until there is one.
	failure string

	// response is the last answer the server gave, kept for the shapes that
	// emit the server's own message rather than prose about it.
	response *v1.GetResponse

	// outageSince is when the server was first observed unable to answer,
	// zero once it answers again.
	outageSince time.Time

	// lastError is the most recent failure, so a live view can say why
	// nothing is moving instead of appearing to have frozen.
	lastError error

	// gaveUp records that lastError is why the walk ended rather than
	// something it survived.
	gaveUp bool
}

// NewState begins a walk, optionally already knowing something about the
// run.
//
// `flow run` knows the run exists and what its ids are before it starts
// following, and seeding that matters: a machine-readable caller interrupted
// before the first poll would otherwise be given nothing at all, while a
// durable workload it can no longer name goes on running.
func NewState(deps Deps, workflowID string, known *v1.GetResponse) *State {
	state := &State{deps: deps, workflowID: workflowID}
	if known != nil {
		state.response = known
		state.runID = known.GetRunId()
		state.status = known.GetStatus()
	}

	return state
}

// Progress is what one poll means for a reader.
type Progress struct {
	// Changed reports that a reader has something new to be told. False for
	// a poll that found the run exactly where it was, which is most of them.
	Changed bool

	// Done reports that the walk is over.
	Done bool

	// Err is why it ended, when it ended badly. A Done with no Err is the
	// run having reached a terminal status.
	Err error
}

// Absorb folds one poll result into the state.
//
// at is when the result was observed, taken by the caller rather than read
// here. Both shapes already have it — the plain loop reads the clock, the
// live view has the time on the tick that scheduled the poll — and taking it
// makes the whole state machine a function of its inputs, so a test can
// state exactly when it should give up rather than wait to find out.
func (s *State) Absorb(at time.Time, response *v1.GetResponse, err error) Progress {
	if err != nil {
		return s.absorbError(at, err)
	}

	// A status the schema forbids is a peer that is not answering the
	// question. Treating it as "still running" would wait forever on a
	// server that will never say otherwise.
	if response.GetStatus() == v1.RunResponse_STATUS_UNSPECIFIED {
		return s.stop(fmt.Errorf(
			"the server reported no status for run %q, which is a status the schema does not permit; "+
				"ask `flow get %s` and report it if it persists", s.workflowID, s.workflowID))
	}

	recovered := !s.outageSince.IsZero()
	s.outageSince, s.lastError = time.Time{}, nil

	steps := CompletedSteps(response)
	position := s.deps.PositionPath(response.GetProgress())
	pendingKeys := pendingActivityKeys(response.GetPendingActivities())
	waitKeys := pendingWaitKeys(response.GetProgress())

	// No separate "is this the first answer" flag: the zero value of status
	// is UNSPECIFIED, and an UNSPECIFIED answer never reaches here, so the
	// first answer to get this far always has a status differing from the
	// one held.
	//
	// Position and retry state are grounds on their own, because most of a
	// run's interesting movement happens under one unchanging status. The
	// wait set is its own ground with a sharper edge: a gate opening or
	// closing inside concurrent work moves neither the position (those
	// workers deliberately carry none) nor the pending activities.
	changed := recovered ||
		s.status != response.GetStatus() ||
		s.runID != response.GetRunId() ||
		s.position != position ||
		!slices.Equal(s.pendingKeys, pendingKeys) ||
		!slices.Equal(s.waitKeys, waitKeys) ||
		!slices.Equal(s.steps, steps)

	s.response = response
	s.runID = response.GetRunId()
	s.status = response.GetStatus()
	s.steps = steps
	s.position = position
	s.pending = s.deps.PendingActivityLines(response.GetPendingActivities(), at)
	s.waits = s.deps.PendingWaitLines(response.GetProgress(), at)
	s.pendingKeys = pendingKeys
	s.waitKeys = waitKeys
	if failure := response.GetError(); failure != nil {
		s.failure = failure.GetMessage()
	}

	return Progress{Changed: changed, Done: TerminalStatus(s.status)}
}

// absorbError folds a refused poll in, deciding whether to keep asking.
func (s *State) absorbError(at time.Time, err error) Progress {
	var transient TransientError
	if !errors.As(err, &transient) {
		return s.stop(err)
	}

	first := s.outageSince.IsZero()
	if first {
		s.outageSince = at
	}
	s.lastError = err

	// The measured elapsed time, and the allowance therefore always gets its
	// full span whatever the interval and however long a request took to
	// fail.
	if elapsed := at.Sub(s.outageSince); elapsed >= OutageAllowance {
		return s.stop(fmt.Errorf(
			"gave up watching %q after %s of the server being unable to answer: %w",
			s.workflowID, elapsed.Round(time.Second), err))
	}

	// A change, so the reader is told the server went quiet rather than
	// watching a still screen and guessing. Only the first one: an outage
	// that persists is not news each second.
	return Progress{Changed: first}
}

// stop records why the walk ended and reports it.
func (s *State) stop(err error) Progress {
	s.lastError, s.gaveUp = err, true

	return Progress{Done: true, Err: err}
}

// Line renders the state as one line of prose, for the shape that prints a
// line per change.
func (s *State) Line(theme ui.Theme) string {
	if s.lastError != nil {
		return fmt.Sprintf("%s %s", theme.Pill(ui.ToneWarning, "unreachable"), s.lastError)
	}

	line := fmt.Sprintf("%s workflow %s run %s%s",
		theme.Pill(s.deps.StatusTone(s.status), s.deps.StatusLabel(s.status)), s.workflowID, s.runID,
		s.deps.RunPosition(theme, s.response.GetProgress()))

	for _, pending := range s.pending {
		line += fmt.Sprintf(" (%s)", pending)
	}

	for _, wait := range s.waits {
		line += fmt.Sprintf(" (%s)", wait)
	}

	if len(s.steps) > 0 {
		line += fmt.Sprintf(" after %s", strings.Join(s.steps, ", "))
	}
	if s.failure != "" {
		line += fmt.Sprintf(": %s", s.failure)
	}

	return line
}

// GaveUp reports that the walk ended because the server stopped answering,
// rather than because the run reached a terminal status.
func (s *State) GaveUp() bool { return s.gaveUp }

// LastError is the most recent failure the walk observed.
func (s *State) LastError() error { return s.lastError }

// Response is the last answer the server gave.
func (s *State) Response() *v1.GetResponse { return s.response }

// Status is the run's status as of the last poll folded in.
func (s *State) Status() v1.RunResponse_Status { return s.status }

// WorkflowID is the workflow this walk is following.
func (s *State) WorkflowID() string { return s.workflowID }

// RunID is the run id of the last poll folded in.
func (s *State) RunID() string { return s.runID }

// Position is where the run has got to, as bare text.
func (s *State) Position() string { return s.position }

// Pending is what Temporal is retrying, already rendered.
func (s *State) Pending() []string { return s.pending }

// Waits are the gates the run is parked on, already rendered.
func (s *State) Waits() []string { return s.waits }

// Steps are the ids of steps that have produced outputs, sorted.
func (s *State) Steps() []string { return s.steps }

// Failure is a failed run's message, empty until there is one.
func (s *State) Failure() string { return s.failure }

// OutageSince is when the server was first observed unable to answer, zero
// once it answers again.
func (s *State) OutageSince() time.Time { return s.outageSince }

// pendingActivityKeys reduces the retries to what a reader would call news:
// the attempt count and the last failure, and nothing else — the countdown
// is left out and kept only in the rendered line.
func pendingActivityKeys(pending []*v1.PendingActivity) []string {
	if len(pending) == 0 {
		return nil
	}

	keys := make([]string, 0, len(pending))
	for _, activity := range pending {
		keys = append(keys, fmt.Sprintf("%d\x00%s", activity.GetAttempt(), activity.GetLastFailure()))
	}

	return keys
}

// pendingWaitKeys reduces the held gates to what identifies them across
// polls: which step, where, which signal, policed or not, and the
// deadline's fixed instant rather than the countdown to it.
func pendingWaitKeys(progress *v1.RunProgress) []string {
	waits := progress.GetPendingWaits()
	if len(waits) == 0 {
		return nil
	}

	keys := make([]string, 0, len(waits))
	for _, wait := range waits {
		keys = append(keys, fmt.Sprintf("%s\x00%s\x00%s\x00%t\x00%d",
			wait.GetStepId(), wait.GetPath(), wait.GetSignalName(), wait.GetPoliced(),
			wait.GetDeadline().GetSeconds()))
	}

	return keys
}

// TerminalStatus reports whether a run has stopped moving.
//
// UNSPECIFIED is deliberately absent: it is not a run in progress, it is a
// server that has not answered the question, and Absorb refuses it rather
// than waiting on it.
func TerminalStatus(status v1.RunResponse_Status) bool {
	switch status {
	case v1.RunResponse_STATUS_COMPLETED, v1.RunResponse_STATUS_FAILED,
		v1.RunResponse_STATUS_CANCELED, v1.RunResponse_STATUS_TERMINATED,
		v1.RunResponse_STATUS_TIMED_OUT:
		return true
	default:
		return false
	}
}

// CompletedSteps lists the ids of steps that have produced outputs, in
// order.
//
// Sorted because the outputs arrive in a protobuf map, which has no
// iteration order — an unsorted list would reshuffle itself on every redraw
// and read as though the run were going backwards.
func CompletedSteps(response *v1.GetResponse) []string {
	values := response.GetOutputs().GetStepValues()
	if len(values) == 0 {
		return nil
	}

	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	return ids
}
