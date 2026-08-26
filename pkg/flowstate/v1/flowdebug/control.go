package flowdebug

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// Moving a paused run from another goroutine.
//
// # Why this exists
//
// [Session.Evaluate] and its neighbours answer questions about a session that
// is already paused, and answering one changes nothing — which is what made
// them safe to add. Moving the run is the other half, and it is the half a
// debug adapter cannot do without: an editor's step button, a REPL's `next`,
// a test that wants to walk a run and assert what it saw at each stop.
//
// It was not reachable because [Session.BeforeStep] parks inside
// [Session.readCommand], blocked on a line of text. There is no field to set
// and no method to call that the parked boundary would notice.
//
// # How
//
// Not by reaching around the prompt. [Session.readCommand] already runs its
// reader on a goroutine of its own and selects on channels, because a held run
// has to stay cancellable — so the whole mechanism is one more arm on that
// select, carrying a line. A command delivered here is dispatched by the same
// loop, through the same `dispatch`, as one somebody typed.
//
// That is the property worth protecting. A programmatic front that resumed the
// run by writing `s.mode` directly would be a second implementation of every
// verb, free to drift from the one people type — the same "one meaning written
// down twice" this repository keeps paying for, and the same reason the local
// and durable drivers share their conformance cases rather than each having
// their own.
//
// So there is one vocabulary. [Session.Control] takes a line; the movement
// verbs below are that call plus a wait, and every other verb the prompt
// understands is reachable through it without this file growing a method per
// command.

// ErrRunOver reports that the session stopped without the run stopping again.
//
// It is what a caller waiting for the next pause gets once [Session.Close] has
// been called, and it is the honest answer rather than a hang, because *this
// package cannot see a run end*. [v1.Debugger] is called before each step and
// [v1.RunObserver] after each one; neither has a callback for the run
// finishing, so a session whose run completed cleanly is indistinguishable
// from one whose next step has not arrived yet.
//
// Whoever owns the run does know, and closing the session is how they say so.
// `flow test --debug` already does it when the case returns.
var ErrRunOver = errors.New("flowdebug: the session closed without the run stopping again")

// ErrNotControlled reports a session driven programmatically without
// [Options.Controlled].
//
// Refused rather than accepted-and-ignored: the command would otherwise sit on
// an unread channel until the caller's context expired, which is a hang with a
// timeout on it. See [Options.Controlled] for why the session cannot work this
// out for itself.
var ErrNotControlled = errors.New("flowdebug: this session was not created with Controlled set")

// controlRequest is one command on its way to a parked boundary, with the
// channel the boundary answers on.
//
// The answer is the generation of the pause the command was delivered into,
// which only the receiver can know: see the control arm of
// [Session.readCommand].
type controlRequest struct {
	line string
	at   chan<- uint64
}

// Control delivers one command line to the paused run, from any goroutine.
//
// The same lines the prompt takes, dispatched by the same loop — `step`,
// `continue`, `until <id>`, `break <id>`, `inspect <expr>`, `quit`. What a
// command does, whether it is accepted, and what it prints are all decided
// where they already were.
//
// It returns once the session has taken the line, which is not the same as the
// run having moved: `break` changes nothing about where the run is, and the
// verbs that do move it have their own methods below that wait. A command
// delivered while the run is between stops waits for the next prompt rather
// than being refused, because a caller cannot know which side of a boundary it
// is on — and ctx is what bounds that wait.
//
// One at a time. A run has one position, so two callers moving it at once is
// not something to arbitrate between; serializing makes it an ordering rather
// than two commands interleaved into one prompt.
func (s *Session) Control(ctx context.Context, line string) error {
	_, err := s.deliver(ctx, line)

	return err
}

// deliver is [Session.Control], also reporting which pause took the command.
func (s *Session) deliver(ctx context.Context, line string) (uint64, error) {
	s.mu.Lock()
	controlled := s.controlled
	s.mu.Unlock()

	if !controlled {
		return 0, ErrNotControlled
	}

	// The same bound a console owes on a typed line, applied where this line
	// arrives. [MaxCommandBytes] is enforced by the reader for text, and a
	// caller reaching this method arrives on no reader at all — the identical
	// asymmetry [Session.Evaluate] has for expressions.
	if len(line) > MaxCommandBytes {
		return 0, fmt.Errorf("flowdebug: a command may be %d bytes and this one is %d",
			MaxCommandBytes, len(line))
	}

	// A line is a line. The reader hands the loop one command with no
	// terminator in it, and a caller that could deliver two would be sending
	// something no prompt could have produced — with the second one landing
	// wherever the first left the run.
	if strings.ContainsAny(line, "\r\n") {
		return 0, errors.New("flowdebug: a command is one line, and this one holds a line break")
	}

	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	// Buffered, so the boundary's answer never depends on this goroutine still
	// being here to hear it: a caller whose context expires between the send
	// and the answer must not leave a parked run blocked on writing to nobody.
	at := make(chan uint64, 1)

	select {
	case s.control <- controlRequest{line: line, at: at}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		return 0, ErrRunOver
	}

	select {
	case generation := <-at:
		return generation, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		return 0, ErrRunOver
	}
}

// Step runs the next step and waits for the run to stop again.
func (s *Session) Step(ctx context.Context) (Position, error) {
	return s.move(ctx, "step")
}

// Continue resumes the run and waits for the next breakpoint.
//
// [ErrRunOver] where there is no next one, which is the ordinary end of a run
// that was let go — see that error for why it arrives on [Session.Close]
// rather than on the run's own last step.
func (s *Session) Continue(ctx context.Context) (Position, error) {
	return s.move(ctx, "continue")
}

// Until runs to a named step and waits for the run to stop there.
func (s *Session) Until(ctx context.Context, step string) (Position, error) {
	if err := oneArgument(step); err != nil {
		return Position{}, err
	}

	return s.move(ctx, "until "+step)
}

// oneArgument refuses a step id that could not have been typed.
//
// The prompt splits a line on spaces, so an id carrying one is two arguments by
// the time `dispatch` reads it — a caller would be composing a command
// different from the one it asked for, and `until "a b"` would silently become
// `until a`. Control characters go for the reason every other surface here
// escapes them: they are not part of any id, and they reach a terminal.
//
// A refusal rather than quoting, because the grammar has no quoting: inventing
// one here would be a spelling the prompt does not have.
func oneArgument(step string) error {
	if step == "" {
		return errors.New("flowdebug: no step named")
	}

	for _, r := range step {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf(
				"flowdebug: a step id is one word and %q holds a space or control character; "+
					"the prompt this composes a line for has no way to quote one", step)
		}
	}

	return nil
}

// move delivers one movement command and waits for the run to stop somewhere
// new.
//
// "New" is measured from the pause that *took the command*, reported by the
// boundary that took it, and the alternative — reading the generation here
// before sending — is wrong in a way worth writing down because it is wrong
// only sometimes.
//
// A caller may command a run that has not started: an adapter configures its
// breakpoints and asks for the first stop before handing the workflow to the
// engine. The generation read here is then from before any pause existed, so
// the *first* pause already satisfies "newer than that" — and it is the pause
// the command is about to be consumed by, not the one after it. Whether that
// shows depends on whether this goroutine samples the state before the
// boundary has moved on, which it usually does not: an unbuffered send leaves
// the receiver running and the sender merely runnable, so the boundary is
// several statements ahead by the time this looks. Under GOMAXPROCS=1 it
// always is.
//
// So it is a race that mostly resolves the right way, which is the worst kind
// to leave in a debugger: the failure is a *wrong answer* — a stop reported at
// the position it was commanded from — rather than a crash anything would
// catch. Asking the boundary removes the guess instead of narrowing the window.
// `TestDeliverReportsThePauseThatTookTheCommand` pins the difference directly,
// because the end-to-end walk cannot: it passes under either version on every
// scheduler tried.
func (s *Session) move(ctx context.Context, line string) (Position, error) {
	took, err := s.deliver(ctx, line)
	if err != nil {
		return Position{}, err
	}

	return s.waitForPause(ctx, took)
}

// waitForPause blocks until the session is paused on a generation later than
// after.
func (s *Session) waitForPause(ctx context.Context, after uint64) (Position, error) {
	for {
		s.mu.Lock()
		generation, subject, changed := s.pauseGen, s.at, s.pauseChanged
		s.mu.Unlock()

		if generation > after && subject.scope != nil {
			return Position{
				Step:    subject.step,
				Kind:    subject.kind,
				Autopsy: subject.autopsy,
			}, nil
		}

		select {
		case <-changed:
			// Taken under the lock above, before the state was read, so a
			// change published in between is already waiting here rather than
			// missed. This is the whole reason the signal is a channel that
			// gets replaced rather than one that gets sent on.
		case <-ctx.Done():
			return Position{}, ctx.Err()
		case <-s.done:
			return Position{}, ErrRunOver
		}
	}
}
