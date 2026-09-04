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
// The answer describes the pause the command was delivered into, which only
// the receiver can know: see the control arm of [Session.readCommand].
type controlRequest struct {
	line string
	at   chan<- controlTaken
}

// controlTaken is which pause took a command.
type controlTaken struct {
	// generation identifies that pause, so a caller can wait for a later one
	// without having to guess where it was.
	generation uint64

	// autopsy reports that the pause was the account of a finished run, where
	// every movement verb means "leave" and there is no next stop.
	autopsy bool

	// refused says the boundary declined the command and did not dispatch it.
	//
	// A refusal is answered rather than left to silence, which is what makes
	// this a request with exactly one response instead of a race. A sender that
	// had to *guess* — waiting on the acknowledgement and the session's done
	// channel together — could give up on a command the boundary was about to
	// dispatch, and report it refused when the run had taken it (Codex, #1122,
	// caught by TestACommandRacingCloseIsEitherDispatchedOrRefusedButNotBoth
	// after two narrower fixes had left the window open).
	refused bool
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
	release, err := s.takeControl(ctx, line)
	if err != nil {
		return err
	}
	defer release()

	_, err = s.deliver(ctx, line)

	return err
}

// takeControl refuses a line this session will not accept and then takes the
// serialization slot, returning what gives it back.
//
// Split from [Session.deliver] so that the *movement* verbs can hold the slot
// across their wait as well as their send. Releasing it when the send is
// acknowledged orders deliveries and nothing else: a second command could then
// be consumed at the very pause the first caller was waiting to observe, and
// resume the run out from under it — after which that caller reports a stop two
// ahead of the one it asked for, or none at all. Serializing the send is not
// serializing the movement (Codex, #1122).
//
// Refusals come first, so a command this session was never going to accept is
// answered immediately rather than queued behind somebody else's run.
func (s *Session) takeControl(ctx context.Context, line string) (func(), error) {
	s.mu.Lock()
	controlled := s.controlled
	s.mu.Unlock()

	if !controlled {
		return nil, ErrNotControlled
	}

	// The same bound a console owes on a typed line, applied where this line
	// arrives. [MaxCommandBytes] is enforced by the reader for text, and a
	// caller reaching this method arrives on no reader at all — the identical
	// asymmetry [Session.Evaluate] has for expressions.
	if len(line) > MaxCommandBytes {
		return nil, fmt.Errorf("flowdebug: a command may be %d bytes and this one is %d",
			MaxCommandBytes, len(line))
	}

	// A line is a line. The reader hands the loop one command with no
	// terminator in it, and a caller that could deliver two would be sending
	// something no prompt could have produced — with the second one landing
	// wherever the first left the run.
	if strings.ContainsAny(line, "\r\n") {
		return nil, errors.New("flowdebug: a command is one line, and this one holds a line break")
	}

	// Taken cancellably. Waiting for the slot is part of the wait a caller's
	// context is promised to bound, and a [sync.Mutex] has no cancellable
	// acquire — so a second caller would block *before* reaching any of the
	// selects below, and never return at all if the first command is never
	// consumed (Codex, #1122).
	select {
	case s.controlSlot <- struct{}{}:
		return func() { <-s.controlSlot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrRunOver
	}
}

// deliver hands one line to a parked boundary and reports which pause took it.
//
// The caller holds the serialization slot; see [Session.takeControl] for why
// that is the caller's to decide rather than this function's.
func (s *Session) deliver(ctx context.Context, line string) (controlTaken, error) {
	// Buffered, so the boundary's answer never depends on this goroutine still
	// being here to hear it: a caller whose context expires between the send
	// and the answer must not leave a parked run blocked on writing to nobody.
	at := make(chan controlTaken, 1)

	select {
	case s.control <- controlRequest{line: line, at: at}:
	case <-ctx.Done():
		return controlTaken{}, ctx.Err()
	case <-s.done:
		return controlTaken{}, ErrRunOver
	}

	// The send succeeded, so a boundary holds this request and will answer it —
	// dispatched, or refused. Both paths answer, into a buffered channel that
	// cannot block them, so there is exactly one response to wait for.
	//
	// Deliberately *not* racing the session's done channel here. That was the
	// shape this had, and it is the bug: a close landing between the send and
	// the answer let the sender report [ErrRunOver] for a command the boundary
	// went on to dispatch. Waiting for the answer that is certainly coming is
	// what makes "dispatched and reported, or neither" structural rather than
	// probable. The context stays, because a caller giving up is a different
	// thing from a session that will not answer.
	select {
	case taken := <-at:
		if taken.refused {
			return controlTaken{}, ErrRunOver
		}

		return taken, nil
	case <-ctx.Done():
		return controlTaken{}, ctx.Err()
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
	// Held across the wait as well as the send, which is what makes concurrent
	// movements an ordering rather than an interleaving: see
	// [Session.takeControl].
	//
	// The cost, stated: a `continue` on a run that never stops again holds the
	// slot until the context expires or the session closes, so another
	// caller's command waits that long too. That is the contract — a run has
	// one position — and [Session.Close] is in every select, so a caller
	// tearing the session down is never the one left waiting.
	release, err := s.takeControl(ctx, line)
	if err != nil {
		return Position{}, err
	}
	defer release()

	took, err := s.deliver(ctx, line)
	if err != nil {
		return Position{}, err
	}

	// An autopsy takes a movement verb as a request to leave — `step`,
	// `continue` and `until` all land in the clause that records `quit` and
	// returns — so there is no next stop by construction. Waiting for one
	// would block until [Session.Close] or the context, and on a session
	// reused for a second case it would eventually answer with *that* case's
	// first pause, reported as the result of moving a run that had already
	// finished (Codex, #1122).
	if took.autopsy {
		return Position{}, ErrRunOver
	}

	return s.waitForPause(ctx, took.generation)
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

// WaitForPause returns the pause the session is holding, waiting for one if it
// is not holding a pause now.
//
// The exported face of the wait the movement verbs already do, for the front
// that has to observe a stop *it did not cause*. A debug adapter is that front:
// a client starts a run and then waits to be told where it stopped, so the
// first pause — the one nobody stepped into — has to be reportable. Without
// this the adapter can only announce a stop it asked for, and an editor sits
// with its step buttons disabled watching a run that is, as far as it can tell,
// still going (Codex, #1124).
//
// It returns immediately where the session is already paused, because "wait
// until stopped" and "where is it stopped" are the same question asked at two
// moments and a caller cannot know which one it is asking. [ErrRunOver] once
// the session closes, for the reason that error exists: this package cannot see
// a run end, so a session that finished without stopping again is answered
// rather than waited on forever.
func (s *Session) WaitForPause(ctx context.Context) (Position, error) {
	// Generation zero: any pause at all satisfies it. A pause increments the
	// counter on the way in and again on the way out, so a session between
	// steps has a non-zero generation and no scope, and the wait continues —
	// which is what makes this "the pause it is holding" rather than "the last
	// one it held".
	return s.waitForPause(ctx, 0)
}

// SetBreakpoints replaces the session's breakpoints with the named step ids,
// from any goroutine and whether or not a run is under way.
//
// A direct method rather than a line through [Session.Control], and the reason
// is when a client needs it. Setting a breakpoint is not moving the run — it
// changes where the run will stop, which is a question about the future — so it
// does not need a pause to be delivered into. A debug adapter sets breakpoints
// *before* the run starts, which is what DAP's `configurationDone` exists to
// order, and a command that waited for a boundary would deadlock exactly there:
// no run, no prompt, no way to configure the run you are about to start.
//
// Unconditional, which is the honest limit of a method that can run before
// anything else does. A `break <id> if <expr>` compiles its condition against
// the scope the run is paused in, and before the run there is no scope to
// compile against — so conditions stay with the prompt, where a scope exists.
//
// It replaces rather than adds, because that is what a client means: DAP sends
// the whole set for a source each time one changes, so anything kept from the
// last call is a breakpoint the person removed.
func (s *Session) SetBreakpoints(ids []string) error {
	// Bounded before anything is replaced, so a refusal leaves the session with
	// the set it had rather than half of a new one.
	if len(ids) > MaxBreakpoints {
		return fmt.Errorf("flowdebug: a session may hold %d breakpoints and %d were named",
			MaxBreakpoints, len(ids))
	}

	for _, id := range ids {
		if err := oneArgument(id); err != nil {
			return err
		}
	}

	// Also before anything is replaced, and for the same reason the count is: an
	// id naming no declared step is never armed, so nothing here is ever reported
	// as installed and then silently skipped at run time (#1367). A caller that
	// wants to answer per breakpoint rather than lose the whole set asks
	// [Session.UnknownStep] first and sends only what it holds — which is what
	// the DAP adapter does, because a client sets breakpoints one edit at a time
	// and expects each to come back with its own verdict.
	for _, id := range ids {
		if notice, unknown := s.UnknownStep(id); unknown {
			return fmt.Errorf("flowdebug: %s", notice)
		}
	}

	// One critical section, and that is the correctness of the method rather
	// than a tidiness. The first draft emptied the set under the lock, released
	// it, and then re-added each id through [Session.holdBreakpoint] — which
	// takes the lock per entry, so a run stepping concurrently could look up a
	// step in the window between and find an empty or half-rebuilt set. A step
	// reached there passes a breakpoint the client was just told was installed,
	// and a run that does not come back that way never stops at all
	// (Copilot and Codex, #1124, the same finding from both).
	//
	// "It replaces" therefore has to be true from every other goroutine's point
	// of view and not only from this one's: an id in the old set *and* the new
	// one is never absent, because nothing observes the set between the clear
	// and the fill.
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.breakpoints)
	clear(s.notedUnbound)

	for _, id := range ids {
		// The bound was checked on the way in, over the whole slice, which is
		// the whole-set analogue of the per-entry check [Session.holdBreakpoint]
		// makes: this set replaces rather than adds, so what it may cost is
		// decided before anything is touched. Duplicates only make the result
		// smaller than the number checked.
		s.breakpoints[id] = breakpoint{source: id}
	}

	return nil
}
