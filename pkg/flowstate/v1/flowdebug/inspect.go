package flowdebug

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Asking a paused session a question, without typing at it.
//
// # Why this exists
//
// The session's whole control surface is [Session.dispatch], which takes a
// *line of text*. That is right for a person at a prompt and for a script over
// MCP, and it is the only thing there is — so every other front has to compose
// a command string and then read the session's printed output back to learn
// what happened. A debug adapter doing that is parsing a human-readable
// rendering it also has to keep stable; a Go test doing it cannot ask "what is
// the value" at all, only "what did it print".
//
// That is the gap between "the core is protocol-agnostic" and what is actually
// true, which is that the core is agnostic about *where the text comes from*.
//
// [Session.Complete] already proved the seam: it answers from a console's own
// goroutine, against the scope the run is parked in, by reading the same
// [promptSubject] the prompt is holding. Everything here is that pattern
// applied to the questions the prompt can already answer — so a caller that is
// not a terminal gets values, and the printing verbs become one renderer of
// them rather than the only way to reach them.
//
// # What is not here
//
// Control. [Session.Evaluate] and its neighbours are questions about a session
// that is *already* paused, and answering one changes nothing — which is what
// makes them safe to call from another goroutine while the run is held. Making
// a run *move* from another goroutine is a different problem, because the
// parked [Session.BeforeStep] is blocked reading text, and it is deliberately
// not solved here.

// ErrNotPaused reports that the session is not holding a run, so there is no
// scope to answer against.
//
// A distinct error rather than an empty answer, because "nothing is in scope"
// and "there is no scope" are different facts and only the second is a caller's
// mistake.
var ErrNotPaused = errors.New("flowdebug: the session is not paused at a step")

// Position is where a paused session is holding the run.
type Position struct {
	// Step is the id of the step the run is stopped before, empty at an
	// autopsy — where the run is over and there is no step to be at.
	Step string

	// Kind is what that step is, in the same words `info` prints.
	Kind string

	// Autopsy reports which of the two prompts is holding: a breakpoint before
	// a step, or the account of a case that has already failed.
	Autopsy bool
}

// Paused reports whether the session is holding a run right now, and where.
//
// The boolean rather than a nil Position, because an autopsy is a real pause
// with no step to name — so "there is a position" and "the position has a
// step" have to stay separable.
func (s *Session) Paused() (Position, bool) {
	s.mu.Lock()
	subject := s.at
	s.mu.Unlock()

	if subject.scope == nil {
		return Position{}, false
	}

	return Position{
		Step:    subject.step,
		Kind:    subject.kind,
		Autopsy: subject.autopsy,
	}, true
}

// Evaluate answers one CEL expression against the scope the run is paused in,
// which is what `inspect` prints.
//
// The text is redacted and capped exactly as the printed form is, because a
// caller reaching this way is no more entitled to a secret than one at a
// terminal — the front changes, the withholding does not. The [ref.Val] is
// returned beside it for a caller that needs the value's shape rather than its
// rendering (a debug adapter deciding whether a variable is expandable, say),
// and it is deliberately *not* redacted, because redaction is a property of
// what is displayed and a caller holding a value can already see it.
//
// An expression that does not compile is an ordinary answer here rather than an
// error on the session, for the reason the prompt treats it as one: somebody
// asking questions will ask some that do not parse.
func (s *Session) Evaluate(ctx context.Context, expression string) (string, ref.Val, error) {
	s.mu.Lock()
	subject := s.at
	s.mu.Unlock()

	if subject.scope == nil {
		return "", nil, ErrNotPaused
	}

	libs, err := v1.ProfileLibraries(subject.scope.GetProfile())
	if err != nil {
		return "", nil, fmt.Errorf("cannot inspect: %w", err)
	}

	activation := subject.scope.Activation(ctx)
	if len(subject.extra) > 0 {
		activation = subject.scope.ActivationWith(ctx, subject.extra)
	}

	out, err := v1.DefaultEvaluator().EvalString(ctx, expression, libs, activation)
	if err != nil {
		return "", nil, err
	}

	return capRunes(s.redactText(s.refValText(out)), MaxInspectRunes), out, nil
}

// Names are the bindings the paused run can reach, which is what `scope`
// prints.
//
// Grouped as the prompt groups them rather than flattened, because the grouping
// is the answer: a debug adapter renders one pane per group, and a person
// reading `scope` is looking for which *kind* of name they can reach.
type Names struct {
	// Group is the root the names hang from — `steps`, `vars`, the workflow's
	// declared vars, and the bare bindings an autopsy adds.
	Group string

	// Names are the members of that group, sorted as the prompt lists them,
	// and all of them.
	//
	// [MaxScopeNames] is a property of a line somebody reads rather than of
	// what a run can name, so it is applied by the renderer and not here: a
	// caller filling a variables pane wants every name and does its own
	// paging, and a value surface quietly narrower than the run would be a
	// worse lie than a long line.
	Names []string

	// listing is the command that enumerates these names, for the renderer.
	// Unexported because it is a fact about the text prompt rather than about
	// the scope.
	listing string
}

// Scope lists what the paused run can name.
func (s *Session) Scope() ([]Names, error) {
	s.mu.Lock()
	subject := s.at
	s.mu.Unlock()

	if subject.scope == nil {
		return nil, ErrNotPaused
	}

	return s.scopeNames(subject.scope, subject.extra), nil
}
