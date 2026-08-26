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
// Both halves of the answer are redacted, and that is the whole point of the
// method existing rather than a caution around it. A caller reaching a session
// this way is no more entitled to a secret than one at a terminal: the front
// changes and the withholding does not.
//
// The first draft returned the raw [ref.Val] beside the redacted text, on the
// reasoning that redaction is a property of what is *displayed*. That is wrong
// here, and wrong in the one direction that matters. `flow test` installs
// [Session.SetValueRedactor] precisely so that a structured value a debugger
// hands out does not carry a sensitive input or a resolved secret
// (`flowtest/run.go:755-790`), and an adapter expanding a variable in a pane
// reads exactly that value — so returning it raw would open, on a new surface,
// the hole the seam exists to close (Codex, #1120).
//
// The [ref.Val] is still returned, because a caller does need the value's shape
// to decide whether a variable is expandable. It is the redacted one — or, on a
// session that has a text redactor and no value redactor, nothing at all. See
// [Session.redactedRefVal] for why that case fails closed rather than handing
// back what it cannot redact.
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

	// Bounded before the parser sees it. A console reader owes
	// [MaxCommandBytes] and enforces it on the way in, so the text path was
	// bounded by the surface it arrives on — and a caller reaching this method
	// arrives on no such surface. [v1.DefaultCostLimit] bounds *evaluation*,
	// which is work that happens after a parse, so an expression large enough
	// to be a problem is one the cost limit never gets to see (Codex, #1120).
	if len(expression) > MaxCommandBytes {
		return "", nil, fmt.Errorf(
			"%w: an expression may be %d bytes and this one is %d",
			ErrExpressionTooLarge, MaxCommandBytes, len(expression))
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

	return capRunes(s.redactText(s.refValText(out)), MaxInspectRunes), s.redactedRefVal(out), nil
}

// ErrExpressionTooLarge reports an expression past [MaxCommandBytes].
//
// The same bound a console reader enforces on a typed line, because the two are
// the same resource reached two ways — and named rather than anonymous so a
// caller can tell a refusal to look at an expression from an expression that
// did not compile.
var ErrExpressionTooLarge = errors.New("flowdebug: expression too large")

// redactedRefVal is out through the installed value redactor, or nothing.
//
// Three cases, and the middle one is the reason this is a function rather than
// a call.
//
// With a value redactor installed, the value goes through the same seam the
// structured printing path uses, so there is one answer to "may this value be
// handed out" rather than one per surface.
//
// With *no* redactor of any kind, the value is returned untouched rather than
// round-tripped through the adapter: a conversion nobody asked for is a way for
// a type to change on a path that was meant to be a no-op.
//
// With a text redactor and no value redactor, nothing is returned. The session
// has been told there is something to withhold and has no way to withhold it
// structurally, and a component that allows when it cannot decide will
// eventually allow everything — CLAUDE.md's fail-closed rule, which applies
// here because the two redactors are installed independently
// (`flowtest/run.go:755-790` sets them in two separate blocks behind two
// separate interface assertions). The caller still gets the redacted *text*, so
// this withholds a representation rather than the answer.
func (s *Session) redactedRefVal(out ref.Val) ref.Val {
	s.mu.Lock()
	redactValue, redactText := s.redactValue, s.redact
	s.mu.Unlock()

	switch {
	case redactValue != nil:
		return v1.TypeAdapter.NativeToValue(redactValue(out.Value()))
	case redactText != nil:
		return nil
	default:
		return out
	}
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
