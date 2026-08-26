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
// session that has a text redactor and no value redactor, nothing at all: see
// the withholding branch below for why that case fails closed rather than
// handing back what it cannot redact.
//
// An expression that does not compile is an ordinary answer here rather than an
// error on the session, for the reason the prompt treats it as one: somebody
// asking questions will ask some that do not parse. The error is redacted too —
// see [withheld], because an error names what caused it and what caused it can
// be the value being withheld.
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
		return "", nil, withheld(subject.redactText, err)
	}

	// Redacted with the redactors this *pause* began under, taken from the
	// subject snapshotted at the top rather than read from the session now.
	// Evaluation takes time, and a console exiting the pause meanwhile clears
	// them — so reading them here would hand back exactly what they existed to
	// withhold. See [promptSubject.redactText].
	//
	// One conversion, shared with the printing path, because a second one that
	// reached for [ref.Val.Value] directly would redact a scalar and hand a map
	// straight through. See [redactedNative].
	native, converted := redactedNative(out, subject.redactValue)
	if !converted {
		// Nothing structured to offer for a value the conversion cannot read,
		// and the text redactor still covers what comes back as prose.
		return capRunes(applyText(subject.redactText, fmt.Sprint(out.Value())), MaxInspectRunes), nil, nil
	}

	text := capRunes(applyText(subject.redactText, nativeText(native)), MaxInspectRunes)

	// The structured half is withheld entirely when this session cannot redact
	// one. Told there is something to withhold, with no way to withhold it
	// structurally, and a component that allows when it cannot decide will
	// eventually allow everything — CLAUDE.md's rule, which bites here because
	// the two redactors are installed independently
	// (`flowtest/run.go:755-790`). The redacted text still comes back, so what
	// is withheld is a representation rather than the answer.
	if subject.redactValue == nil && subject.redactText != nil {
		return text, nil, nil
	}

	return text, v1.TypeAdapter.NativeToValue(native), nil
}

// withheld is err with the pause's text redactor applied to its message.
//
// A CEL runtime error interpolates the value that caused it — `hours(n)` past
// the duration ceiling prints n itself (`celenv.go:642-645`) — so an error is a
// way for a value to leave, and the prompt has always known this: it prints one
// through `printfTone`, which redacts. This surface returned the error raw, so
// the same failing expression withheld its value when typed and disclosed it
// when asked for (Codex, #1120).
//
// Rebuilt rather than wrapped, which is the part worth stating. Wrapping keeps
// the original reachable through [errors.Unwrap], and the whole message is what
// had to be withheld — the same leak CLAUDE.md records for Temporal's failure
// converter, which walks the unwrap chain and persists every level. So the
// redacted sentence is the whole of what comes back.
//
// With no redactor installed the error is returned exactly as it is: identity
// is preserved precisely where there is nothing to withhold, and the sentinels
// this method returns are all returned before evaluation, so none of them
// passes through here.
func withheld(redact func(string) string, err error) error {
	if redact == nil || err == nil {
		return err
	}

	return errors.New(redact(err.Error()))
}

// applyText is text through redact, or text.
func applyText(redact func(string) string, text string) string {
	if redact == nil {
		return text
	}

	return redact(text)
}

// ErrExpressionTooLarge reports an expression past [MaxCommandBytes].
//
// The same bound a console reader enforces on a typed line, because the two are
// the same resource reached two ways — and named rather than anonymous so a
// caller can tell a refusal to look at an expression from an expression that
// did not compile.
var ErrExpressionTooLarge = errors.New("flowdebug: expression too large")

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
