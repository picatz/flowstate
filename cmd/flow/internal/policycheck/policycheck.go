// Package policycheck answers "who may act" for a compiled workflow without
// running it: for one identity, which of the workflow's authorization gates
// admit it, decided by the very functions the engine decides them with.
//
// # Why this exists
//
// A Flowfile can name three authorities - `signals:` (who may answer a gate),
// `debug:` (who may pause and inspect a durable run) and `triggers.manual` (who
// may start it) - and until this package the only way to test one was to run the
// workflow as one identity at a time. This asks all of them at once, for as many
// identities as a reader cares to list, and executes no step.
//
// # One decision, not a second one
//
// Nothing here evaluates a predicate. [Evaluate] calls [v1.SignalPolicyCheck],
// [v1.DebugPolicyCheck] and [v1.CheckManualStart] - the functions the server's
// authorization and the local driver call - and reports their answer. A second
// matcher is the drift the architecture forbids (invariant 2), and it would be a
// worse one here than anywhere: a tool that says "admitted" where the engine says
// "refused" is a security tool that lies.
//
// So the fail-closed rules are the engine's own. A starter nobody named is an
// unknown starter, and a predicate reading `run.identity` refuses it; a manual
// start by a caller with no issuer-qualified principal is refused; a predicate
// that errors refuses. The only decision this package makes itself is the one
// the engine also makes by omission: a signal no policy governs is open
// ([Decision.Note] says so, so an author does not mistake it for a gate).
//
// # Refusals are the engine's sentences
//
// A [Decision]'s reason is the engine's refusal verbatim. Those sentences are
// fixed: they never quote a claim, an input or an evaluation error, which is
// what lets this package print them without redacting anything. Nothing here
// formats an identity or an input value into its output either - a sensitive
// input is bound and read by a predicate, and never appears in a result.
//
// # Bounds
//
// A [Matrix] is a file a person wrote and is bounded like one
// ([MaxMatrixBytes], [MaxMatrixRows], and per-row limits), refused before any
// decision is made. Decisions are bounded by the workflow's own size (its gates)
// and by the matrix's rows; each predicate carries the engine's cost and time
// bound.
package policycheck

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// Stanza names which part of a Flowfile a [Gate] belongs to.
type Stanza string

// The stanzas that decide who may act.
const (
	// StanzaSignals is `signals:`, who may answer a wait_for_signal gate.
	StanzaSignals Stanza = "signals"

	// StanzaDebug is `debug:`, who may hold a debug lease on a durable run.
	StanzaDebug Stanza = "debug"

	// StanzaManual is `triggers.manual`, who may start the workflow by hand.
	StanzaManual Stanza = "triggers.manual"
)

// Gate is one authorization decision a workflow declares: a stanza, and for
// `signals:` the signal's name.
type Gate struct {
	Stanza Stanza

	// Name is the signal a StanzaSignals gate governs. Empty for the other two,
	// which are one decision each.
	Name string
}

// String is the gate as an author writes it down: `signals.deploy-approved`,
// `debug`, `triggers.manual`. It is also the key a [Matrix] row's per-gate
// expectation uses.
func (g Gate) String() string {
	if g.Name == "" {
		return string(g.Stanza)
	}

	return string(g.Stanza) + "." + g.Name
}

// compare orders gates the way a reader scans them: signals by name, then
// debug, then manual.
func (g Gate) compare(other Gate) int {
	return cmp.Or(
		cmp.Compare(stanzaRank(g.Stanza), stanzaRank(other.Stanza)),
		cmp.Compare(g.Name, other.Name),
	)
}

func stanzaRank(s Stanza) int {
	switch s {
	case StanzaSignals:
		return 0
	case StanzaDebug:
		return 1
	default:
		return 2
	}
}

// Subject is who is asking, and the facts a predicate may read about the run
// that is hypothetically asked about.
//
// Every field has a meaningful zero value that is the engine's fail-closed
// reading, never a permissive one.
type Subject struct {
	// Sender is the identity attempting the act: the approver of a gate, the
	// debugger of a run, the person starting it. Nil is an unauthenticated
	// caller - the one no `allow:` rule a real deployment writes admits.
	Sender *flowtest.ScriptedIdentity

	// Starter is who started the run, which a predicate reads as
	// `run.identity`. Nil is an *unknown* starter: a predicate that reads
	// `run.identity` errors and refuses, exactly as the engine treats a run
	// whose record of its starter is missing. A non-nil empty identity is a
	// known fact - the run was started by nobody authenticated - and is how
	// `flow run local` models its own starter. Not read by `triggers.manual`,
	// where there is no run yet.
	Starter *flowtest.ScriptedIdentity

	// Inputs are the arguments the run would be started with, as the caller
	// supplied them. They are bound against the workflow's `inputs:` here,
	// exactly as a start binds them, so a predicate reads defaults too. Nil is
	// no arguments, not unbound ones.
	Inputs map[string]*v1.Value

	// Reason is what a `triggers.manual` start that requires one would carry.
	Reason string
}

// Decision is the engine's answer for one gate.
type Decision struct {
	Gate Gate

	// Admitted is true only when the engine's check returned nil.
	Admitted bool

	// Reason is the engine's own refusal sentence, set exactly when Admitted
	// is false. It is fixed text that quotes no claim, input or error; see the
	// package documentation.
	Reason string

	// Note qualifies an admission that is not a policy saying yes: the workflow
	// declares nothing for this gate, so nothing was asked of the sender.
	// Printed beside "admitted" so it is not mistaken for a gate that was
	// passed.
	Note string
}

// Outcome is the word a decision and an expectation are both written in.
type Outcome string

// The two outcomes.
const (
	OutcomeAdmitted Outcome = "admitted"
	OutcomeRefused  Outcome = "refused"
)

// ParseOutcome reads "admitted" or "refused" and nothing else, so an
// expectation spelled "allowed" is refused rather than read as a third thing.
func ParseOutcome(word string) (Outcome, error) {
	switch Outcome(word) {
	case OutcomeAdmitted, OutcomeRefused:
		return Outcome(word), nil
	default:
		return "", errors.New("that is not an outcome; write admitted or refused")
	}
}

// Outcome is the decision's word.
func (d Decision) Outcome() Outcome {
	if d.Admitted {
		return OutcomeAdmitted
	}

	return OutcomeRefused
}

// Gates resolves which gates to check.
//
// With no signal named and neither debug nor manual asked for, every declared
// signal is checked: the names the workflow waits for and the names it writes a
// policy for. Naming anything checks only what is named, so asking for
// `debug` alone answers about `debug:` alone. A signal that is neither waited
// for nor given a policy is refused by name, listing the ones that exist - a
// typo would otherwise be reported as "admitted", the answer for a name nothing
// governs.
//
// The result is ordered and free of duplicates.
func Gates(wf *v1.Workflow, signals []string, debug, manual bool) ([]Gate, error) {
	declared := declaredSignals(wf)

	var gates []Gate

	switch {
	case len(signals) == 0 && !debug && !manual:
		if len(declared) == 0 {
			return nil, errors.New("this workflow declares no signals, so there is nothing to check by default; " +
				"ask for the other stanzas with --debug or --manual")
		}

		for _, name := range declared {
			gates = append(gates, Gate{Stanza: StanzaSignals, Name: name})
		}

	default:
		for _, name := range signals {
			if !slices.Contains(declared, name) {
				return nil, fmt.Errorf("this workflow declares no signal %q (a wait_for_signal step or a `signals:` policy); it declares %s",
					name, listed(declared))
			}
			gates = append(gates, Gate{Stanza: StanzaSignals, Name: name})
		}
	}

	if debug {
		gates = append(gates, Gate{Stanza: StanzaDebug})
	}
	if manual {
		gates = append(gates, Gate{Stanza: StanzaManual})
	}

	slices.SortFunc(gates, Gate.compare)

	return slices.Compact(gates), nil
}

// declaredSignals is every signal name the workflow waits for or governs,
// sorted.
func declaredSignals(wf *v1.Workflow) []string {
	names := map[string]struct{}{}
	for _, name := range v1.SignalNames(wf) {
		names[name] = struct{}{}
	}
	for name := range wf.GetSignals() {
		names[name] = struct{}{}
	}

	return slices.Sorted(maps.Keys(names))
}

func listed(names []string) string {
	if len(names) == 0 {
		return "none"
	}

	return fmt.Sprintf("%q", names)
}

// Evaluate asks the engine whether subject may act at each gate, in the order
// given.
//
// An error is not a refusal: it means the question could not be put - a
// malformed identity, arguments the workflow's `inputs:` do not accept, or a
// context cancelled before the engine finished - and
// no decision is returned, so a caller cannot read half an answer as a verdict.
// A refusal is a [Decision] with Admitted false.
func Evaluate(ctx context.Context, wf *v1.Workflow, gates []Gate, subject Subject) ([]Decision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// The same rule a test file's identities are loaded with, so a half-written
	// issuer and subject is refused as itself here too, rather than as a policy
	// that mystifies by never admitting a subject that does match it.
	if err := subject.Sender.Check("the sender"); err != nil {
		return nil, err
	}
	if err := subject.Starter.Check("the starter"); err != nil {
		return nil, err
	}

	// Bound the way both drivers bind: a predicate reads the defaulted
	// argument, and an absent one is an empty, non-nil map.
	bound, err := v1.BindRunInputs(wf, subject.Inputs)
	if err != nil {
		return nil, err
	}
	if bound == nil {
		bound = map[string]*v1.Value{}
	}

	var (
		sender     = subject.Sender.WorkloadIdentity()
		hasStarter = subject.Starter != nil
		starter    = subject.Starter.WorkloadIdentity()
	)

	decisions := make([]Decision, 0, len(gates))

	for _, gate := range gates {
		decision := Decision{Gate: gate, Admitted: true}

		var refusal error

		switch gate.Stanza {
		case StanzaSignals:
			policy, governed := wf.GetSignals()[gate.Name]
			if !governed {
				decision.Note = "no `signals:` policy governs this signal, so any sender is admitted"
				break
			}

			refusal = v1.SignalPolicyCheck(ctx, policy, sender, starter, hasStarter, bound)

		case StanzaDebug:
			refusal = v1.DebugPolicyCheck(ctx, wf.GetDebug(), sender, starter, hasStarter, bound)

		case StanzaManual:
			if wf.GetTriggers().GetManual() == nil {
				decision.Note = "no `triggers.manual` block, so any caller the server authenticates may start it"
			}

			refusal = v1.CheckManualStart(ctx, wf, sender, principal.Qualified(sender.GetPrincipal().GetIssuer(), sender.GetPrincipal().GetSubject()), subject.Reason, bound)

		default:
			return nil, fmt.Errorf("unknown gate %q", gate)
		}

		// A cancelled run is not a refusal. The engine folds every evaluator
		// error, a cancelled parent context included, into its one fixed
		// refusal sentence, so an interrupted check would otherwise print
		// `refused` - and satisfy `--expect refused`. Asked after the engine
		// answers and before the answer is recorded.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if refusal != nil {
			decision.Admitted = false
			decision.Reason = refusal.Error()
			decision.Note = ""
		}

		decisions = append(decisions, decision)
	}

	return decisions, nil
}
