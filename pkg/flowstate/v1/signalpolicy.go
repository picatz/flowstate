package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Signal authorization, evaluated where a signal is accepted.
//
// [Workflow.Signals] declares, per signal name, who may deliver it. The server
// enforces it in `FlowstateServer.Signal`, against the [SignalSender] it just
// attested for the request, before the signal ever reaches Temporal — a check
// the workflow performs is a check the workflow can skip, so this is not that.
//
// # The zero case, stated once
//
// A signal name absent from [Workflow.Signals] carries no constraint: any
// authenticated caller who can address the run in its tenant may deliver it,
// exactly as before this field existed. That is deliberate, not an oversight —
// authorization is opt-in per signal name, because failing closed on every
// signal a workflow declares would turn every existing workflow's next `flow
// signal` into a denial the day this shipped. A run whose memo predates this
// field entirely reads exactly the same way, for the same reason: absent means
// unconstrained, both for a name nobody wrote a policy for and for a run that
// could not have carried one at all.
//
// # Fail closed once a policy exists
//
// Once a signal name *does* carry a policy, the rule is ordinary fail-closed:
// a memo that cannot be read, a policy that cannot be parsed, or a sender the
// policy's `allow:` predicate does not admit is refused. There is no ambiguous outcome once a policy is
// declared.

// CheckSignalPolicies reports what is wrong with a workflow's declared signal
// policies, beyond what protovalidate's per-field rules already catch (the
// predicate's length).
//
// Two cross-field facts protovalidate cannot see are checked here: a policy
// for a signal name the workflow never waits for, which is almost always a
// misspelling — the name that was meant is the one `wait_for_signal:` actually
// uses — and each policy's own shape ([CheckPolicyShape]).
func CheckSignalPolicies(wf *Workflow) error {
	declared := wf.GetSignals()
	if len(declared) == 0 {
		return nil
	}

	if err := CheckSignalPolicyShape(declared); err != nil {
		return err
	}

	known := make(map[string]struct{})
	for _, name := range SignalNames(wf) {
		known[name] = struct{}{}
	}

	for _, name := range slices.Sorted(maps.Keys(declared)) {
		if _, ok := known[name]; !ok {
			return fmt.Errorf(
				"signals declares a policy for %q, but no `wait_for_signal:` in this workflow waits for "+
					"that name; a policy for a signal nobody waits for is almost always a misspelling of "+
					"the name a wait actually uses", name)
		}
	}

	return nil
}

// CheckSignalPolicyShape reports what is wrong with a set of signal policies
// on its own terms, without reference to any workflow's steps — everything
// [CheckSignalPolicies] can check about a policy map without also knowing
// which signal names a `wait_for_signal:` actually waits for.
//
// Split out from [CheckSignalPolicies] so a caller that has only the policy
// map — not the workflow it came from — can still ask "is this well
// formed?". `FlowstateServer`'s `signalPolicies` is exactly that caller: it
// decodes a run's memo back into a bare `map[string]*SignalPolicy` with no
// steps beside it (the memo carries only the policy, never the whole
// specification, to keep it small), so it cannot ask the name-existence
// question [CheckSignalPolicies] asks — but it must still refuse a decoded
// map that is empty or that holds a policy with no predicate, because a memo
// that decodes to either is corruption (or a policy frozen by a release that
// still wrote the retired rule list), not a legitimately declared policy, and
// must be denied rather than misread as "no policy" (see lifecycle.go's
// `signalPolicies`).
func CheckSignalPolicyShape(declared map[string]*SignalPolicy) error {
	if len(declared) == 0 {
		return fmt.Errorf("no signal policies are declared")
	}

	for _, name := range slices.Sorted(maps.Keys(declared)) {
		if err := CheckPolicyShape(fmt.Sprintf("signals[%q]", name), declared[name]); err != nil {
			return err
		}
	}

	return nil
}

// CheckPolicyShape is [CheckSignalPolicyShape]'s per-policy body, with the
// stanza that carries the policy named by `where` — `signals["approve"]` for a
// signal name, `debug` for the `Workflow.debug` stanza.
//
// Extracted rather than restated, because a second stanza compiles to the
// same [SignalPolicy] and a second copy of these rules is exactly the "one
// value, written down twice" failure the repository names — the narrowing rule
// in particular is a security check, and a debug policy laxer than a signal
// policy would be that drift in the direction that matters. `where` is a
// label, not a lookup: it appears only in the diagnostic, so an author reading
// a fault about `debug:` is not told about `signals:`.
func CheckPolicyShape(where string, policy *SignalPolicy) error {
	// A policy frozen by an earlier release carries its retired rule list as
	// unknown fields; refuse it by name rather than read it as a weaker policy.
	if len(policy.ProtoReflect().GetUnknown()) > 0 {
		return fmt.Errorf("%s carries a field this version does not know, such as the retired rule list or "+
			"`distinct_from_starter`; run `flow fix` on the Flowfile to write its `allow` predicate", where)
	}

	if policy.GetAllow() == "" {
		return fmt.Errorf(
			"%s declares no `allow:` predicate, so it authorizes nobody", where)
	}

	if err := CheckSignalPolicyExpr(policy.GetAllow()); err != nil {
		return fmt.Errorf("%s.allow is not a usable predicate: %w", where, err)
	}

	return nil
}

// SignalPolicyCheck reports whether identity may deliver a signal governed by
// policy — the whole of what `FlowstateServer.authorizeSignal` enforces once
// it already knows a policy exists for the name in question, factored out so
// a second caller can enforce identically rather than re-derive it.
//
// It is the one function both the durable driver (`server/lifecycle.go`'s
// authorizeSignal, which wraps this in a connect error and adds the memo
// plumbing only the server has) and the local driver ([LocalSignals], through
// a run's own declared policy) call: a second matcher is exactly the drift the
// repository's "one mechanism per concept" rule forbids.
//
// A policy is decided by its one CEL predicate ([signalPolicyExprAllows]) over
// the sender, the starter (when hasStarter) and inputs, and fails closed on
// everything but a clean true. A policy with no predicate authorizes nobody:
// that is what a memo frozen by a release that still wrote the retired rule
// list decodes to, and it is refused rather than read as "no policy".
// hasStarter false leaves `run` unbound, so a predicate that reads the starter
// errors and denies, the same as a run whose memo predates the starter record.
// inputs is the run's bound arguments: nil for a caller that has none, in which
// case a predicate reading them errors and denies.
func SignalPolicyCheck(ctx context.Context, policy *SignalPolicy, identity *WorkloadIdentity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) error {
	return signalPolicyCheck(ctx, "signal", policy, identity, starter, hasStarter, inputs)
}

// signalPolicyCheck is [SignalPolicyCheck] with the stanza's name for its
// refusals: `debug:` is decided by this same function ([DebugPolicyCheck]) and
// says "debug policy", not "signal".
func signalPolicyCheck(ctx context.Context, label string, policy *SignalPolicy, identity *WorkloadIdentity, starter *WorkloadIdentity, hasStarter bool, inputs map[string]*Value) error {
	if policy.GetAllow() == "" {
		return fmt.Errorf("this %s's policy declares no allow predicate, so no sender is authorized", label)
	}

	return signalPolicyExprAllows(ctx, label, policy.GetAllow(), identity, starter, hasStarter, inputs)
}

// QualifiedSubject renders an issuer and subject as "<issuer>#<subject>", the
// `principal` a predicate compares. Exported so a caller — `flow`'s own
// diagnostics, a Flowfile author copying a value out of a token — has one place
// that produces the exact spelling, rather than restating the format by hand
// and risking a stray separator. A subject is only unique within its issuer, so
// the joined form is what keeps two identity providers' "runner" apart.
func QualifiedSubject(issuer, subject string) string {
	return issuer + "#" + subject
}

// LooksLikeQualifiedSubject reports whether s has the shape [QualifiedSubject]
// writes: something, a single '#', and something after it. Exported so a
// diagnostic closer to an author can explain a malformed principal in its own
// words.
func LooksLikeQualifiedSubject(s string) bool {
	i := strings.IndexByte(s, '#')
	return i > 0 && i < len(s)-1 && strings.LastIndexByte(s, '#') == i
}
