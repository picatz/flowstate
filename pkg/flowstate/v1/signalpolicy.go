package flowstatev1

import (
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
// a memo that cannot be read, a policy that cannot be parsed, or a sender that
// matches no rule is refused. There is no ambiguous outcome once a policy is
// declared.

// CheckSignalPolicies reports what is wrong with a workflow's declared signal
// policies, beyond what protovalidate's per-field rules already catch (rule
// counts, string lengths, the subject pattern).
//
// Two things only protovalidate cannot see, because they are facts about a
// *set* rather than about one field: a policy for a signal name the workflow
// never waits for (almost always a misspelling — the name that was meant is
// the one `wait_for_signal:` actually uses), and a rule that matches every
// sender because nothing on it was set (which defeats the point of writing a
// rule, and reads as a mistake rather than an intentional wide-open policy).
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
// map that is empty, or that holds a policy with no rules, or a rule that
// authorizes every sender, because a memo that decodes to any of those is
// corruption, not a legitimately declared policy, and must be denied rather
// than misread as "no policy" (see lifecycle.go's `signalPolicies`).
func CheckSignalPolicyShape(declared map[string]*SignalPolicy) error {
	if len(declared) == 0 {
		return fmt.Errorf("no signal policies are declared")
	}

	for _, name := range slices.Sorted(maps.Keys(declared)) {
		policy := declared[name]

		if len(policy.GetAllow()) == 0 {
			return fmt.Errorf(
				"signals[%q] declares no `allow:` rule, so it authorizes nobody", name)
		}

		for i, rule := range policy.GetAllow() {
			if ruleMatchesEverySender(rule) {
				return fmt.Errorf(
					"signals[%q].allow[%d] sets no subject, namespace, or claims, so it matches every "+
						"sender — which defeats the point of a policy; give it a subject:, a namespace:, "+
						"or claims:, or remove the rule", name, i)
			}
			if subject := rule.GetSubject(); subject != "" && !LooksLikeQualifiedSubject(subject) {
				return fmt.Errorf(
					"signals[%q].allow[%d].subject %q is not \"<issuer>#<subject>\"; a bare subject is "+
						"refused because a subject is only unique within its issuer", name, i, subject)
			}
		}
	}

	return nil
}

// ruleMatchesEverySender reports whether a rule has nothing on it to check —
// the shape that would authorize any sender at all.
func ruleMatchesEverySender(rule *SignalPolicyRule) bool {
	return rule.GetSubject() == "" && rule.GetNamespace() == "" && len(rule.GetClaims()) == 0
}

// SignalPolicyAllows reports whether identity satisfies policy — whether it
// matches at least one of policy's alternative rules.
//
// identity is always the server's own attestation ([SignalSender.identity]),
// never anything a request or a payload supplied; see [SignalPolicyRule] for
// why that is what every field here is checked against. A nil policy is not
// meaningful input here — the caller (`FlowstateServer.Signal`) only reaches
// this function once it already knows a policy exists for the signal name in
// question, since an absent policy is the opt-out zero case handled entirely
// by the caller and never by evaluating an empty rule set.
func SignalPolicyAllows(policy *SignalPolicy, identity *WorkloadIdentity) bool {
	for _, rule := range policy.GetAllow() {
		if signalPolicyRuleMatches(rule, identity) {
			return true
		}
	}
	return false
}

// signalPolicyRuleMatches reports whether identity satisfies every field a
// rule set — the fields left unset impose no requirement, so a rule naming
// only `subject` matches on subject alone.
func signalPolicyRuleMatches(rule *SignalPolicyRule, identity *WorkloadIdentity) bool {
	if subject := rule.GetSubject(); subject != "" {
		if subject != QualifiedSubject(identity.GetIssuer(), identity.GetSubject()) {
			return false
		}
	}

	if namespace := rule.GetNamespace(); namespace != "" {
		if namespace != identity.GetNamespace() {
			return false
		}
	}

	for key, want := range rule.GetClaims() {
		if got, ok := identity.GetClaims()[key]; !ok || got != want {
			return false
		}
	}

	return true
}

// QualifiedSubject renders an issuer and subject the way [SignalPolicyRule.subject]
// is written and matched: "<issuer>#<subject>". Exported so a caller writing a
// rule — `flow`'s own diagnostics, a Flowfile author copying a value out of a
// token — has one place that produces the exact spelling the matcher accepts,
// rather than restating the format by hand and risking a stray separator.
//
// Matching on the joined form, rather than comparing issuer and subject as two
// separate fields, is what makes an unqualified rule (no "#") refused at the
// schema rather than silently matching every issuer's version of a subject —
// see [SignalPolicyRule.subject]'s own comment for why subject alone is
// refused as ambiguous across issuers.
func QualifiedSubject(issuer, subject string) string {
	return issuer + "#" + subject
}

// LooksLikeQualifiedSubject reports whether s has the shape a
// [SignalPolicyRule.subject] requires: something, a single '#', and something
// after it. The schema's own pattern rule enforces this at the wire level;
// this is exported so a diagnostic closer to an author — the Flowfile
// compiler — can explain a malformed subject in its own words rather than
// only through protovalidate's generic pattern-mismatch message.
func LooksLikeQualifiedSubject(s string) bool {
	i := strings.IndexByte(s, '#')
	return i > 0 && i < len(s)-1
}
