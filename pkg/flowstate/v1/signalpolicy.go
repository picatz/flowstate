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

	// false: this is a workflow's own declared signals:, checked at submit
	// before BindRunInputs has run — a rule's subject_from is expected to
	// still be an unresolved expression here, and it is not an error for it
	// to be. See [CheckSignalPolicyShape]'s own doc comment for the other
	// caller, which asks the opposite question of a decoded memo.
	if err := CheckSignalPolicyShape(declared, false); err != nil {
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
//
// # requireResolvedSubjects, and the two shapes this function is asked about
//
// This function has exactly two callers, and they ask about two different
// documents that happen to share a Go type. [CheckSignalPolicies] asks about
// a workflow's own declared `signals:` block, checked at submit before
// [BindRunInputs] has run — a rule may legitimately still carry an
// unresolved `subject_from`, written as `subject: ${...}`, that has not been
// evaluated yet. `signalPolicies` in `server/lifecycle.go` asks about a
// policy already decoded back off a run's memo, which resolution has
// *already* run against before anything was frozen there — so a decoded
// policy that still carries a populated `subject_from` is not "not yet
// resolved", it is corruption or a bug that skipped resolution, and is
// refused exactly like any other shape a memo this server wrote would never
// have. requireResolvedSubjects is which question is being asked: false for
// the declared side, true for the decoded side. See
// [SignalPolicyRule.subject_from] for when resolution happens and why the
// enforcement path never evaluates an expression.
func CheckSignalPolicyShape(declared map[string]*SignalPolicy, requireResolvedSubjects bool) error {
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
			if requireResolvedSubjects && rule.GetSubjectFrom() != nil {
				return fmt.Errorf(
					"signals[%q].allow[%d].subject is still an unresolved expression; a policy read back "+
						"off a run's memo must never carry one — resolution happens once, at submit, "+
						"before this policy is frozen, so a populated subject_from here is not a policy "+
						"this server would have written", name, i)
			}
		}
	}

	return nil
}

// ruleMatchesEverySender reports whether a rule has nothing on it to check —
// the shape that would authorize any sender at all. subject_from counts as a
// constraint here even though its content is not yet known: it will resolve
// to a subject before this policy is ever enforced, so a rule that sets it
// (with nothing else) is narrower than "matches every sender" even though it
// is not yet narrower than anything in particular — the narrowing check that
// answers "narrower than what" is the Flowfile compiler's, not this one's.
func ruleMatchesEverySender(rule *SignalPolicyRule) bool {
	return rule.GetSubject() == "" && rule.GetSubjectFrom() == nil &&
		rule.GetNamespace() == "" && len(rule.GetClaims()) == 0
}

// ResolveSignalPolicySubjects resolves every rule's subject_from expression
// against inputs — the run's own bound arguments, and the only names such an
// expression may reference, since resolution happens at submit before any
// step has run — writing the result into that rule's subject and clearing
// subject_from.
//
// Returns a new map; wf.GetSignals() itself is never mutated, so a
// specification kept for replay or re-use is untouched by resolving a
// policy that will be frozen into one particular run's memo.
//
// Called once, by the server's `signalPolicyMemoEntry` — the one function
// [FlowstateServer.Run] and [FlowstateServer.CreateSchedule] both use to
// turn a workflow's declared policy into the memo entry that governs that
// run — after [BindRunInputs] has established the inputs both submit paths
// bind through. That "one function, two callers" shape is what keeps a
// scheduled run's resolution identical to a direct run's, the same
// discipline [BindRunInputs]'s own package doc states for input binding
// itself.
//
// The result the enforcement path (`authorizeSignal`/[SignalPolicyAllows])
// ever sees never contains an expression: it evaluates nothing, because
// resolution has already happened here, once, before the policy was ever
// stored. [CheckSignalPolicyShape] with requireResolvedSubjects true is what
// makes that a checked fact rather than an assumption — a decoded policy
// that still carries subject_from is refused rather than silently evaluated
// on a future signal delivery.
func ResolveSignalPolicySubjects(ctx context.Context, wf *Workflow, inputs map[string]*Value) (map[string]*SignalPolicy, error) {
	signals := wf.GetSignals()
	if len(signals) == 0 {
		return nil, nil
	}

	scope := &Scope{Profile: wf.GetProfile(), Inputs: inputs}

	resolved := make(map[string]*SignalPolicy, len(signals))
	for _, name := range slices.Sorted(maps.Keys(signals)) {
		policy := signals[name]

		allow := make([]*SignalPolicyRule, len(policy.GetAllow()))
		for i, rule := range policy.GetAllow() {
			exprValue := rule.GetSubjectFrom()
			if exprValue == nil {
				allow[i] = rule
				continue
			}

			subject, err := evalSubjectFrom(ctx, exprValue, scope)
			if err != nil {
				return nil, fmt.Errorf("signals[%q].allow[%d].subject: %w", name, i, err)
			}

			allow[i] = &SignalPolicyRule{
				Subject:   subject,
				Namespace: rule.GetNamespace(),
				Claims:    rule.GetClaims(),
				// SubjectFrom deliberately left unset: resolution is exactly the
				// act of replacing it with a literal, and a rule carried past
				// this point must never hold both.
			}
		}

		resolved[name] = &SignalPolicy{
			Allow:               allow,
			DistinctFromStarter: policy.GetDistinctFromStarter(),
		}
	}

	return resolved, nil
}

// evalSubjectFrom evaluates one rule's subject_from expression to the
// literal it resolves to, refusing anything that is not a string shaped
// like [SignalPolicyRule.subject] requires.
//
// Evaluated against scope — inputs only, no step outputs, no run identity —
// which is the whole of what a submit-time expression may legitimately see:
// nothing has executed yet. Through [DefaultEvaluator], so this resolution
// is bounded by [DefaultCostLimit] exactly as every other CEL evaluation in
// this codebase is; there is no second, unbounded evaluation path here.
func evalSubjectFrom(ctx context.Context, value *Value, scope *Scope) (string, error) {
	kind, ok := value.GetKind().(*Value_Expr)
	if !ok {
		return "", fmt.Errorf("subject_from is %T, not an expression", value.GetKind())
	}

	out, err := DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, scope.Activation(ctx))
	if err != nil {
		return "", fmt.Errorf("evaluating: %w", err)
	}

	s, ok := out.Value().(string)
	if !ok {
		return "", fmt.Errorf("must evaluate to a string, got %s", out.Type())
	}

	if !LooksLikeQualifiedSubject(s) {
		return "", fmt.Errorf(
			"resolved to %q, which is not \"<issuer>#<subject>\"; a bare subject is refused because a "+
				"subject is only unique within its issuer", s)
	}

	return s, nil
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

// SignalPolicyCheck reports whether identity may deliver a signal governed by
// policy — the whole of what [FlowstateServer.authorizeSignal] enforces once
// it already knows a policy exists for the name in question, factored out so
// a second caller can enforce identically rather than re-derive it.
//
// It is the one function both the durable driver (`server/lifecycle.go`'s
// authorizeSignal, which wraps this in a connect error and adds the memo
// plumbing only the server has) and the local driver ([LocalSignals], through
// a run's own declared and locally-resolved policy) call — see CLAUDE.md's
// "one function, two callers" rule. Before this existed, local delivery
// called [SignalPolicyAllows] alone or not at all, which checked the `allow:`
// rules but never `distinct_from_starter` — a second matcher was exactly the
// drift the rule warns about, so this folds both checks into the one place
// either caller reaches.
//
// starter/hasStarter follow [SignalPolicy.distinct_from_starter]'s own
// fail-closed rule, generalized past the server's one source (a run's memo)
// to whatever a caller's own notion of "this run's starter" is: hasStarter
// false means nothing here can prove separation, and is refused exactly like
// a run whose memo predates the starter record — never treated as
// "unconstrained."
func SignalPolicyCheck(policy *SignalPolicy, identity *WorkloadIdentity, starter *WorkloadIdentity, hasStarter bool) error {
	if !SignalPolicyAllows(policy, identity) {
		return fmt.Errorf("the sender does not match any rule this signal's policy declares")
	}

	// distinct_from_starter is ANDed onto whichever rule the sender just
	// satisfied, un-bypassable by any rule in allow — see
	// [SignalPolicy.distinct_from_starter]'s own doc comment for why it lives
	// at the policy level rather than as one more alternative a rule could
	// opt out of.
	if policy.GetDistinctFromStarter() {
		if !hasStarter {
			return fmt.Errorf(
				"this signal requires a sender distinct from the run's own starter, but this run has no " +
					"starter identity recorded to compare against; a run that cannot prove separation does not get it")
		}

		senderQualified := QualifiedSubject(identity.GetIssuer(), identity.GetSubject())
		starterQualified := QualifiedSubject(starter.GetIssuer(), starter.GetSubject())
		if senderQualified == starterQualified {
			return fmt.Errorf(
				"this signal requires a sender distinct from the run's own starter, and the sender is the run's own starter")
		}
	}

	return nil
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
