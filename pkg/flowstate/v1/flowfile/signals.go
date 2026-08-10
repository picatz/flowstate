package flowfile

import (
	"fmt"
	"maps"
	"slices"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `signals:` — who may deliver a named signal to a run, enforced by the
// server before the signal ever reaches the workflow. See
// [v1.Workflow.Signals]'s own doc comment for the mechanism and the zero-case
// trade; this file is only the grammar.
//
// Parsing, writing and validating the block are together here for the same
// reason `triggers.go` keeps its three together: [signalsToYAML] is the
// inverse of the parser below, and a key one of them knows about and the
// other does not is a `flow fmt` that silently deletes an author's policy.
//
// Nothing here binds a name into any expression's scope. Unlike a loop's
// `as:` or a step's own `vars:` — the shapes `flow fix`'s rewriter has to
// know the grammar of, per CLAUDE.md — a signal policy's `subject:`,
// `namespace:` and `claims:` values are plain strings the schema matches
// literally, and `distinct_from_starter:` is a bare boolean; none of them are
// bindings. `subject:` may additionally be written `${...}` — routed to
// [v1.SignalPolicyRule.subject_from] rather than [v1.SignalPolicyRule.subject]
// — but that expression only ever reads `inputs.*`, the ambient root every
// expression in this file can already see, so it still introduces no name a
// rewriter could rebind. There is nothing here for a rewriter to corrupt by
// rebinding a reference, which is why this file carries no `flow fix` scope
// rules the way `fixshadow_test.go` and its kin exist for `as:`/`vars:`/`now`.
//
// # The narrowing check
//
// A rule that writes `subject: ${...}` lets the *caller* choose what value
// the subject resolves to, by choosing what they submit for the input the
// expression reads. Left alone, that would let a caller author their own
// authorization: submit `expected_approver: "attacker#id"` and a rule
// checking only that expression would allow anyone. [validateSignals] refuses
// a rule that sets a per-run subject and nothing else — an interpolated
// field must be accompanied by at least one literal constraint (`namespace:`
// or `claims:`) that the caller cannot influence, so a caller-supplied value
// can only *narrow* a grant the workflow's author already wrote, never
// invent one from nothing.

// signalRuleKeys are what one rule under `allow:` may say.
var signalRuleKeys = []string{"subject", "namespace", "claims"}

// signalPolicyKeys are what one signal's policy may say.
var signalPolicyKeys = []string{"allow", "distinct_from_starter"}

// signals compiles the top-level `signals:` block: one policy per signal
// name, keyed by the name a `wait_for_signal:` elsewhere in the file uses.
func (c *compiler) signals(n ast.Node, path string, r ref) map[string]*v1.SignalPolicy {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	if len(entries) == 0 {
		// Nil rather than an empty map, so `signals: {}` reads back identically to
		// `signals:` absent — the same rule `inputs:` and `vars:` follow, and what
		// keeps [Marshal] an exact inverse.
		return nil
	}

	policies := make(map[string]*v1.SignalPolicy, len(entries))
	for _, e := range entries {
		policyPath := fieldPath(path, e.name)
		policy := c.signalPolicy(e.value, policyPath, ref{path: policyPath, label: "signals." + e.name})
		if policy != nil {
			policies[e.name] = policy
		}
	}

	if len(policies) == 0 {
		return nil
	}

	return policies
}

// signalPolicy compiles one signal name's policy: the `allow:` list of
// alternative rules.
func (c *compiler) signalPolicy(n ast.Node, path string, r ref) *v1.SignalPolicy {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fields(n, path, r, signalPolicyKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping saying who may deliver this signal: `allow:`, a list of rules "+
				"(each with a `subject:`, a `namespace:`, `claims:`, or a combination) and an "+
				"optional `distinct_from_starter:`")
		return nil
	}

	f, found := fields.get("allow")
	if !found {
		c.report(spanOfNode(n), r,
			"declares no `allow:` list, so it authorizes nobody; write at least one rule, "+
				"or remove this signal's policy so the signal keeps today's behavior "+
				"(any authenticated caller in the run's tenant may deliver it)")
		return nil
	}

	allowPath := fieldPath(path, "allow")
	rules := c.signalPolicyRules(f.value, allowPath, ref{path: allowPath, label: r.label + ".allow"})
	if len(rules) == 0 {
		return nil
	}

	policy := &v1.SignalPolicy{Allow: rules}

	if f, found := fields.get("distinct_from_starter"); found {
		distinctPath := fieldPath(path, "distinct_from_starter")
		if distinct, ok := c.boolean(f.value, distinctPath,
			ref{path: distinctPath, label: r.label + ".distinct_from_starter"}); ok {
			policy.DistinctFromStarter = distinct
		}
	}

	return policy
}

// signalPolicyRules compiles the `allow:` list.
func (c *compiler) signalPolicyRules(n ast.Node, path string, r ref) []*v1.SignalPolicyRule {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	sequence, ok := n.(*ast.SequenceNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be a list of rules, each with a `subject:`, a `namespace:`, or `claims:`")
		return nil
	}
	if len(sequence.Values) == 0 {
		c.report(spanOfNode(n), r,
			"is an empty list, so this policy authorizes nobody; write at least one rule, or remove the policy")
		return nil
	}

	rules := make([]*v1.SignalPolicyRule, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		elementPath := indexPath(path, i)
		if rule := c.signalPolicyRule(value, elementPath, ref{path: elementPath, label: r.label}); rule != nil {
			rules = append(rules, rule)
		}
	}

	return rules
}

// signalPolicyRule compiles one rule under `allow:`.
func (c *compiler) signalPolicyRule(n ast.Node, path string, r ref) *v1.SignalPolicyRule {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fields(n, path, r, signalRuleKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping with a `subject:` (\"issuer#subject\"), a `namespace:`, `claims:` (a mapping), or a combination")
		return nil
	}

	rule := &v1.SignalPolicyRule{}
	var anyField bool

	if f, found := fields.get("subject"); found {
		anyField = true
		subjectPath := fieldPath(path, "subject")
		subjectRef := ref{path: subjectPath, label: r.label + ".subject"}
		if subject, exprVal, ok := c.signalSubject(f.value, subjectPath, subjectRef); ok {
			if exprVal != nil {
				// Routed to subject_from rather than subject — see this file's
				// package doc for the narrowing check that applies to a rule
				// shaped this way, and [v1.SignalPolicyRule.subject_from] for
				// when it resolves.
				rule.SubjectFrom = exprVal
			} else {
				if !v1.LooksLikeQualifiedSubject(subject) {
					c.report(spanOfNode(f.value), subjectRef,
						"is %q, which is not \"<issuer>#<subject>\"; a bare subject is refused because a "+
							"subject is only unique within its issuer: two identity providers can mint the "+
							"same subject for different callers, and matching on subject alone would "+
							"authorize the wrong one's signal. Write both, joined by a single '#'",
						subject)
				}
				rule.Subject = subject
			}
		}
	}

	if f, found := fields.get("namespace"); found {
		anyField = true
		namespacePath := fieldPath(path, "namespace")
		if namespace, ok := c.text(f.value, namespacePath,
			ref{path: namespacePath, label: r.label + ".namespace"}); ok {
			rule.Namespace = namespace
		}
	}

	if f, found := fields.get("claims"); found {
		claimsPath := fieldPath(path, "claims")
		claimsRef := ref{path: claimsPath, label: r.label + ".claims"}
		claims := c.stringMap(f.value, claimsPath, claimsRef)
		if len(claims) > 0 {
			anyField = true
			rule.Claims = claims
		}
	}

	if !anyField {
		c.report(spanOfNode(n), r,
			"sets none of `subject:`, `namespace:`, or `claims:`, so it would match every sender; "+
				"give it something to check, or remove the rule")
		return nil
	}

	return rule
}

// signalSubject compiles a rule's `subject:` value, which may be written
// either way a scalar in this grammar can be: a literal "issuer#subject", or
// a whole-value `${...}` fence resolved once at submit against the run's
// bound inputs (routed to [v1.SignalPolicyRule.subject_from] — see this
// file's package doc for the narrowing check that applies to a rule written
// this way).
//
// Returns exactly one of (literal, "") or ("", expression) on success; ok is
// false only after a diagnostic has already been reported, mirroring
// [compiler.text]'s own contract.
func (c *compiler) signalSubject(n ast.Node, path string, r ref) (literal string, exprVal *v1.Value, ok bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return "", nil, false
	}
	c.pos.record(path, spanOfNode(n))

	var raw string
	switch node := n.(type) {
	case *ast.StringNode:
		raw = node.Value
	case *ast.LiteralNode:
		raw = blockText(node)
	default:
		c.report(spanOfNode(n), r, "must be a string, but %s was written here", describeNode(n))
		return "", nil, false
	}

	if inner, fenced := SplitFence(raw); fenced {
		val := c.expression(n, inner, path, r, secretNotEvaluable)
		if val == nil {
			return "", nil, false
		}
		return "", val, true
	}

	if err := fenceError(raw); err != nil {
		c.report(spanOfNode(n), r, "%s", err)
		return "", nil, false
	}

	return raw, nil, true
}

// stringMap compiles a mapping of string to string, such as `claims:`.
func (c *compiler) stringMap(n ast.Node, path string, r ref) map[string]string {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	compiled := make(map[string]string, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		if value, ok := c.text(e.value, valuePath, ref{path: valuePath, label: r.label + "." + e.name}); ok {
			compiled[e.name] = value
		}
	}

	if len(compiled) == 0 {
		return nil
	}

	return compiled
}

// signalsToYAML writes the `signals:` block back out.
//
// Signal names are written in sorted order rather than any order the schema's
// map preserved — a Go map has none — so that `flow fmt` on the same file
// twice in a row produces byte-identical output.
func signalsToYAML(policies map[string]*v1.SignalPolicy) (yaml.MapSlice, error) {
	doc := yaml.MapSlice{}

	for _, name := range sortedPolicyNames(policies) {
		written, err := signalPolicyToYAML(policies[name])
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: name, Value: written})
	}

	return doc, nil
}

// sortedPolicyNames returns policies' keys sorted, so writing them is
// deterministic.
func sortedPolicyNames(policies map[string]*v1.SignalPolicy) []string {
	return slices.Sorted(maps.Keys(policies))
}

// signalPolicyToYAML writes one signal's policy.
//
// distinct_from_starter is written only when true — the compiler's default
// (unset) is false, and Marshal's own rule everywhere else is to omit a
// field that would round-trip to the zero value rather than write it out
// redundantly.
func signalPolicyToYAML(policy *v1.SignalPolicy) (yaml.MapSlice, error) {
	rules := make([]yaml.MapSlice, 0, len(policy.GetAllow()))
	for _, rule := range policy.GetAllow() {
		written, err := signalPolicyRuleToYAML(rule)
		if err != nil {
			return nil, err
		}
		rules = append(rules, written)
	}

	doc := yaml.MapSlice{{Key: "allow", Value: rules}}
	if policy.GetDistinctFromStarter() {
		doc = append(doc, yaml.MapItem{Key: "distinct_from_starter", Value: true})
	}

	return doc, nil
}

// signalPolicyRuleToYAML writes one rule, in the order [signalPolicyRule]
// reads it: subject, namespace, then claims.
//
// A rule's subject is written one of two ways depending on which of
// [v1.SignalPolicyRule.subject] and [v1.SignalPolicyRule.subject_from] the
// compiler set — never both, since [compiler.signalSubject] only ever
// populates one. Getting this wrong in either direction is the asymmetric
// marshal this file's package doc warns about: writing subject_from back as
// a literal would silently drop the expression, turning `flow fmt` into a
// command that deletes half of an author's policy.
func signalPolicyRuleToYAML(rule *v1.SignalPolicyRule) (yaml.MapSlice, error) {
	doc := yaml.MapSlice{}

	switch {
	case rule.GetSubjectFrom() != nil:
		written, err := inputValueToYAML(rule.GetSubjectFrom())
		if err != nil {
			return nil, fmt.Errorf("subject: %w", err)
		}
		doc = append(doc, yaml.MapItem{Key: "subject", Value: written})
	case rule.GetSubject() != "":
		doc = append(doc, yaml.MapItem{Key: "subject", Value: rule.GetSubject()})
	}

	if namespace := rule.GetNamespace(); namespace != "" {
		doc = append(doc, yaml.MapItem{Key: "namespace", Value: namespace})
	}
	if claims := rule.GetClaims(); len(claims) > 0 {
		claimsDoc := yaml.MapSlice{}
		for _, k := range slices.Sorted(maps.Keys(claims)) {
			claimsDoc = append(claimsDoc, yaml.MapItem{Key: k, Value: claims[k]})
		}
		doc = append(doc, yaml.MapItem{Key: "claims", Value: claimsDoc})
	}

	return doc, nil
}

// validateSignals reports what is wrong with the declared `signals:` block
// beyond what the schema's own per-field rules already catch — a policy for a
// name no `wait_for_signal:` waits for, and a rule with nothing on it to
// check. See [v1.CheckSignalPolicies], which this asks where there is a line
// to point at.
func validateSignals(wf *v1.Workflow) Diagnostics {
	declared := wf.GetSignals()
	if len(declared) == 0 {
		return nil
	}

	var ds Diagnostics

	known := make(map[string]struct{})
	for _, name := range v1.SignalNames(wf) {
		known[name] = struct{}{}
	}

	for _, name := range sortedPolicyNames(declared) {
		field := fieldPath("signals", name)

		if _, ok := known[name]; !ok {
			ds = append(ds, Diagnostic{
				Field: field,
				Message: "declares a policy for a signal no `wait_for_signal:` in this workflow waits for; " +
					"this is almost always a misspelling of the name a wait actually uses",
			})
			continue
		}

		for i, rule := range declared[name].GetAllow() {
			interpolated := rule.GetSubjectFrom() != nil

			if rule.GetSubject() == "" && !interpolated && rule.GetNamespace() == "" && len(rule.GetClaims()) == 0 {
				ds = append(ds, Diagnostic{
					Field: indexPath(fieldPath(field, "allow"), i),
					Message: "sets no `subject:`, `namespace:`, or `claims:`, so it matches every sender; " +
						"give it something to check, or remove the rule",
				})
			}
			if subject := rule.GetSubject(); subject != "" && !v1.LooksLikeQualifiedSubject(subject) {
				ds = append(ds, Diagnostic{
					Field: indexPath(fieldPath(field, "allow"), i) + ".subject",
					Message: "is not \"<issuer>#<subject>\"; a bare subject is refused because a subject " +
						"is only unique within its issuer",
				})
			}

			// The narrowing check. A rule whose subject: is an expression lets
			// the caller decide what it resolves to, by choosing what they
			// submit for the input the expression reads — so a rule that
			// interpolates and sets nothing else lets the caller author their
			// own authorization. Requiring a co-resident literal constraint
			// (namespace: or claims:, neither of which a caller can influence)
			// means an interpolated subject can only narrow a grant the
			// workflow's author already wrote, never invent one.
			if interpolated && rule.GetNamespace() == "" && len(rule.GetClaims()) == 0 {
				ds = append(ds, Diagnostic{
					Field: indexPath(fieldPath(field, "allow"), i) + ".subject",
					Message: "is an expression resolved from this run's own inputs, but the rule sets no " +
						"`namespace:` or `claims:` alongside it; an interpolated subject must be narrowed " +
						"by a constraint the caller cannot choose, or the caller would be choosing their " +
						"own authorization; add a `namespace:` or `claims:` entry, or write the subject " +
						"as a literal",
				})
			}
		}
	}

	return ds
}
