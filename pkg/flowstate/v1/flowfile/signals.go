package flowfile

import (
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
// literally, never expressions and never bindings. There is nothing here for
// a rewriter to corrupt by rebinding a reference, which is why this file
// carries no `flow fix` scope rules the way `fixshadow_test.go` and its kin
// exist for `as:`/`vars:`/`now`.

// signalRuleKeys are what one rule under `allow:` may say.
var signalRuleKeys = []string{"subject", "namespace", "claims"}

// signalPolicyKeys are what one signal's policy may say.
var signalPolicyKeys = []string{"allow"}

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
			"is a mapping saying who may deliver this signal: `allow:`, a list of rules — "+
				"each with a `subject:`, a `namespace:`, `claims:`, or a combination")
		return nil
	}

	f, found := fields.get("allow")
	if !found {
		c.report(spanOfNode(n), r,
			"declares no `allow:` list, so it authorizes nobody — write at least one rule, "+
				"or remove this signal's policy so the signal keeps today's behavior "+
				"(any authenticated caller in the run's tenant may deliver it)")
		return nil
	}

	allowPath := fieldPath(path, "allow")
	rules := c.signalPolicyRules(f.value, allowPath, ref{path: allowPath, label: r.label + ".allow"})
	if len(rules) == 0 {
		return nil
	}

	return &v1.SignalPolicy{Allow: rules}
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
		if subject, ok := c.text(f.value, subjectPath, subjectRef); ok {
			if !v1.LooksLikeQualifiedSubject(subject) {
				c.report(spanOfNode(f.value), subjectRef,
					"is %q, which is not \"<issuer>#<subject>\"; a bare subject is refused because a "+
						"subject is only unique within its issuer — two identity providers can mint the "+
						"same subject for different callers, and matching on subject alone would "+
						"authorize the wrong one's signal. Write both, joined by a single '#'",
					subject)
			}
			rule.Subject = subject
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
			"sets none of `subject:`, `namespace:`, or `claims:`, so it would match every sender — "+
				"give it something to check, or remove the rule")
		return nil
	}

	return rule
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
func signalPolicyToYAML(policy *v1.SignalPolicy) (yaml.MapSlice, error) {
	rules := make([]yaml.MapSlice, 0, len(policy.GetAllow()))
	for _, rule := range policy.GetAllow() {
		rules = append(rules, signalPolicyRuleToYAML(rule))
	}

	return yaml.MapSlice{{Key: "allow", Value: rules}}, nil
}

// signalPolicyRuleToYAML writes one rule, in the order [signalPolicyRule]
// reads it: subject, namespace, then claims.
func signalPolicyRuleToYAML(rule *v1.SignalPolicyRule) yaml.MapSlice {
	doc := yaml.MapSlice{}

	if subject := rule.GetSubject(); subject != "" {
		doc = append(doc, yaml.MapItem{Key: "subject", Value: subject})
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

	return doc
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
			if rule.GetSubject() == "" && rule.GetNamespace() == "" && len(rule.GetClaims()) == 0 {
				ds = append(ds, Diagnostic{
					Field: indexPath(fieldPath(field, "allow"), i),
					Message: "sets no `subject:`, `namespace:`, or `claims:`, so it matches every sender — " +
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
		}
	}

	return ds
}
