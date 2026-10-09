package flowfile

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

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
// Nothing here binds a name into any expression's scope: the one `allow:`
// predicate reads `sender`, `run` and `inputs`, the ambient roots every
// expression in this file can already see, so it introduces no name a rewriter
// could rebind. There is nothing here for a rewriter to corrupt by rebinding a
// reference, which is why this file carries no `flow fix` scope rules the way
// `fixshadow_test.go` and its kin exist for `as:`/`vars:`/`now`.
//
// # The retired rule list
//
// Before the predicate, a policy was a list of rules (`subject:`, `namespace:`,
// `claims:`) with an optional `distinct_from_starter:`. That grammar is gone:
// the parser refuses it with a sentence that says to run `flow fix`, which
// rewrites each list into the predicate that says the same ([fixallow.go]). The
// refusals name the keys and never echo the values written under them.
//
// # The narrowing check
//
// A predicate that reads `inputs` lets the *caller* influence the verdict by
// choosing what they submit. [v1.CheckSignalPolicyExpr] refuses one that reads
// `inputs` without also reading `sender.identity.claims` or `run.identity`, so
// a caller-supplied value can only *narrow* a grant the workflow's author
// already wrote, never invent one from nothing.

// signalPolicyKeys are what one signal's policy may say.
var signalPolicyKeys = []string{"allow"}

// retiredPolicyKeys are the keys a policy used to have beside `allow:`, and
// what to write instead. Held back from [signalPolicyKeys] so the sentence is
// the one reported, rather than an unknown-key suggestion for a typo nobody made.
var retiredPolicyKeys = map[string]string{
	"distinct_from_starter": "`distinct_from_starter:` is retired: compare the sender with the run's starter " +
		"inside the one `allow:` predicate, as `sender.identity.principal != run.identity.principal`. " +
		"Run `flow fix` to rewrite this file",
}

// retiredAllowList is reported for an `allow:` written as a list of rules.
const retiredAllowList = "is a list of rules, which is retired: write one `${...}` predicate over " +
	"`sender.identity` and `run.identity`, with alternatives joined by `||`. Run `flow fix` to rewrite this file"

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

// signalPolicy compiles one signal name's policy: the one `allow: ${...}`
// predicate that decides who may act.
func (c *compiler) signalPolicy(n ast.Node, path string, r ref) *v1.SignalPolicy {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fieldsRetiring(n, path, r, signalPolicyKeys, retiredPolicyKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping saying who may deliver this signal: `allow:`, one `${...}` predicate over "+
				"`sender.identity` (and `run.identity`, the starter)")
		return nil
	}

	f, found := fields.get("allow")
	if !found {
		if fields.retired > 0 {
			// The retired key's sentence is the diagnostic; a second one about
			// the missing predicate would be noise on a file `flow fix` repairs.
			return nil
		}

		// The remedy is where the two stanzas sharing this grammar differ:
		// removing a signal's policy opens the signal, and removing `debug:`
		// closes debugging entirely (see [v1.Workflow.Debug]).
		remedy := "or remove this signal's policy so the signal keeps today's behavior " +
			"(any authenticated caller in the run's tenant may deliver it)"
		if path == "debug" {
			remedy = "or remove the `debug:` stanza, which leaves the run not debuggable at all " +
				"(every pause ask is refused)"
		}
		c.report(spanOfNode(n), r,
			"declares no `allow:` predicate, so it authorizes nobody; write `allow: ${...}`, %s", remedy)
		return nil
	}

	allowPath := fieldPath(path, "allow")
	allowRef := ref{path: allowPath, label: r.label + ".allow"}

	resolved := c.resolveQuiet(f.value)
	if !isScalarNode(resolved) {
		c.pos.record(allowPath, spanOfNode(resolved))
		c.report(spanOfNode(resolved), allowRef, "%s", retiredAllowListMessage(resolved))

		return nil
	}

	expression, source, ok := c.signalPolicyPredicate(resolved, allowPath, allowRef)
	if !ok {
		return nil
	}

	return &v1.SignalPolicy{Allow: expression, AllowSource: source}
}

// retiredAllowListMessage says why a non-string `allow:` is refused. A list is
// the retired rule list and gets the migration sentence; anything else is just
// the wrong shape. Neither echoes what was written.
func retiredAllowListMessage(n ast.Node) string {
	if _, isList := n.(*ast.SequenceNode); isList {
		return retiredAllowList
	}

	return "must be one `${...}` predicate, but " + describeNode(n) + " was written here"
}

// isScalarNode reports whether n is a string, the shape `allow: ${...}` is
// written in. Block scalars are strings too.
func isScalarNode(n ast.Node) bool {
	switch n.(type) {
	case *ast.StringNode, *ast.LiteralNode:
		return true
	default:
		return false
	}
}

// signalPolicyPredicate compiles `allow: ${...}`: the one CEL predicate that
// decides who may act, kept as the source text between the fence.
//
// Only its syntax is checked here, with the position of the fault; its scope,
// its type and the narrowing rule are [v1.CheckSignalPolicyExpr]'s, asked by
// [validatePolicyRules] so the file the validator accepts is the policy the
// server compiles. The text is stored trimmed and unnormalized, so [Marshal]
// writes back exactly what was read.
//
// The one reader for all three stanzas that take `allow: ${...}` (`signals:`,
// `debug:`, `triggers: manual:`).
func (c *compiler) signalPolicyPredicate(n ast.Node, path string, r ref) (expression string, source *string, ok bool) {
	c.pos.record(path, spanOfNode(n))

	var raw string
	switch node := n.(type) {
	case *ast.StringNode:
		raw = node.Value
	case *ast.LiteralNode:
		raw = blockText(node)
	}

	inner, fenced := SplitFence(strings.TrimSpace(raw))
	if !fenced {
		if err := fenceError(raw); err != nil {
			c.report(spanOfNode(n), r, "%s", err)
			return "", nil, false
		}
		c.report(spanOfNode(n), r,
			"is a string that is not a `${...}` expression; write the whole predicate as one `${...}` "+
				"(for example `${sender.identity.claims.team == \"release-managers\"}`)")
		return "", nil, false
	}

	expression = strings.TrimSpace(inner)
	span := spanWithin(n, inner)
	c.recordExpr(path, span)

	if val := v1.NewExpr(expression); val.Error() != nil {
		at, msg := celFailure(val, span, expression)
		c.report(at, r, "is not a valid expression: %s", msg)
		return "", nil, false
	}

	return c.expandPredicate(expression, span, r)
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
	return yaml.MapSlice{{Key: "allow", Value: fencedToYAML(cmp.Or(policy.GetAllowSource(), policy.GetAllow()))}}, nil
}

// validateDebug reports what is wrong with the declared `debug:` stanza.
//
// It asks [validatePolicyRules] the same questions [validateSignals] asks of a
// signal's policy, because the stanza compiles to the same [v1.SignalPolicy]
// and a debug policy laxer than a signal policy for the same words would be
// drift in the direction that matters — the narrowing check in particular is
// what stops whoever started a run from naming themselves as the caller
// allowed to pause it.
//
// The one question it does not ask is the one [validateSignals] opens with: a
// signal policy for a name nothing waits for is a misspelling, and there is no
// analogue here, because what `debug:` governs is the run rather than a name
// written somewhere else in the file.
func validateDebug(wf *v1.Workflow) Diagnostics {
	policy := wf.GetDebug()
	if policy == nil {
		// Absent is a workflow that is simply not debuggable durably
		// ([v1.Workflow.Debug]'s fail-closed zero case), which is the state
		// every workflow in the tree is in and not a fault to report.
		return nil
	}

	return validatePolicyRules("debug", policy, v1.SensitiveInputNames(wf))
}

// reservedSignalWaitDiagnostic reports a wait on a name the engine reserved, or
// nothing when the name is the author's.
//
// Asked per waiting step rather than over [v1.SignalNames], because that
// function answers with names and a diagnostic has to point at a *place*: the
// step id and the key the author actually wrote are what turn this from "some
// step in this file" into an underlined token. The two spellings are named
// apart for the reason `validateWait`'s `timeout:` already is — a bare field
// makes `Locate` find the outer span first.
//
// Reported here as well as refused at submit ([v1.CheckReservedSignalNames]),
// because a diagnostic in an author's editor with a line and a column is the
// version of this refusal somebody can act on — the standard
// `flowfile/validate.go` sets. The rule is a property of the file rather than of
// a deployment: which names the engine owns is decided by this build, not by
// configuration.
func reservedSignalWaitDiagnostic(id string, wait *v1.Wait) (Diagnostic, bool) {
	name, field := wait.GetSignal().GetName(), "wait_for_signal.name"
	if name == "" {
		name, field = wait.GetSignalBatch().GetName(), "wait_for_signals.name"
	}

	if name == "" || !v1.IsReservedSignalName(name) {
		return Diagnostic{}, false
	}

	return Diagnostic{
		Step:  id,
		Field: field,
		Message: "names a signal beginning " + v1.ReservedSignalPrefix +
			", which belongs to the engine; an ask to pause this run for debugging travels on one " +
			"of those channels and would answer this wait instead. Rename the signal",
	}, true
}

// validateReservedSignalNames reports a `signals:` policy declared for a name
// the engine reserved.
//
// The waiting half is [reservedSignalWaitDiagnostic], reported from
// `validateWait` where the step is known. This half stays whole-workflow because
// the map key *is* the place: `signals.<name>` is a path the position index
// already carries.
func validateReservedSignalNames(wf *v1.Workflow) Diagnostics {
	var ds Diagnostics

	for _, name := range sortedPolicyNames(wf.GetSignals()) {
		if !v1.IsReservedSignalName(name) {
			continue
		}

		ds = append(ds, Diagnostic{
			Field: fieldPath("signals", name),
			Message: "declares a policy for a name beginning " + v1.ReservedSignalPrefix +
				", which belongs to the engine; who may debug this workflow is the `debug:` stanza, " +
				"not a policy under a reserved signal name",
		})
	}

	return ds
}

// validatePolicyRules is the per-policy half of [validateSignals], asked of one
// policy under the field path that carries it.
//
// Extracted when `debug:` became a second stanza compiling to the same message
// — one checker, two call sites, the rule the repository states for exactly the
// pair of surfaces that would otherwise drift apart. What is asked is
// [v1.CheckSignalPolicyExpr], which is also what the server asks at submit and
// again at every delivery: unknown names and fields in the closed scope, a bool
// result, and the narrowing rule.
//
// sensitive is the workflow's `sensitive:` inputs: a predicate's inputs are
// recorded with the run, so one that reads such an input is refused
// ([v1.CheckPolicyInputsNotSensitive], which the server asks again at submit).
func validatePolicyRules(field string, policy *v1.SignalPolicy, sensitive map[string]bool) Diagnostics {
	if err := v1.CheckSignalPolicyExpr(policy.GetAllow()); err != nil {
		return Diagnostics{{
			Field:   fieldPath(field, "allow"),
			Message: "is not a usable `${...}` predicate: " + err.Error(),
		}}
	}

	if err := v1.CheckPolicyInputsNotSensitive(field, policy, sensitive); err != nil {
		return Diagnostics{{
			Field:   fieldPath(field, "allow"),
			Message: "is not a usable `${...}` predicate: " + err.Error(),
		}}
	}

	return nil
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

	sensitive := v1.SensitiveInputNames(wf)

	for _, name := range sortedPolicyNames(declared) {
		field := fieldPath("signals", name)

		if _, ok := known[name]; !ok {
			ds = append(ds, Diagnostic{
				Field: field,
				Message: "declares a policy for a signal no `wait_for_signal:` or `wait_for_signals:` in this workflow waits for; " +
					"this is almost always a misspelling of the name a wait actually uses",
			})
			continue
		}

		ds = append(ds, validatePolicyRules(field, declared[name], sensitive)...)
	}

	return ds
}

// validateQuorums reports a `quorum:` that its own gate's `signals:` policy makes
// impossible to meet.
//
// A distinct quorum needs `approve:` different senders, and when the policy for
// the signal is a *closed* predicate - a literal `sender.identity.principal in
// [...]`, or an `||` of `sender.identity.principal == "..."` comparisons, each
// possibly narrowed by `&&` - the senders who can ever reach the wait are those
// principals, so an `approve:` above their count can never be satisfied and the
// run would sit at the gate until its `timeout:`. Reported where somebody can
// fix it, rather than discovered as a gate that never opens.
//
// # Conservative, because a false refusal is worse than a missed one
//
// A policy is closed only when it can be counted exactly
// ([v1.SignalPolicyClosedPrincipals]). Anything that admits a sender this check
// cannot enumerate leaves the quorum alone: a predicate over claims, a
// namespace, an expression, or any shape that is not one of the two above. An
// `&&` clause can only shrink the set further, so ignoring it can only miss a
// refusal, never make one wrongly; `exclude:` likewise. A quorum with
// `distinct: false` is not asked at all, since one sender may then meet it alone.
func validateQuorums(root *v1.Workflow) Diagnostics {
	return validateQuorumsIn(root, root.GetSteps(), 0)
}

// validateQuorumsIn walks one workflow's steps, and the steps of every callee
// reached through a `call:`, against the policy of root.
//
// The root's `signals:` is the one a delivery is authorized against, durably
// (the server records the top-level workflow's) and locally (the wait policies
// are recorded from the top-level workflow only), so a callee's own declarations
// admit nobody and are not the answer here. A callee gate is therefore asked of
// the root's policy for its name, and a finding is reported at the call step
// that reaches it, in the voice [validateCallAtDepth] gives every other callee
// finding.
func validateQuorumsIn(root *v1.Workflow, steps []*v1.Node, depth int) Diagnostics {
	var ds Diagnostics

	v1.WalkNodes(steps, v1.Walk{Node: func(node *v1.Node) {
		if call := node.GetCall(); call != nil {
			// Depth is reported by the call validation itself; stop here rather
			// than recurse past the bound it names.
			if callee := call.GetWorkflow(); callee != nil && v1.CheckCallDepth(depth+1) == nil {
				for _, d := range validateQuorumsIn(root, callee.GetSteps(), depth+1) {
					d.Step = node.GetId()
					d.Message = fmt.Sprintf("workflow %q: %s", callee.GetName(), d.Message)
					ds = append(ds, d)
				}
			}

			return
		}

		batch := node.GetWait().GetSignalBatch()
		quorum := batch.GetQuorum()
		if quorum == nil || (quorum.Distinct != nil && !quorum.GetDistinct()) {
			return
		}

		principals, closed := v1.SignalPolicyClosedPrincipals(root.GetSignals()[batch.GetName()])
		permitted := len(principals)
		if !closed || int(quorum.GetApprove()) <= permitted {
			return
		}

		ds = append(ds, Diagnostic{
			Step:  node.GetId(),
			Field: "wait_for_signals.quorum.approve",
			Message: fmt.Sprintf(
				"is %d, but the `signals:` policy for %q permits only %d distinct subject(s), so the quorum can never be met; "+
					"lower `approve:`, add the missing approvers to the policy, or set `distinct: false`",
				quorum.GetApprove(), batch.GetName(), permitted),
		})
	}})

	return ds
}
