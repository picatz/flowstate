package flowfile

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/goccy/go-yaml/ast"
	"github.com/google/cel-go/cel"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Rewriting who may act into the one predicate that says it.
//
// `signals: <name>: allow:` and `debug: allow:` as a list of rules,
// `distinct_from_starter: true`, and `triggers: manual: allowed_principals:` are
// three spellings of one question that `allow: ${...}` answers over a closed scope
// (docs/STYLE.md R1 and R2, issue #326). This file brings each into that form.
//
//	signals:
//	  deploy-approved:
//	    allow:
//	      - subject: ${"https://issuer.example.com#" + inputs.approver}
//	        claims: {team: release-managers}
//	      - claims: {role: sre-lead}
//	    distinct_from_starter: true
//
// becomes
//
//	signals:
//	  deploy-approved:
//	    allow: ${(sender.identity.principal == "https://issuer.example.com#" + inputs.approver && sender.identity.claims.team == "release-managers" || sender.identity.claims.role == "sre-lead") && sender.identity.principal != run.identity.principal}
//
// # No edition boundary
//
// This is a style sweep in the sense [Fix]'s shaping rewrite is: both spellings
// compile and both run today, so there is no file this build refuses that the
// rewrite has to rescue, and an edition marker exists to say which grammar a file
// was written in, not to gate a rewrite. The boundary belongs to the change that
// removes the old spellings, which is what makes a v2026.4 file that writes one a
// refusal rather than a file with a better spelling available.
//
// # What makes the rewrite safe
//
// Each rule is a conjunction and the rules are a disjunction, so the text is
// total: `subject:` is `sender.identity.principal == S`, `claims:` entries are
// `sender.identity.claims.k == "v"`, `namespace:` is `sender.identity.namespace
// == "N"`, rules join with `||`, and the policy-level `distinct_from_starter:`
// conjoins `sender.identity.principal != run.identity.principal` around the
// whole. Three things keep "total" from meaning "approximately":
//
//   - CEL evaluates a missing claim, or a `run` that is not bound because the
//     starter is unknown, as an error rather than as false. Every operator here is
//     monotone (`&&`, `||`, `==`, `!=`), so an error can only ever withhold a `true`,
//     and the evaluator denies on error: the policy answers allow exactly where the
//     rule list did.
//   - A literal subject is always `issuer#subject`, one `#` with both halves, so it
//     can be compared as is. An interpolated one is whatever the run's inputs
//     computed, and the rule list refused the run at submit when that was not
//     qualified: an empty value would equal the "" principal of every unauthenticated
//     sender, and `a#b` would admit a sender whose subject is `a#b`. So the predicate
//     says `sender.identity.principal.split("#").size() == 2` first, which holds only
//     for a principal with exactly one `#` (and is false for ""), and the comparison
//     that follows can then only admit a sender the rule list would have admitted.
//   - The narrowing rule moved from a per-rule check to a syntactic one over the
//     whole predicate, which is coarser. A policy the old rule accepted has, for
//     every interpolated subject, a `claims:` entry beside it or a policy-level
//     `distinct_from_starter: true`, and both are kept as conjuncts, so what it read
//     before it reads now. A policy the old rule *refused* is refused here too, rather
//     than quietly accepted by the coarser check because some other rule carries a
//     claim.
//
// # What it refuses
//
// Anything it would have to guess at: a value that is not a string, one holding a
// fence where the old grammar read literal text, a subject that is not
// `issuer#subject`, an interpolated subject nothing narrows, a comment that could
// be read as part of an expression, a flow-style policy it cannot splice, a
// predicate-plus-`distinct_from_starter:` whose predicate would not validate on
// its own, and any output the policy compiler would itself refuse. A refusal
// leaves the file as it was; the old spelling keeps working.

// The keys this rewrite reads and the paths it writes into a predicate. The paths are
// the closed scope [v1.CompileSignalPolicyPredicate] type-checks against, spelled once.
const (
	allowKey             = "allow"
	distinctFromStarter  = "distinct_from_starter"
	allowedPrincipalsKey = "allowed_principals"
	manualKey            = "manual"

	senderPrincipal  = "sender.identity.principal"
	starterPrincipal = "run.identity.principal"
	senderClaims     = "sender.identity.claims"
	senderNamespace  = "sender.identity.namespace"

	// singleSeparator keeps an interpolated subject what the rule list required of a
	// resolved one: `<issuer>#<subject>` with exactly one `#` and both halves present.
	// Asked of the sender's principal, which the predicate equates to the computed
	// subject, so it bounds the computed value too. It is also false for "", so an
	// unauthenticated sender (principal "") can never equal an empty computed subject.
	singleSeparator = `sender.identity.principal.split("#").size() == 2`

	// maxManualPrincipals is the most a `manual:` block names, the compiler's bound.
	maxManualPrincipals = 64
)

// policyStanzaKeys are the top-level keys whose predicates read `sender` and `run`,
// and policyScopeNames are the two names they bind.
var (
	policyStanzaKeys = map[string]bool{"signals": true, "debug": true}
	policyScopeNames = map[string]bool{"sender": true, "run": true}
)

// celIdentifier is what may follow a `.` in CEL without indexing.
var celIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// celReservedWords cannot be a field name after a `.`, so a claim called one of
// them is read by index.
var celReservedWords = map[string]bool{
	"as": true, "break": true, "const": true, "continue": true, "else": true, "false": true,
	"for": true, "function": true, "if": true, "import": true, "in": true, "let": true,
	"loop": true, "namespace": true, "null": true, "package": true, "return": true,
	"true": true, "var": true, "void": true, "while": true,
}

// signalPolicies rewrites every policy under a `signals:` block.
func (f *fixer) signalPolicies(n ast.Node) {
	mapping := asMapping(n)
	if mapping == nil {
		return
	}
	for _, v := range mapping.Values {
		name, ok := keyNameOf(v.Key)
		if !ok {
			continue
		}
		f.policyStanza(v.Value, "signals."+name)
	}
}

// policyStanza rewrites one signal's policy or the `debug:` stanza, which share
// a grammar and a compiled message.
func (f *fixer) policyStanza(n ast.Node, where string) {
	mapping := asMapping(n)
	if mapping == nil {
		return
	}

	if f.refuseMergeKey(mapping, where) {
		return
	}

	var allow, distinct *ast.MappingValueNode
	var other []string
	for _, v := range mapping.Values {
		name, ok := keyNameOf(v.Key)
		switch {
		case ok && name == allowKey:
			allow = v
		case ok && name == distinctFromStarter:
			distinct = v
		case ok:
			other = append(other, name)
		}
	}
	if allow == nil {
		return
	}

	var (
		rules     *ast.SequenceNode
		predicate string
	)
	switch value := unwrapAnchor(allow.Value).(type) {
	case *ast.SequenceNode:
		rules = value
	case *ast.StringNode, *ast.LiteralNode:
		if distinct == nil {
			// The canonical spelling already.
			return
		}
		raw, _ := scalarText(value)
		inner, fenced := SplitFence(strings.TrimSpace(raw))
		if !fenced {
			// Not a predicate the compiler reads either; its own diagnostic says so.
			return
		}
		predicate = strings.TrimSpace(inner)
	default:
		// A mapping, a number: nothing the compiler reads as a policy, so nothing
		// here to bring forward.
		return
	}

	if mapping.IsFlowStyle {
		f.refuse(mapping, "%s: this policy is written in flow style, which `flow fix` does not reflow; rewrite its `allow:` as one `${...}` predicate by hand", where)
		return
	}
	if len(other) > 0 {
		f.refuse(mapping, "%s: this policy also writes `%s:`, which a policy does not say; nothing here can be rewritten until it is removed", where, other[0])
		return
	}

	distinctOn := false
	if distinct != nil {
		b, ok := unwrapAnchor(distinct.Value).(*ast.BoolNode)
		if !ok {
			f.refuse(distinct.Value, "%s: `distinct_from_starter:` is not `true` or `false`, so it cannot be folded into a predicate", where)
			return
		}
		distinctOn = b.Value
	}

	var (
		body string
		ok   bool
	)
	if rules != nil {
		body, ok = f.rulesPredicate(rules, where, distinctOn)
	} else {
		body, ok = f.wrapPredicate(allow.Value, predicate, where)
	}
	if !ok {
		return
	}
	if distinctOn {
		body = joinDistinct(body, rules != nil && len(rules.Values) > 1 || rules == nil && needsParensForAnd(predicate))
	}

	if err := v1.CheckSignalPolicyExpr(body); err != nil {
		f.refuse(allow.Key, "%s: the predicate this would write does not validate (%s), so nothing was rewritten; the old spelling is unchanged", where, err)
		return
	}

	f.writeAllow(allow, distinct, body, where, "")
}

// refuseMergeKey refuses a policy that merges keys in with `<<:`: what the merge
// brings may be an old-form `allow:` or a `distinct_from_starter:` this walk cannot
// see, and rewriting the keys written here would leave the merged ones to contradict
// the predicate.
func (f *fixer) refuseMergeKey(mapping *ast.MappingNode, where string) bool {
	for _, v := range mapping.Values {
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			f.refuse(v.Key, "%s: this policy merges keys in with `<<:`, which `flow fix` does not resolve, so it cannot tell what the policy says; write the keys out", where)

			return true
		}
	}

	return false
}

// rulesPredicate renders an `allow:` rule list as one predicate body (no fence).
func (f *fixer) rulesPredicate(rules *ast.SequenceNode, where string, distinct bool) (string, bool) {
	if len(rules.Values) == 0 {
		f.refuse(rules, "%s: `allow:` is an empty list, which authorizes nobody and which the compiler refuses; there is nothing to rewrite", where)
		return "", false
	}

	rendered := make([]string, 0, len(rules.Values))
	for i, item := range rules.Values {
		text, compound, ok := f.rulePredicate(item, fmt.Sprintf("%s.allow[%d]", where, i), distinct)
		if !ok {
			return "", false
		}
		// `&&` binds tighter than `||`, so the parentheses change nothing; they say
		// so to a reader, which is the only one who has to be told.
		if compound && len(rules.Values) > 1 {
			text = "(" + text + ")"
		}
		rendered = append(rendered, text)
	}

	return strings.Join(rendered, " || "), true
}

// rulePredicate renders one rule as a conjunction, and says whether it holds more
// than one conjunct.
func (f *fixer) rulePredicate(item ast.Node, where string, distinct bool) (string, bool, bool) {
	rule := asMapping(item)
	if rule == nil {
		f.refuse(item, "%s: a rule is a mapping with a `subject:`, a `namespace:`, `claims:`, or a combination, and this is not one", where)
		return "", false, false
	}

	var (
		subject, namespace, claims ast.Node
		conjuncts                  []string
	)
	for _, v := range rule.Values {
		name, ok := keyNameOf(v.Key)
		if !ok {
			f.refuse(v.Key, "%s: this rule has a key that is not a plain name", where)
			return "", false, false
		}
		switch name {
		case "subject":
			subject = v.Value
		case "namespace":
			namespace = v.Value
		case "claims":
			claims = v.Value
		default:
			f.refuse(v.Key, "%s: a rule says `subject:`, `namespace:` or `claims:`, and `%s:` is none of them", where, name)
			return "", false, false
		}
	}

	var claimConjuncts []string
	if claims != nil {
		var ok bool
		if claimConjuncts, ok = f.claimConjuncts(claims, where+".claims"); !ok {
			return "", false, false
		}
	}

	if subject != nil {
		text, ok := f.subjectConjunct(subject, where+".subject", len(claimConjuncts) > 0 || distinct)
		if !ok {
			return "", false, false
		}
		conjuncts = append(conjuncts, text)
	}
	conjuncts = append(conjuncts, claimConjuncts...)

	if namespace != nil {
		literal, ok := f.literal(namespace, where+".namespace")
		if !ok {
			return "", false, false
		}
		if literal == "" {
			f.refuse(namespace, "%s.namespace: is empty, which the rule list reads as no constraint at all; the compiler refuses a rule that matches every sender, and so does this", where)
			return "", false, false
		}
		quoted, ok := celString(literal)
		if !ok {
			f.refuse(namespace, "%s.namespace: is not valid UTF-8, so it cannot be written as an expression", where)
			return "", false, false
		}
		conjuncts = append(conjuncts, senderNamespace+" == "+quoted)
	}

	if len(conjuncts) == 0 {
		f.refuse(item, "%s: sets none of `subject:`, `namespace:` or `claims:`, so it matches every sender, which the compiler refuses and so does this", where)
		return "", false, false
	}

	joined := strings.Join(conjuncts, " && ")

	return joined, len(conjuncts) > 1 || strings.Contains(joined, " && "), true
}

// subjectConjunct renders one rule's `subject:`.
//
// narrowed says something beside it — a claim in the same rule, or the policy's
// distinct_from_starter — keeps the caller from choosing the sender, which an
// interpolated subject needs and which the old per-rule check enforced.
func (f *fixer) subjectConjunct(n ast.Node, where string, narrowed bool) (string, bool) {
	raw, ok := f.rawScalar(n, where)
	if !ok {
		return "", false
	}

	if inner, fenced := SplitFence(raw); fenced {
		if !narrowed {
			f.refuse(n, "%s: is an expression resolved from this run's inputs and nothing beside it narrows who that may name, which the compiler refuses; add a `claims:` entry to the rule or `distinct_from_starter: true` to the policy first", where)
			return "", false
		}

		expression := strings.TrimSpace(inner)
		single, ok := singleLineCEL(expression)
		if !ok {
			f.refuse(n, "%s: this expression holds a `//` comment or a multi-line string, which cannot be moved onto one line of a predicate without changing what it says", where)
			return "", false
		}

		top, err := inspectCEL(single)
		if err != nil {
			f.refuse(n, "%s: this expression does not parse (%v), so it cannot be moved", where, err)
			return "", false
		}
		if needsParensForEquality(top) {
			single = "(" + single + ")"
		}

		return singleSeparator + " && " + senderPrincipal + " == " + single, true
	}

	literal, ok := f.literal(n, where)
	if !ok {
		return "", false
	}
	if !v1.LooksLikeQualifiedSubject(literal) {
		f.refuse(n, "%s: is %q, which is not \"<issuer>#<subject>\"; the compiler refuses a bare subject, and a predicate comparing a principal to one would never match", where, literal)
		return "", false
	}
	quoted, ok := celString(literal)
	if !ok {
		f.refuse(n, "%s: is not valid UTF-8, so it cannot be written as an expression", where)
		return "", false
	}

	return senderPrincipal + " == " + quoted, true
}

// claimConjuncts renders a `claims:` mapping, in the order it is written.
func (f *fixer) claimConjuncts(n ast.Node, where string) ([]string, bool) {
	mapping := asMapping(n)
	if mapping == nil {
		f.refuse(n, "%s: `claims:` is a mapping of claim name to the value it must have", where)
		return nil, false
	}

	var out []string
	for _, v := range mapping.Values {
		key, ok := keyNameOf(v.Key)
		if !ok {
			f.refuse(v.Key, "%s: this claim name is not a plain string", where)
			return nil, false
		}
		value, ok := f.literal(v.Value, where+"."+key)
		if !ok {
			return nil, false
		}
		quotedKey, ok1 := celString(key)
		quotedValue, ok2 := celString(value)
		if !ok1 || !ok2 {
			f.refuse(v.Key, "%s.%s: is not valid UTF-8, so it cannot be written as an expression", where, key)
			return nil, false
		}

		read := senderClaims + "[" + quotedKey + "]"
		if celIdentifier.MatchString(key) && !celReservedWords[key] {
			read = senderClaims + "." + key
		}
		out = append(out, read+" == "+quotedValue)
	}

	return out, true
}

// rawScalar reads a string scalar exactly as the compiler does before it decides
// whether it is a fence, and refuses anything that is not one.
func (f *fixer) rawScalar(n ast.Node, where string) (string, bool) {
	raw, ok := scalarText(unwrapAnchor(n))
	if !ok {
		f.refuse(n, "%s: must be a string, and this is not one", where)
		return "", false
	}

	return raw, true
}

// literal reads a string the old grammar took as literal text: a fence anywhere
// in it was a compile error, so one here is a file that did not compile.
func (f *fixer) literal(n ast.Node, where string) (string, bool) {
	raw, ok := f.rawScalar(n, where)
	if !ok {
		return "", false
	}
	text, err := LiteralText(raw)
	if err != nil {
		f.refuse(n, "%s: %v", where, err)
		return "", false
	}

	return text, true
}

// wrapPredicate validates an existing predicate that is about to gain a
// `distinct_from_starter:` clause, and returns it as is.
//
// The old narrowing check ignored `distinct_from_starter:` for a predicate, so a
// predicate that read inputs and nothing else was refused with it beside it. The
// conjunct this rewrite adds would read `run.identity` and make the pair pass, so
// the predicate has to stand on its own first.
func (f *fixer) wrapPredicate(n ast.Node, predicate, where string) (string, bool) {
	single, ok := singleLineCEL(predicate)
	if !ok {
		f.refuse(n, "%s: this predicate holds a `//` comment or a multi-line string, which cannot be joined to a clause on one line without changing what it says", where)
		return "", false
	}
	if err := v1.CheckSignalPolicyExpr(single); err != nil {
		f.refuse(n, "%s: this predicate does not validate on its own (%v), so `distinct_from_starter:` cannot be folded into it; fix the predicate first", where, err)
		return "", false
	}

	return single, true
}

// joinDistinct conjoins the starter comparison around a whole predicate.
func joinDistinct(body string, parenthesize bool) string {
	if parenthesize {
		body = "(" + body + ")"
	}

	return body + " && " + senderPrincipal + " != " + starterPrincipal
}

// manualPolicies rewrites `manual: allowed_principals:` wherever it is written in
// a `triggers:` block, in either of the block's two spellings.
func (f *fixer) manualPolicies(n ast.Node) {
	if seq, ok := unwrapAnchor(n).(*ast.SequenceNode); ok {
		for _, item := range seq.Values {
			f.manualEntry(asMapping(item))
		}

		return
	}
	f.manualEntry(asMapping(n))
}

func (f *fixer) manualEntry(mapping *ast.MappingNode) {
	if mapping == nil {
		return
	}
	for _, v := range mapping.Values {
		if name, ok := keyNameOf(v.Key); ok && name == manualKey {
			f.manualPolicy(v.Value)
		}
	}
}

// manualPolicy rewrites one `manual:` mapping's `allowed_principals:`.
func (f *fixer) manualPolicy(n ast.Node) {
	mapping := asMapping(n)
	if mapping == nil {
		return
	}

	if f.refuseMergeKey(mapping, "triggers.manual") {
		return
	}

	var principals, allow *ast.MappingValueNode
	for _, v := range mapping.Values {
		switch name, _ := keyNameOf(v.Key); name {
		case allowedPrincipalsKey:
			principals = v
		case allowKey:
			allow = v
		}
	}
	if principals == nil {
		return
	}

	const where = "triggers.manual"
	if mapping.IsFlowStyle {
		f.refuse(mapping, "%s: this block is written in flow style, which `flow fix` does not reflow; write `allow: ${...}` by hand", where)
		return
	}
	if allow != nil {
		f.refuse(allow.Key, "%s: this block writes both `allowed_principals:` and `allow:`, which the compiler refuses; keep one", where)
		return
	}

	nodes := []ast.Node{principals.Value}
	if seq, ok := unwrapAnchor(principals.Value).(*ast.SequenceNode); ok {
		nodes = seq.Values
	}
	if len(nodes) == 0 || len(nodes) > maxManualPrincipals {
		f.refuse(principals.Value, "%s.allowed_principals: names %d principals, and the compiler accepts between 1 and %d", where, len(nodes), maxManualPrincipals)
		return
	}

	quoted := make([]string, 0, len(nodes))
	seen := make(map[string]bool, len(nodes))
	for i, node := range nodes {
		at := fmt.Sprintf("%s.allowed_principals[%d]", where, i)
		principal, ok := f.literal(node, at)
		if !ok {
			return
		}
		switch {
		case strings.TrimSpace(principal) == "":
			f.refuse(node, "%s: is empty, which names nobody", at)
			return
		case !v1.LooksLikeQualifiedSubject(principal):
			f.refuse(node, "%s: is %q, which is not \"<issuer>#<subject>\"; the compiler refuses it", at, principal)
			return
		case seen[principal]:
			f.refuse(node, "%s: lists %q twice, which the compiler refuses", at, principal)
			return
		}
		seen[principal] = true

		text, ok := celString(principal)
		if !ok {
			f.refuse(node, "%s: is not valid UTF-8, so it cannot be written as an expression", at)
			return
		}
		quoted = append(quoted, text)
	}

	body := senderPrincipal + " in [" + strings.Join(quoted, ", ") + "]"
	if err := v1.CheckManualAllowExpr(body); err != nil {
		f.refuse(principals.Key, "%s: the predicate this would write does not validate (%s), so nothing was rewritten", where, err)
		return
	}

	f.writeAllow(principals, nil, body, where, "`allowed_principals:` is now")
}

// writeAllow replaces a policy's old keys with one `allow:` entry.
//
// The replaced run is the key's own block, which takes in the comments written
// among its values; they are carried above the new line rather than dropped,
// because a comment is the part of a file a rewriter can least afford to lose,
// and said so in a note since a comment beside a rule list is about that rule.
// A comment this cannot place with certainty refuses instead.
func (f *fixer) writeAllow(old, distinct *ast.MappingValueNode, body, where, lead string) {
	span := spanOfNode(old.Key)
	if !span.IsValid() || span.Start.Line > len(f.lines) {
		return
	}
	keyLine := span.Start.Line
	indent := indentWidth(f.line(keyLine))
	if span.Start.Column-1 != indent {
		f.refuse(old.Key, "%s: this key shares its line with something else, which `flow fix` does not splice; rewrite it by hand", where)
		return
	}

	through := f.blockEnd(keyLine, indent)
	if end := spanOfNode(old.Value); end.IsValid() && end.End.Line > through {
		f.refuse(old.Key, "%s: the value of this key is not indented under it, which `flow fix` does not splice; rewrite it by hand", where)
		return
	}
	// Whatever follows the value inside the run must be a comment or blank, or the
	// run holds something this did not read.
	last := keyLine
	if end := spanOfNode(old.Value); end.IsValid() {
		last = max(last, end.End.Line)
	}
	for n := last + 1; n <= through; n++ {
		if t := strings.TrimSpace(f.line(n)); t != "" && !strings.HasPrefix(t, "#") {
			f.refuse(old.Key, "%s: line %d sits inside this key's block and is not part of its value, which `flow fix` does not splice", where, n)
			return
		}
	}

	pad := strings.Repeat(" ", indent)
	comments, ok := f.commentsIn(keyLine, through, pad)
	if !ok {
		f.refuse(old.Key, "%s: a comment here sits beside a value in a way that cannot be told from text, so it would not be carried; move it onto its own line first", where)
		return
	}

	var distinctComments []string
	var distinctLine int
	if distinct != nil {
		ds := spanOfNode(distinct.Key)
		if !ds.IsValid() || ds.Start.Column-1 != indentWidth(f.line(ds.Start.Line)) || f.blockEnd(ds.Start.Line, indentWidth(f.line(ds.Start.Line))) != ds.Start.Line {
			f.refuse(distinct.Key, "%s: `distinct_from_starter:` is not alone on its line, which `flow fix` does not splice; rewrite it by hand", where)
			return
		}
		distinctLine = ds.Start.Line
		if distinctLine >= keyLine && distinctLine <= through {
			f.refuse(distinct.Key, "%s: `distinct_from_starter:` sits inside the rule list's block, which `flow fix` does not splice", where)
			return
		}
		if distinctComments, ok = f.commentsIn(distinctLine, distinctLine, pad); !ok {
			f.refuse(distinct.Key, "%s: a comment on this line sits beside a value in a way that cannot be told from text; move it onto its own line first", where)
			return
		}
	}

	text := "${" + body + "}"
	line, ok := allowLine(text)
	if !ok {
		f.refuse(old.Key, "%s: the predicate this would write cannot be placed on one line of YAML", where)
		return
	}

	replacement := append(append(comments, distinctComments...), pad+line)

	message, pending := "", ""
	switch {
	case distinct != nil:
		message = fmt.Sprintf("%s: the `allow:` rule list and `distinct_from_starter:` are now one `allow: ${...}` predicate", where)
		pending = fmt.Sprintf("%s: the `allow:` rule list and `distinct_from_starter:` would become one `allow: ${...}` predicate", where)
	case lead != "":
		message = fmt.Sprintf("%s: %s `allow: ${...}`", where, lead)
		pending = fmt.Sprintf("%s: `allowed_principals:` would become `allow: ${...}`", where)
	default:
		message = fmt.Sprintf("%s: the `allow:` rule list is now one `allow: ${...}` predicate", where)
		pending = fmt.Sprintf("%s: the `allow:` rule list would become one `allow: ${...}` predicate", where)
	}

	if len(comments)+len(distinctComments) > 0 {
		// In the change rather than a note: a note is read from the last pass of
		// [Fix], which is the one that finds nothing left to do.
		message += "; comments written inside the old rules were moved above it, so check they still describe it"
		pending += "; comments written inside the old rules would be moved above it"
	}

	if f.edits == nil {
		f.edits = make(map[int]lineEdit)
	}
	// Both edits or neither: [fixer.record] drops a replacement whose line is taken,
	// and a deletion left alone would remove `distinct_from_starter:` with nothing
	// written in its place.
	if _, taken := f.edits[keyLine]; taken {
		return
	}
	if distinct != nil {
		if _, taken := f.edits[distinctLine]; taken {
			return
		}
		// A deletion rides on the same change: one rewrite, one line in the report.
		f.edits[distinctLine] = lineEdit{through: distinctLine}
	}
	f.record(keyLine, through, replacement, message, pending)
}

// commentsIn returns the comments on lines first..last as comment-only lines at
// pad's indent, or false when a line holds a `#` it cannot classify.
func (f *fixer) commentsIn(first, last int, pad string) ([]string, bool) {
	var out []string
	for n := first; n <= last; n++ {
		line := f.line(n)
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			out = append(out, pad+trimmed)
			continue
		}
		if at := yamlCommentStart(line); at >= 0 {
			out = append(out, pad+strings.TrimSpace(line[at:]))
		} else if strings.Contains(line, " #") {
			return nil, false
		}
	}

	return out, true
}

// yamlCommentStart finds a trailing comment: a `#` outside quotes that follows
// whitespace. -1 when there is none.
func yamlCommentStart(line string) int {
	var quote rune
	prev := ' '
	escaped := false
	for i, r := range line {
		if escaped {
			// The character after a backslash in a double-quoted scalar is part of
			// the escape, so `\"` does not close the string and `#` after it is text.
			escaped = false
			prev = r
			continue
		}
		switch {
		case quote == '"' && r == '\\':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '"' || r == '\'':
			quote = r
		case r == '#' && (prev == ' ' || prev == '\t'):
			return i
		}
		prev = r
	}

	return -1
}

// allowLine writes `allow: <text>` through [fencedToYAML], the one function the
// formatter writes a fenced expression with, so a rewritten file is already what
// `flow fmt` produces.
func allowLine(text string) (string, bool) {
	body, ok := fencedToYAML(strings.TrimSuffix(strings.TrimPrefix(text, fenceOpen), fenceClose)).(styledScalar)
	if !ok || strings.ContainsAny(string(body), "\r\n") {
		return "", false
	}

	return allowKey + ": " + string(body), true
}

// celString renders s as a double-quoted CEL string literal, or reports that it
// cannot be rendered faithfully.
func celString(s string) (string, bool) {
	if !utf8.ValidString(s) {
		return "", false
	}

	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')

	return b.String(), true
}

// singleLineCEL returns an expression on one line, or false when putting it there
// could change what it says: a `//` comment would swallow whatever is written after
// it, and a triple-quoted string may span lines on purpose.
func singleLineCEL(src string) (string, bool) {
	if hasCELComment(src) || strings.Contains(src, `"""`) || strings.Contains(src, `'''`) {
		return "", false
	}

	return multiline.ReplaceAllString(src, " "), true
}

var multiline = regexp.MustCompile(`[ \t]*[\r\n]+[ \t]*`)

// hasCELComment reports whether src holds a `//` comment outside a string.
func hasCELComment(src string) bool {
	rs := []rune(src)
	for i := 0; i < len(rs); i++ {
		switch c := rs[i]; {
		case c == '/' && i+1 < len(rs) && rs[i+1] == '/':
			return true
		case c == '\'' || c == '"':
			raw := i > 0 && (rs[i-1] == 'r' || rs[i-1] == 'R')
			triple := i+2 < len(rs) && rs[i+1] == c && rs[i+2] == c
			if triple {
				i += 3
				for i < len(rs) && !(rs[i] == c && i+2 < len(rs) && rs[i+1] == c && rs[i+2] == c) {
					if !raw && rs[i] == '\\' {
						i++
					}
					i++
				}
				i += 2
				continue
			}
			i++
			for i < len(rs) && rs[i] != c {
				if !raw && rs[i] == '\\' {
					i++
				}
				i++
			}
		}
	}

	return false
}

// inspectCEL parses src in the profile's environment and reports its top-level
// operator, "" when it is not an operator call.
func inspectCEL(src string) (top string, err error) {
	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	if err != nil {
		return "", err
	}
	env, err := v1.DefaultEvaluator().Env(libs...)
	if err != nil {
		return "", err
	}
	parsed, issues := env.Parse(src)
	if issues != nil && issues.Err() != nil {
		return "", issues.Err()
	}
	expr, err := cel.AstToParsedExpr(parsed)
	if err != nil {
		return "", err
	}
	if call := expr.GetExpr().GetCallExpr(); call != nil {
		top = call.GetFunction()
	}

	return top, nil
}

// needsParensForEquality: an expression whose top operator binds no tighter
// than `==` must be parenthesized to be one operand of it.
func needsParensForEquality(top string) bool {
	switch top {
	case "_?_:_", "_||_", "_&&_", "_==_", "_!=_", "_<_", "_<=_", "_>_", "_>=_", "@in":
		return true
	}

	return false
}

// needsParensForAnd: an expression must be parenthesized to be an operand of
// `&&` when its top operator binds looser.
func needsParensForAnd(src string) bool {
	top, err := inspectCEL(src)

	return err != nil || top == "_?_:_" || top == "_||_"
}
