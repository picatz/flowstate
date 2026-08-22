package flowfile

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/cel-go/common/operators"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Tier 4 of the style charter (docs/STYLE.md, Part II): suggest.
//
// The charter sorts every rule it has by whose problem a violation is. A
// spelling that is wrong everywhere is the parser's or the validator's, and it
// is refused (tier 1). Two spellings for one meaning are the formatter's or the
// rewriter's (tiers 2 and 3). What is left is legal, runs, means what it says,
// and there is a better way to write it — which is nobody's refusal and
// everybody's review comment, and that is what this is.
//
// # What is in here, and why it is only this
//
// The charter names the tier-4 checks itself, in Part II's table: "R5's
// ternary, repeat and dispatch checks; the tooling half of R8". This file is
// those three, each with a doc comment naming the rule it descends from. It is
// not every mechanically checkable sentence in the charter, and the omissions
// are decisions rather than a backlog:
//
//   - R5's guarded read (`has(x.y) ? x.y : d`) is tier 3 and shipped, in
//     fixoptional.go. A suggestion beside a rewrite that already performs the
//     change is noise.
//   - R5's "a comment explaining what an expression computes" is taste, and the
//     charter says so. There is no mechanical shape.
//   - R4's fence rule (`if: ${...}` over the bare form that also parses) is
//     tier 1 or nothing, per R4's own enforcement line: it is waiting on #545 to
//     decide which positions require a fence and which refuse one, and the
//     migration is tier 3. A tier-4 suggestion here would be this file
//     pre-empting that decision in the one venue nobody is arguing in.
//   - R8's byte-identical clause is measured as unmet in Part III, and the
//     measurement says why: `flow fmt` rewrites or refuses every shown Flowfile
//     in the repository, so the corpus cannot be held to it until the formatter
//     is good enough to be the canon for a hand-written teaching file. What
//     lands here is R8's other half — the shown corpus produces no tier-4
//     findings — which needs no formatter change to be true.
//   - R1, R2, R3, R6, R7 and R9 name review, tier 1 or tier 3 as their
//     enforcement. None of them is a property of a single file that a walk can
//     decide.
//
// # Advisory, and what that has to mean
//
// A finding here never fails anything on its own. `flow lint` exits zero on
// every finding it has, exactly as `flow audit` does, and the caller opts into
// a nonzero exit with `--strict` — which is what the CI leg over `examples/`
// uses, because the shown corpus is held to a standard the whole language is
// not.
//
// The reason is R8's containment: `shown ⊆ canonical ⊂ legal`. Making a
// suggestion a refusal narrows *legal*, and a language that refuses its own
// generators grows a workaround culture. So the same check is advice to an
// author and a gate over the files this repository teaches from, and the
// difference is a flag rather than a second implementation.
//
// # What a finding may be drawn from
//
// The file, and nothing else. No deployment is consulted, no policy is read,
// nothing resolves over a network — anti-goal 8, and the same rule the
// validator follows about a property of the file versus a decision a deployment
// makes. This runs in an editor's keystroke path through the same package the
// language server uses.

// maxLintNodes bounds how many expression nodes one file's checks visit.
//
// The same reasoning [maxAuditCandidates] carries, for the same walk over the
// same input: the parser bounds one expression, nothing bounds the sum over a
// file of arbitrarily many of them, and a document is somebody else's. Past the
// bound the walk stops looking, which costs a missed suggestion on a file far
// larger than any real one — and a missed suggestion is the cheapest thing this
// file can get wrong.
const maxLintNodes = 100_000

// styleDispatchThreshold is how many sibling equality tests on one value make a
// `switch:`.
//
// Three, from the decided-spellings table's first row: "dispatch on one value
// with three or more outcomes". Two named cases are a pair of conditions and
// read perfectly well as one; the table's claim is about the point where a
// reader can no longer see that the branches partition anything, which is also
// where the validator's unreachable-`default:` check starts earning its keep.
const styleDispatchThreshold = 3

// styleRepeatThreshold is how many statements of one expression make it a
// `value:` step waiting for a name.
//
// Three, from R5: "a structurally repeated subexpression appearing three or
// more times". Deliberately above `flow audit`'s two, which is a different
// question — the measurement counts friction the language imposes and reports
// a pair honestly, while a suggestion made at two would fire on the many pairs
// this corpus holds for good reasons.
const styleRepeatThreshold = 3

// A StyleRule names the charter rule a finding descends from, so an author can
// read the sentence that decided it.
//
// The spelling is `R<n>/<check>`: the rule is the part `docs/STYLE.md` is
// organized by and the part worth citing in a review, and the check
// distinguishes the several mechanical shapes one rule can have. R5 alone has
// three.
//
// Deliberately a string rather than a code in the schema. [v1.DiagnosticCode]
// is the closed set a *program* branches on, and it is closed because a
// consumer switching on it needs the set not to grow under it; these are
// addressed to a person, who resolves one by opening the charter and reading a
// paragraph. Adding a tier-4 check must not be a schema change.
type StyleRule string

// The tier-4 checks, each named for the charter rule that decides it.
const (
	// StyleNestedConditional is R5's first threshold: a ternary inside a
	// ternary.
	StyleNestedConditional StyleRule = "R5/nested-conditional"

	// StyleRepeatedExpr is R5's third: one expression stated three or more
	// times, which is a `value:` step waiting for a name.
	StyleRepeatedExpr StyleRule = "R5/repeated-expression"

	// StyleEqualityDispatch is R5's fourth: sibling `if:` steps that all test
	// one value for equality, which is a `switch:`.
	StyleEqualityDispatch StyleRule = "R5/equality-dispatch"
)

// A StyleFinding is one tier-4 suggestion about one file.
//
// Not a [Diagnostic], and the distance is the point rather than an omission. A
// Diagnostic is a refusal: it travels in [v1.DiagnosticReport], it carries a
// [v1.DiagnosticCode] a program branches on, and everything that renders one
// treats it as a reason the file was rejected. A finding here is advice about a
// file that is correct, so it is a separate type whose consumers cannot
// accidentally weigh it as a refusal — which is precisely how the tiers stop
// being tiers.
type StyleFinding struct {
	// Rule is the charter rule this descends from.
	Rule StyleRule

	// Line is the 1-based source line, or zero when the position is unknown.
	Line int

	// Column is the 1-based column within Line, or zero when only the line is
	// known — which is what a block scalar leaves knowable; see [exprPosition].
	Column int

	// Step is the id of the step the suggestion is about, empty for a
	// workflow-level position.
	Step string

	// Field names the property it is about, using the key an author spells:
	// `if`, `outputs.decision`, a task input's name.
	Field string

	// Message says what is written, what to write instead, and nothing about
	// severity. See [StyleFinding.String] for the rendering.
	Message string
}

// String renders the finding in the conventional line:column: message form, with
// the rule id last so a reader can look it up.
//
// The same shape [Diagnostic.Error] renders, because the position is the half a
// terminal, an editor and a CI annotation all match on and there is no reason
// for two spellings of it in one repository (#384).
func (f StyleFinding) String() string {
	var b strings.Builder

	if f.Line > 0 {
		b.WriteString(strconv.Itoa(f.Line))
		if f.Column > 0 {
			b.WriteString(":")
			b.WriteString(strconv.Itoa(f.Column))
		}
		b.WriteString(": ")
	}

	if f.Step != "" {
		b.WriteString("step ")
		b.WriteString(strconv.Quote(f.Step))
		b.WriteString(" ")
	}
	if f.Field != "" {
		b.WriteString(strconv.Quote(f.Field))
		b.WriteString(": ")
	}

	b.WriteString(f.Message)
	b.WriteString(" (docs/STYLE.md ")
	b.WriteString(string(f.Rule))
	b.WriteString(")")

	return b.String()
}

// Lint reports the tier-4 style suggestions a workflow's document earns, in
// source order.
//
// The positions come from the same [Positions] the parser hands back beside the
// workflow, so a nil one costs the lines and nothing else — the suggestions are
// still correct, they just cannot be pointed at.
//
// Every finding is advice about a file that compiles and validates. Nothing
// here is a defect and nothing here is refused; see this file's leading comment
// for what that buys and what it costs.
func Lint(wf *v1.Workflow, pos *Positions) []StyleFinding {
	var findings []StyleFinding

	findings = append(findings, nestedConditionals(wf, pos)...)
	findings = append(findings, repeatedExpressions(wf, pos)...)
	findings = append(findings, equalityDispatch(wf, pos)...)

	slices.SortStableFunc(findings, func(a, b StyleFinding) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		if a.Column != b.Column {
			return a.Column - b.Column
		}
		return strings.Compare(string(a.Rule), string(b.Rule))
	})

	return findings
}

// nestedConditionals reports an expression holding a ternary inside a ternary.
//
// R5, first threshold: "A ternary inside a ternary is a `switch:`, a `value:`
// step or a `vars:` entry. Mechanical shape, mechanical suggestion (tier 4)."
//
// The shape is exact rather than approximated. Part III counted this over the
// corpus by scanning each `${...}` span for `?` characters and discounting the
// ones belonging to an optional traversal, and said in as many words that it
// was "an approximation of the real check rather than the check itself". This
// is the real check: a conditional is [operators.Conditional] in the parsed
// tree, an optional traversal is not a conditional at all there, and no
// counting of characters is involved.
//
// One finding per expression, at the outermost nesting. A chain of four
// conditionals is one thing to rewrite, not three.
func nestedConditionals(wf *v1.Workflow, pos *Positions) []StyleFinding {
	var findings []StyleFinding
	budget := maxLintNodes

	exprSites(wf, pos, func(step, field, path string, val *v1.Value) {
		parsed := val.GetExpr()
		if parsed.GetExpr() == nil {
			return
		}

		// The shape the author wrote, with every macro put back as the call it
		// came from — the same resolution [Audit] does, and for a reason that
		// bites harder here: `filter` expands into a comprehension whose loop
		// step is a ternary nobody typed, so counting conditionals over the
		// expanded tree reports cel-go's expander rather than the file.
		written := resolveMacros(parsed.GetExpr(), parsed.GetSourceInfo().GetMacroCalls(), 0)
		if !holdsNestedConditional(written, false, &budget) {
			return
		}

		at := exprPosition(pos, step, field, parsed)
		findings = append(findings, StyleFinding{
			Rule:    StyleNestedConditional,
			Line:    at.Line,
			Column:  at.Column,
			Step:    step,
			Field:   field,
			Message: nestedConditionalAdvice(written),
		})
	})

	return findings
}

// nestedConditionalAdvice says what to write instead, which is not the same
// sentence in both positions this fires in.
//
// R5 offers three remedies — a `switch:`, a `value:` step, a `vars:` entry — and
// every one of them names the answer *somewhere else in the file*. That is
// available whenever the expression only reads what the whole file can read. It
// is not available where the expression reads a name bound exactly where it sits:
// a wait's `payload` and `timed_out` in its own `outputs:` shaping, a loop's item
// in its body. Telling an author there to "name it in a `value:` step" is telling
// them to write a file that does not compile, and a wrong remedy is the half of a
// diagnostic that costs the most to follow.
//
// The remedy that *is* available there is the one the charter's own positive
// example demonstrates: shape the raw facts into named outputs, and let the
// conditional live in a step that reads them. So the finding is the same finding
// and the sentence differs, rather than the check going quiet on the position
// where the nesting is hardest to read.
func nestedConditionalAdvice(written *expr.Expr) string {
	const problem = "this expression holds a conditional inside a conditional; "

	if hoistable(written) {
		return problem +
			"name the inner answer in a `value:` step and read it back as `${steps.<id>.value}`, " +
			"or dispatch on it with `switch:`"
	}

	return problem +
		"it reads a name bound where it is written, so shape those facts into their own " +
		"named outputs first and put the conditional in a `value:` step that reads them"
}

// holdsNestedConditional reports whether e holds a conditional under a
// conditional.
//
// inside says a conditional encloses this node already, which is what makes the
// next one a nesting rather than the first one.
//
// A comprehension is not descended into, and that is the difference between
// this and [auditCollector.walk]. [resolveMacros] has already replaced every
// comprehension the parser produced *from a macro* with the call the author
// wrote; a comprehension still standing here is one no macro call was recorded
// for, so its ternaries are the expander's own bookkeeping rather than anything
// in the file. Reporting those would be a suggestion to rewrite an expression
// that does not contain what the suggestion describes, which is the false
// diagnostic CLAUDE.md rates worse than a missing one.
func holdsNestedConditional(e *expr.Expr, inside bool, budget *int) bool {
	if e == nil || *budget <= 0 {
		return false
	}
	*budget--

	call, isCall := e.GetExprKind().(*expr.Expr_CallExpr)
	if isCall && call.CallExpr.GetFunction() == operators.Conditional {
		if inside {
			return true
		}
		inside = true
	}

	if _, isComprehension := e.GetExprKind().(*expr.Expr_ComprehensionExpr); isComprehension {
		return false
	}

	for _, child := range children(e) {
		if holdsNestedConditional(child, inside, budget) {
			return true
		}
	}

	return false
}

// repeatedExpressions reports an expression a file states three or more times.
//
// R5, third threshold: "A structurally repeated subexpression appearing three or
// more times is a `value:` step waiting for a name." The counting is [Audit]'s,
// unchanged and unduplicated — structural identity by [exprEqual], macros
// resolved, sub-expressions dropped where a larger one occurs as often — with
// this rule's own threshold applied on top of it and one further condition the
// measurement has no reason to care about.
//
// That condition is scope, and it is the thing a linter shares with a rewriter:
// the suggestion is to lift the expression into a `value:` step *elsewhere in
// the file*, so it is only sound when every name the expression reads is still
// readable there. A loop's `as:` binding, the `item` a loop binds when it writes
// no `as:`, a step's own `vars:` keys, `now` inside a wait, `payload` inside a
// signal's output shaping, `event` inside a trigger, `response` inside an http
// task's own inputs — all of them are bound where the expression sits and
// nowhere else. Advising an author to hoist an expression reading one of those
// is advising them to break the file.
//
// The test is positive rather than a list of names to subtract: every free
// identifier has to be one of the roots a workflow-level expression can read
// ([declarationRoots]). A list of locals to exclude is a list that is wrong the
// day a task binds a new name; a list of roots is the language's own, and
// anything not on it is by construction bound by something between the
// expression and the file.
//
// The rule never rewrites, and this never suggests a name — R5 says so, and the
// reason is anti-goal 9: a rewriter that guesses names is the bug class `flow
// fix` exists never to be.
func repeatedExpressions(wf *v1.Workflow, pos *Positions) []StyleFinding {
	var findings []StyleFinding

	for _, repeat := range Audit(wf, pos) {
		if repeat.Count() < styleRepeatThreshold {
			continue
		}
		if !hoistable(repeat.repr) {
			continue
		}

		first := repeat.Sites[0]
		lines := make([]string, 0, len(repeat.Sites))
		for _, site := range repeat.Sites {
			if site.Line > 0 {
				lines = append(lines, strconv.Itoa(site.Line))
			}
		}

		where := ""
		if len(lines) > 0 {
			where = ", on line" + plural(len(lines)) + " " + strings.Join(lines, ", ")
		}

		findings = append(findings, StyleFinding{
			Rule:  StyleRepeatedExpr,
			Line:  first.Line,
			Step:  first.Step,
			Field: first.Field,
			Message: fmt.Sprintf(
				"`%s` is stated %d times in this file%s; state it once in a `value:` step "+
					"and read it back as `${steps.<id>.value}`, so the copies cannot drift apart",
				repeat.Expr, repeat.Count(), where),
		})
	}

	return findings
}

// plural is the "s" a count of more than one earns.
func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// hoistable reports whether every name an expression reads is one that a
// `value:` step elsewhere in the file could read too.
//
// See [repeatedExpressions] for why the answer decides whether a suggestion is
// advice or damage. Fail-closed: an expression this cannot walk, or one holding
// a free name that is not a declared root, is left unreported.
func hoistable(e *expr.Expr) bool {
	if e == nil {
		return false
	}

	free := true
	var failure error

	walkFreeIdents(e, nil, func(_ int64, name string) error {
		if !isDeclarationRoot(name) {
			free = false
		}
		return nil
	}, &failure)

	return free && failure == nil
}

// equalityDispatch reports sibling `if:` steps that all test one value for
// equality against different literals.
//
// R5, fourth threshold: "A chain of sibling `if:` steps that all compare one
// value for equality against different literals is a `switch:`. Tier 4
// suggests. It never fires on siblings whose conditions are unrelated, or that
// partition a complement into named cases: both are legal and good style."
//
// The unit is a sibling group, which [v1.Walk.Steps] enumerates — the
// workflow's own top level and every `for_each` body, `loop:` body, `parallel`
// branch and `switch:` body under it. It is the same unit the negation-drift
// lint asks for and for the same reason: two steps are only comparable when
// they branch on the same run.
//
// Three narrowings keep it off the two shapes the charter names as good style,
// and each of them is a way this could be wrong rather than merely quiet:
//
//   - The condition has to be exactly `<subject> == <literal>`, in either
//     operand order and nothing else. A guard conjoined onto it
//     (`has(x) && x == "a"`) is not this shape: the guard has to go somewhere a
//     `switch:` has no room for, so suggesting one would be suggesting a
//     rewrite that does not exist.
//   - Every subject in the group has to be the same expression, by [exprEqual]
//     rather than by rendering. Siblings testing different values are the
//     "unrelated conditions" case, and they are the common one.
//   - The literals have to be distinct. Two steps on one value are two things
//     that both happen, which a `switch:` cannot express — its cases are
//     exclusive — so a repeated literal makes the whole group unreportable
//     rather than merely one member of it.
//
// A `!` never reaches any of that, which is what keeps this silent on a
// partitioned complement: `!(x == "a")` is not an equality call.
func equalityDispatch(wf *v1.Workflow, pos *Positions) []StyleFinding {
	var findings []StyleFinding

	v1.WalkNodes(wf.GetSteps(), v1.Walk{
		Steps: func(siblings []*v1.Node) {
			findings = append(findings, dispatchAmong(siblings, pos)...)
		},
	})

	return findings
}

// dispatchAmong reports the equality chains in one group of sibling steps.
func dispatchAmong(nodes []*v1.Node, pos *Positions) []StyleFinding {
	type arm struct {
		id      string
		subject *expr.Expr
		literal *expr.Constant
		parsed  *expr.ParsedExpr
	}

	// One bucket per subject, in the order the subjects are first written, so
	// the report follows the file rather than a map's iteration.
	var (
		subjects []*expr.Expr
		arms     = map[int][]arm{}
	)

	for _, node := range nodes {
		parsed := node.GetCondition().GetExpr()
		subject, literal, ok := equalityAgainstLiteral(parsed.GetExpr())
		if !ok {
			continue
		}

		index := slices.IndexFunc(subjects, func(seen *expr.Expr) bool {
			return exprEqual(seen, subject)
		})
		if index < 0 {
			subjects = append(subjects, subject)
			index = len(subjects) - 1
		}

		arms[index] = append(arms[index], arm{
			id:      node.GetId(),
			subject: subject,
			literal: literal,
			parsed:  parsed,
		})
	}

	var findings []StyleFinding

	for index, group := range arms {
		if len(group) < styleDispatchThreshold {
			continue
		}

		distinct := true
		for i := range group {
			for j := i + 1; j < len(group); j++ {
				if constExprEqual(group[i].literal, group[j].literal) {
					distinct = false
				}
			}
		}
		if !distinct {
			// Two steps that both run on one value. See [equalityDispatch] for
			// why that is not a `switch:` at all rather than a smaller one.
			continue
		}

		rendered, ok := renderExpr(subjects[index])
		if !ok {
			// No name to report the value under, and a suggestion that cannot
			// say what to dispatch on is not a suggestion.
			continue
		}

		ids := make([]string, 0, len(group))
		for _, a := range group {
			ids = append(ids, strconv.Quote(a.id))
		}

		first := group[0]
		at := exprPosition(pos, first.id, conditionKey, first.parsed)

		findings = append(findings, StyleFinding{
			Rule:   StyleEqualityDispatch,
			Line:   at.Line,
			Column: at.Column,
			Step:   first.id,
			Field:  conditionKey,
			Message: fmt.Sprintf(
				"steps %s each test `%s` for equality against a different literal; "+
					"write one `switch:` on that value with a `case:` per outcome, "+
					"so the validator can check the branches against the values it can take",
				strings.Join(ids, ", "), rendered),
		})
	}

	slices.SortStableFunc(findings, func(a, b StyleFinding) int { return a.Line - b.Line })

	return findings
}

// equalityAgainstLiteral reports whether a condition is exactly one `==`
// between a literal and something else, and if so which is which.
//
// A comparison of two literals (`1 == 1`) has no subject to dispatch on and is
// refused here rather than reported as a chain on a constant.
func equalityAgainstLiteral(e *expr.Expr) (subject *expr.Expr, literal *expr.Constant, ok bool) {
	call, isCall := e.GetExprKind().(*expr.Expr_CallExpr)
	if !isCall || call.CallExpr.GetFunction() != operators.Equals {
		return nil, nil, false
	}

	args := call.CallExpr.GetArgs()
	if len(args) != 2 {
		return nil, nil, false
	}

	left, leftIsConst := args[0].GetExprKind().(*expr.Expr_ConstExpr)
	right, rightIsConst := args[1].GetExprKind().(*expr.Expr_ConstExpr)

	switch {
	case leftIsConst && !rightIsConst:
		return args[1], left.ConstExpr, true
	case rightIsConst && !leftIsConst:
		return args[0], right.ConstExpr, true
	default:
		return nil, nil, false
	}
}
