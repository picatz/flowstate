package flowfile

import (
	"fmt"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A workflow states "the gate passed" once and "the gate did not pass" as its
// hand-written negation, because the language gives an author no way to name the
// gate itself — see issue #207. Nothing checks the pair stays in sync: edit one
// side and not the other, and the file still validates, still runs, and both
// branches — or neither — can fire.
//
// The full fix is a binding an author can name (#207's staged plan, step 3),
// which needs the loop slice's carried-state evaluator first. This is step 1 of
// that plan, shippable on its own: a lint that catches the pair drifting apart
// without adding a single word to the grammar.
//
// # What it catches, and why that shape
//
// Two sibling steps' `if:` conditions, each flattened on top-level `&&`, are
// compared clause by clause. Once the longest matching prefix is stripped, what
// is left on one side is checked for being a single `!(...)` wrapping something
// that flattens, the same way, to the same number of clauses the other side has
// left. If every one of those clauses still matches, the pair is an exact
// negation — today's healthy pattern, and silent. If exactly one clause differs
// on each side, the pair *was* an exact negation and one side moved without the
// other: that is the drift this reports.
//
// Anything else — clause counts that do not match, more than one clause
// differing, no explicit `!(...)` on either side once the shared prefix is
// gone — is left alone. A workflow legitimately has sibling conditions that
// share no relationship at all (`rejected` and `undecided` beside `deploy` in
// `examples/approval-gate/`), and others that partition one condition's
// complement into several named cases rather than negate it
// (`examples/enterprise-fund-transfer/`'s `rejected`/`expired`/
// `refused_unauthorized` beside `debit`). Both are silent by construction here:
// neither reduces to one side holding a single `!(...)` of the same clause
// count as the other. Per CLAUDE.md, a false "these should agree" on conditions
// that were never meant to is worse than missing a real drift, so every case
// this cannot say something definite about is left unreported.
//
// This also cannot see the reworked example's other two copies of the gate — the
// `decision` and `approver_subject` outputs are re-derivations, not negations,
// and #207's decision comment is explicit that the negation lint has no view of
// that half of the repetition. Naming the gate once, so all four copies read one
// value, is the language change staged behind this.

// checkNegationDrift reports sibling `if:` conditions that were, or still could
// be, exact negations of each other but no longer agree on every clause.
//
// Asked once per list of sibling steps, which [v1.WalkNodes] enumerates: the
// workflow's own top level, and every `for_each` body, `loop:` body, `parallel`
// branch and `switch` body under it. A sibling group is the unit because a negation
// pair only makes sense between steps that actually branch on the same run — a step
// nested three calls deep sits in a different namespace entirely, and comparing it
// against a top-level step would compare two things that never both existed at
// once.
//
// This is the walk that made [v1.Walk.Steps] a callback of its own rather than
// something a caller reconstructs from the nodes: the groups are what it asks
// about, and rebuilding them from a per-node walk is exactly the hand-kept list
// #508 exists to remove.
func checkNegationDrift(nodes []*v1.Node) Diagnostics {
	var ds Diagnostics

	v1.WalkNodes(nodes, v1.Walk{
		Steps: func(siblings []*v1.Node) {
			ds = append(ds, negationDriftAmong(siblings)...)
		},
	})

	return ds
}

// negationDriftAmong compares one group of sibling steps, pairwise, in file order.
func negationDriftAmong(nodes []*v1.Node) Diagnostics {
	var ds Diagnostics

	type candidate struct {
		id   string
		cond *expr.Expr
	}

	var conditioned []candidate
	for _, node := range nodes {
		if e := node.GetCondition().GetExpr().GetExpr(); e != nil {
			conditioned = append(conditioned, candidate{id: node.GetId(), cond: e})
		}
	}

	for i := 0; i < len(conditioned); i++ {
		for j := i + 1; j < len(conditioned); j++ {
			a, b := conditioned[i], conditioned[j]

			onlyA, onlyB, drifted := negationDrift(a.cond, b.cond)
			if !drifted {
				continue
			}

			ds = append(ds, negationDriftDiagnostics(a.id, b.id, onlyA, onlyB)...)
		}
	}

	return ds
}

// negationDrift compares two conditions and reports the one clause at fault on
// each side when, and only when, the pair is confidently an exact-negation
// relationship that has drifted by exactly one clause. See [checkNegationDrift]
// for what makes a pair confident enough to report and what does not.
func negationDrift(a, b *expr.Expr) (onlyA, onlyB *expr.Expr, drifted bool) {
	listA := flattenAnd(a)
	listB := flattenAnd(b)

	k := commonConjunctPrefix(listA, listB)
	remA, remB := listA[k:], listB[k:]

	if len(remA) == 0 && len(remB) == 0 {
		// Identical conditions once the shared prefix is removed means the two
		// conditions are identical outright — not a negation pair at all, and not
		// this lint's business. `debit`, `credit`, and `notify_settlement` in
		// examples/enterprise-fund-transfer/ share one condition verbatim; that
		// repetition is real (issue #207 names it) but it is not drift, because
		// there is no negation here to drift apart from.
		return nil, nil, false
	}

	// Try B as the negated side first — a single `!(...)` on B matching A's
	// remaining clauses — then A as the negated side. singleClauseDrift's first
	// return belongs to whichever list was passed as its first argument, so the
	// two calls below bind onlyA/onlyB in opposite order from each other.
	if negB, otherA, ok := singleClauseDrift(remB, remA); ok {
		return otherA, negB, otherA != nil || negB != nil
	}
	if negA, otherB, ok := singleClauseDrift(remA, remB); ok {
		return negA, otherB, negA != nil || otherB != nil
	}

	return nil, nil, false
}

// singleClauseDrift checks whether negated is a single `!(...)` clause whose
// operand, flattened the same way a top-level `&&` chain is, has exactly the
// same number of clauses as other — and if so, whether every clause matches
// (a healthy exact negation, reported as no drift) or exactly one does not (the
// drift this lint exists to catch).
//
// The length requirement is deliberate and narrow: it is what keeps this
// silent on `examples/enterprise-fund-transfer/`'s `debit` vs
// `refused_unauthorized` (four clauses negated against one) and on any other
// pair where the shapes do not actually correspond, rather than guessing at a
// relationship the file never stated.
func singleClauseDrift(negated, other []*expr.Expr) (onlyNegated, onlyOther *expr.Expr, ok bool) {
	if len(negated) != 1 {
		return nil, nil, false
	}

	inner, isNot := asLogicalNot(negated[0])
	if !isNot {
		return nil, nil, false
	}

	innerClauses := flattenAnd(inner)
	if len(innerClauses) != len(other) {
		return nil, nil, false
	}

	unmatchedInner, unmatchedOther := unmatchedClauses(innerClauses, other)

	switch {
	case len(unmatchedInner) == 0 && len(unmatchedOther) == 0:
		// Every clause matches: an exact negation, today's healthy pattern.
		return nil, nil, true
	case len(unmatchedInner) == 1 && len(unmatchedOther) == 1:
		// Exactly one clause differs on each side — the pair was an exact
		// negation and one side moved without the other.
		return unmatchedInner[0], unmatchedOther[0], true
	default:
		// More than one clause differs, or the multiset match is otherwise
		// ambiguous. Reporting a "the closest differing clause" guess here would
		// be inventing a relationship the file never stated — see CLAUDE.md on
		// false diagnostics costing more than missed ones.
		return nil, nil, false
	}
}

// unmatchedClauses finds a maximum structural pairing between two clause lists
// and returns what is left over on each side. Matching by content rather than
// by position, because `a && b` and `b && a` are the same clause set written in
// a different order and neither is the one this lint has an opinion about.
func unmatchedClauses(x, y []*expr.Expr) (onlyX, onlyY []*expr.Expr) {
	usedY := make([]bool, len(y))

	for _, cx := range x {
		matched := false
		for j, cy := range y {
			if usedY[j] {
				continue
			}
			if exprEqual(cx, cy) {
				usedY[j] = true
				matched = true
				break
			}
		}
		if !matched {
			onlyX = append(onlyX, cx)
		}
	}

	for j, cy := range y {
		if !usedY[j] {
			onlyY = append(onlyY, cy)
		}
	}

	return onlyX, onlyY
}

// commonConjunctPrefix returns how many leading clauses two flattened `&&`
// chains share, in order. A shared prefix is stripped before the negation
// check runs, because a real pair rarely negates a whole condition — it shares
// a guard (`has(...) && payload.approved`, in `examples/approval-gate/`) and
// negates only what follows it.
func commonConjunctPrefix(a, b []*expr.Expr) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && exprEqual(a[i], b[i]) {
		i++
	}
	return i
}

// flattenAnd unwraps a top-level `&&` chain into its clauses, left to right.
// Anything that is not itself a top-level `&&` call is a single clause.
func flattenAnd(e *expr.Expr) []*expr.Expr {
	if call, ok := e.GetExprKind().(*expr.Expr_CallExpr); ok {
		if call.CallExpr.GetFunction() == "_&&_" && len(call.CallExpr.GetArgs()) == 2 {
			args := call.CallExpr.GetArgs()
			return append(flattenAnd(args[0]), flattenAnd(args[1])...)
		}
	}
	return []*expr.Expr{e}
}

// asLogicalNot reports whether e is a unary `!` and, if so, what it negates.
func asLogicalNot(e *expr.Expr) (*expr.Expr, bool) {
	call, ok := e.GetExprKind().(*expr.Expr_CallExpr)
	if !ok {
		return nil, false
	}
	if call.CallExpr.GetFunction() != "!_" || len(call.CallExpr.GetArgs()) != 1 {
		return nil, false
	}
	return call.CallExpr.GetArgs()[0], true
}

// exprEqual reports whether two CEL expressions are structurally identical —
// same shape, same names, same literals — ignoring the parser-assigned Id on
// every node. Id numbers a position in one parse's own tree; two conditions
// parsed from two different `if:` fields never share a numbering, so comparing
// them at all requires ignoring it, and proto.Equal does not.
func exprEqual(a, b *expr.Expr) bool {
	if a == nil || b == nil {
		return a == b
	}

	switch av := a.GetExprKind().(type) {
	case *expr.Expr_ConstExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_ConstExpr)
		return ok && constExprEqual(av.ConstExpr, bv.ConstExpr)

	case *expr.Expr_IdentExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_IdentExpr)
		return ok && av.IdentExpr.GetName() == bv.IdentExpr.GetName()

	case *expr.Expr_SelectExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_SelectExpr)
		return ok &&
			av.SelectExpr.GetField() == bv.SelectExpr.GetField() &&
			av.SelectExpr.GetTestOnly() == bv.SelectExpr.GetTestOnly() &&
			exprEqual(av.SelectExpr.GetOperand(), bv.SelectExpr.GetOperand())

	case *expr.Expr_CallExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_CallExpr)
		if !ok || av.CallExpr.GetFunction() != bv.CallExpr.GetFunction() {
			return false
		}
		if !exprEqual(av.CallExpr.GetTarget(), bv.CallExpr.GetTarget()) {
			return false
		}
		aArgs, bArgs := av.CallExpr.GetArgs(), bv.CallExpr.GetArgs()
		if len(aArgs) != len(bArgs) {
			return false
		}
		for i := range aArgs {
			if !exprEqual(aArgs[i], bArgs[i]) {
				return false
			}
		}
		return true

	case *expr.Expr_ListExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_ListExpr)
		if !ok {
			return false
		}
		aEl, bEl := av.ListExpr.GetElements(), bv.ListExpr.GetElements()
		if len(aEl) != len(bEl) {
			return false
		}
		for i := range aEl {
			if !exprEqual(aEl[i], bEl[i]) {
				return false
			}
		}
		return true

	case *expr.Expr_StructExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_StructExpr)
		if !ok || av.StructExpr.GetMessageName() != bv.StructExpr.GetMessageName() {
			return false
		}
		aEn, bEn := av.StructExpr.GetEntries(), bv.StructExpr.GetEntries()
		if len(aEn) != len(bEn) {
			return false
		}
		for i := range aEn {
			if !exprEqual(aEn[i].GetMapKey(), bEn[i].GetMapKey()) ||
				aEn[i].GetFieldKey() != bEn[i].GetFieldKey() ||
				!exprEqual(aEn[i].GetValue(), bEn[i].GetValue()) {
				return false
			}
		}
		return true

	case *expr.Expr_ComprehensionExpr:
		bv, ok := b.GetExprKind().(*expr.Expr_ComprehensionExpr)
		if !ok {
			return false
		}
		ac, bc := av.ComprehensionExpr, bv.ComprehensionExpr
		return ac.GetIterVar() == bc.GetIterVar() &&
			ac.GetAccuVar() == bc.GetAccuVar() &&
			exprEqual(ac.GetIterRange(), bc.GetIterRange()) &&
			exprEqual(ac.GetAccuInit(), bc.GetAccuInit()) &&
			exprEqual(ac.GetLoopCondition(), bc.GetLoopCondition()) &&
			exprEqual(ac.GetLoopStep(), bc.GetLoopStep()) &&
			exprEqual(ac.GetResult(), bc.GetResult())

	default:
		return false
	}
}

// constExprEqual compares two CEL constant literals by value.
func constExprEqual(a, b *expr.Constant) bool {
	if a == nil || b == nil {
		return a == b
	}
	switch av := a.GetConstantKind().(type) {
	case *expr.Constant_BoolValue:
		bv, ok := b.GetConstantKind().(*expr.Constant_BoolValue)
		return ok && av.BoolValue == bv.BoolValue
	case *expr.Constant_Int64Value:
		bv, ok := b.GetConstantKind().(*expr.Constant_Int64Value)
		return ok && av.Int64Value == bv.Int64Value
	case *expr.Constant_Uint64Value:
		bv, ok := b.GetConstantKind().(*expr.Constant_Uint64Value)
		return ok && av.Uint64Value == bv.Uint64Value
	case *expr.Constant_DoubleValue:
		bv, ok := b.GetConstantKind().(*expr.Constant_DoubleValue)
		return ok && av.DoubleValue == bv.DoubleValue
	case *expr.Constant_StringValue:
		bv, ok := b.GetConstantKind().(*expr.Constant_StringValue)
		return ok && av.StringValue == bv.StringValue
	case *expr.Constant_BytesValue:
		bv, ok := b.GetConstantKind().(*expr.Constant_BytesValue)
		return ok && string(av.BytesValue) == string(bv.BytesValue)
	case *expr.Constant_NullValue:
		_, ok := b.GetConstantKind().(*expr.Constant_NullValue)
		return ok
	default:
		return false
	}
}

// negationDriftDiagnostics renders the two diagnostics a drifted pair produces
// — one against each step's `if:`, so both positions are reported the way
// [ValidateSource] locates any other two-sided problem: each diagnostic names
// both step ids and both differing clauses, and is positioned at its own
// step's `if:`.
func negationDriftDiagnostics(idA, idB string, onlyA, onlyB *expr.Expr) Diagnostics {
	textA := unparseOrRaw(onlyA)
	textB := unparseOrRaw(onlyB)

	return Diagnostics{
		{
			Step:  idA,
			Field: "if",
			Message: fmt.Sprintf(
				"this condition and step %q's look like they were meant to be exact negations of "+
					"each other, but one clause has drifted: here it is `%s`, while the matching "+
					"clause in %q is `%s`; make the two conditions exact negations again (or, if they "+
					"are meant to differ, change one so the shapes no longer match) so the branches "+
					"cannot both run, or both be skipped, on the same outcome",
				idB, textA, idB, textB),
		},
		{
			Step:  idB,
			Field: "if",
			Message: fmt.Sprintf(
				"this condition and step %q's look like they were meant to be exact negations of "+
					"each other, but one clause has drifted: here it is `%s`, while the matching "+
					"clause in %q is `%s`; make the two conditions exact negations again (or, if they "+
					"are meant to differ, change one so the shapes no longer match) so the branches "+
					"cannot both run, or both be skipped, on the same outcome",
				idA, textB, idA, textA),
		},
	}
}

// unparseOrRaw renders a clause back to CEL source for the diagnostic. Losing
// the fenced text this clause was written inside is fine — cel-go's own
// rendering of a comparison or a field path is exactly what an author typed,
// since neither has the whitespace or comments a human-only rendering would
// need to preserve. A nil clause, or one cel-go's unparser refuses, falls back
// to naming the problem rather than showing nothing.
func unparseOrRaw(e *expr.Expr) string {
	if e == nil {
		return "(nothing: the other side has a clause this side lacks)"
	}

	text, err := cel.AstToString(cel.ParsedExprToAst(&expr.ParsedExpr{Expr: e}))
	if err != nil {
		return "(an expression that could not be rendered back to source)"
	}
	return text
}
