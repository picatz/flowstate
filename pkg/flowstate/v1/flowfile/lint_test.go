package flowfile

import (
	"strings"
	"testing"

	"github.com/google/cel-go/common/operators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Every tier-4 check gets two tests, and the second one is the one that matters.
//
// The first is that a real violation is reported, at the position an author can
// go to. The second is that the shapes the charter calls good style stay silent:
// siblings whose conditions are unrelated, a complement partitioned into named
// cases, and — the class CLAUDE.md's rewriter section records — the four names
// the grammar binds bare. A loop's `as:`, the `item` a loop binds when it writes
// none, a step's own `vars:` keys and `now` inside a wait are all legal beside a
// step of the same id, and every one of them is a name an expression can read
// only where it sits. A suggestion to lift such an expression somewhere else is
// advice that breaks the file, which is the linter's version of the corruption
// `flow fix` has shipped twice.
//
// A finding is asserted by rule and position rather than by message text, except
// where the text *is* the claim — the two remedies the nested-conditional check
// chooses between are different advice, so which one an author is given is
// behaviour and is asserted as such.

// lintOf compiles a Flowfile and returns what the lint makes of it.
func lintOf(t *testing.T, src string) []StyleFinding {
	t.Helper()

	wf, positions, err := Parse([]byte(src))
	require.NoError(t, err, "fixture did not compile:\n%s", src)

	return Lint(wf, positions)
}

// findingsFor returns the findings of one rule.
func findingsFor(found []StyleFinding, rule StyleRule) []StyleFinding {
	var out []StyleFinding
	for _, finding := range found {
		if finding.Rule == rule {
			out = append(out, finding)
		}
	}
	return out
}

// requireNoFindings fails with every finding rendered, because a lint test that
// says only "1 != 0" makes the reader run the fixture by hand to see what fired.
func requireNoFindings(t *testing.T, found []StyleFinding) {
	t.Helper()

	if len(found) == 0 {
		return
	}

	rendered := make([]string, 0, len(found))
	for _, finding := range found {
		rendered = append(rendered, finding.String())
	}
	t.Fatalf("expected no style findings, got:\n%s", strings.Join(rendered, "\n"))
}

// TestLintReportsANestedConditional is R5's first threshold, positive.
func TestLintReportsANestedConditional(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: nested
inputs:
  amount:
    type: int
steps:
  - id: band
    value: '${inputs.amount > 100 ? "high" : (inputs.amount > 10 ? "medium" : "low")}'
  - id: say
    log:
      message: ${steps.band.value}
`)

	nested := findingsFor(found, StyleNestedConditional)
	require.Len(t, nested, 1)

	assert.Equal(t, "band", nested[0].Step)
	assert.Equal(t, "value", nested[0].Field)
	assert.Equal(t, 8, nested[0].Line, "the finding lands on the line the expression is written on")
	assert.Contains(t, nested[0].Message, "`value:` step",
		"the remedy is available here, because the expression reads only rooted names")
	assert.Contains(t, nested[0].String(), "docs/STYLE.md R5/nested-conditional",
		"a finding names the rule an author can look up")
}

// TestLintReportsANestedConditionalOnceForAChain keeps a chain of conditionals
// one thing to rewrite rather than one finding per level.
func TestLintReportsANestedConditionalOnceForAChain(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: chain
inputs:
  amount:
    type: int
steps:
  - id: band
    value: '${inputs.amount > 100 ? "a" : inputs.amount > 50 ? "b" : inputs.amount > 10 ? "c" : "d"}'
`)

	assert.Len(t, findingsFor(found, StyleNestedConditional), 1)
}

// TestLintAdvisesShapingWhereANameIsBoundWhereItIsWritten is the other half of
// the nested-conditional check: the finding is the same, the remedy is not.
//
// A wait's `outputs:` shaping binds `payload` and `timed_out` exactly there, so
// "name it in a `value:` step" is advice that does not compile. See
// [nestedConditionalAdvice].
func TestLintAdvisesShapingWhereANameIsBoundWhereItIsWritten(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: shaped
steps:
  - id: review
    wait_for_signal:
      name: reviewed
      timeout: 24h
      outputs:
        outcome: '${timed_out ? "no_response" : (payload.approved ? "approved" : "rejected")}'
`)

	nested := findingsFor(found, StyleNestedConditional)
	require.Len(t, nested, 1)

	assert.Contains(t, nested[0].Message, "shape those facts into their own named outputs")
	assert.NotContains(t, nested[0].Message, "read it back as",
		"the hoisting remedy is not available where the names are bound")
}

// TestLintIsSilentOnAnOptionalTraversalAndASingleConditional is the negative for
// R5's first threshold.
//
// `x.?y.orValue(d)` is the canonical spelling the decided-spellings table asks
// for and `flow fix` already produces, and its `?` is not a conditional at all.
// A checker counting `?` characters in the source — the approximation Part III
// used to measure the corpus — reports both of these.
func TestLintIsSilentOnAnOptionalTraversalAndASingleConditional(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: fine
inputs:
  order:
    type: struct
steps:
  - id: label
    value: ${inputs.order.?label.orValue("none")}
  - id: band
    value: '${inputs.order.?total.orValue(0) > 100 ? "high" : "low"}'
`)

	requireNoFindings(t, findingsFor(found, StyleNestedConditional))
}

// TestLintReportsAnExpressionStatedThreeTimes is R5's third threshold, positive.
func TestLintReportsAnExpressionStatedThreeTimes(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: repeated
vars:
  names:
    - a
    - b
steps:
  - id: first
    log:
      message: ${string(size(vars.names))}
  - id: second
    log:
      message: ${string(size(vars.names))}
  - id: third
    log:
      message: ${string(size(vars.names))}
`)

	repeats := findingsFor(found, StyleRepeatedExpr)
	require.Len(t, repeats, 1)

	assert.Equal(t, "first", repeats[0].Step)
	assert.Contains(t, repeats[0].Message, "stated 3 times")
	assert.Contains(t, repeats[0].Message, "`value:` step")
}

// TestLintIsSilentOnAPairOfRepeats holds the suggestion to R5's threshold, which
// is deliberately above `flow audit`'s.
func TestLintIsSilentOnAPairOfRepeats(t *testing.T) {
	const src = `edition: v2026.3
name: twice
vars:
  names:
    - a
steps:
  - id: first
    log:
      message: ${string(size(vars.names))}
  - id: second
    log:
      message: ${string(size(vars.names))}
`

	wf, positions, err := Parse([]byte(src))
	require.NoError(t, err)

	require.Len(t, Audit(wf, positions), 1, "the measurement counts a pair")
	requireNoFindings(t, findingsFor(Lint(wf, positions), StyleRepeatedExpr))
}

// TestLintIsSilentOnARepeatReadingALoopBinding is the grammar-bound negative for
// R5's third threshold, and the reason the check asks about scope at all.
//
// `item` is bound for the loop's body and nowhere else. A `value:` step holding
// `size(item.parts)` does not compile, so the suggestion this check exists to
// make is one an author cannot take — which makes reporting it worse than
// silence.
func TestLintIsSilentOnARepeatReadingALoopBinding(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: loop-bound
inputs:
  orders:
    type: list
steps:
  - id: each
    for_each:
      items: ${inputs.orders}
      steps:
        - id: one
          log:
            message: ${string(size(item.parts))}
        - id: two
          log:
            message: ${string(size(item.parts))}
        - id: three
          log:
            message: ${string(size(item.parts))}
`)

	requireNoFindings(t, findingsFor(found, StyleRepeatedExpr))
}

// TestLintIsSilentOnARepeatReadingANamedLoopBinding is the same negative for the
// name an author writes rather than the default one.
func TestLintIsSilentOnARepeatReadingANamedLoopBinding(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: loop-named
inputs:
  orders:
    type: list
steps:
  - id: each
    for_each:
      items: ${inputs.orders}
      as: order
      steps:
        - id: one
          log:
            message: ${string(size(order.parts))}
        - id: two
          log:
            message: ${string(size(order.parts))}
        - id: three
          log:
            message: ${string(size(order.parts))}
`)

	requireNoFindings(t, findingsFor(found, StyleRepeatedExpr))
}

// TestLintIsSilentOnARepeatReadingAStepVar is the same negative for a step's own
// `vars:` keys, which are bound bare for the rest of that step and nowhere else.
func TestLintIsSilentOnARepeatReadingAStepVar(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: step-vars
steps:
  - id: one
    vars:
      parts:
        - a
    log:
      message: ${string(size(parts))}
  - id: two
    vars:
      parts:
        - a
    log:
      message: ${string(size(parts))}
  - id: three
    vars:
      parts:
        - a
    log:
      message: ${string(size(parts))}
`)

	requireNoFindings(t, findingsFor(found, StyleRepeatedExpr))
}

// TestLintIsSilentOnARepeatReadingNowInAWait is the same negative for `now`,
// which is bound inside a wait's expressions and nowhere else.
func TestLintIsSilentOnARepeatReadingNowInAWait(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: waits
steps:
  - id: first
    wait_until: ${now + duration("1h")}
  - id: second
    wait_until: ${now + duration("1h")}
  - id: third
    wait_until: ${now + duration("1h")}
`)

	requireNoFindings(t, findingsFor(found, StyleRepeatedExpr))
}

// TestLintReportsEqualityDispatch is R5's fourth threshold, positive.
func TestLintReportsEqualityDispatch(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: dispatch
steps:
  - id: outcome
    value: approved
  - id: pay
    if: ${steps.outcome.value == "approved"}
    log:
      message: paying
  - id: decline
    if: ${steps.outcome.value == "rejected"}
    log:
      message: declining
  - id: chase
    if: ${steps.outcome.value == "unknown"}
    log:
      message: chasing
`)

	dispatch := findingsFor(found, StyleEqualityDispatch)
	require.Len(t, dispatch, 1)

	assert.Equal(t, "pay", dispatch[0].Step, "the finding lands on the first arm")
	assert.Equal(t, conditionKey, dispatch[0].Field)
	assert.Contains(t, dispatch[0].Message, `steps.outcome.value`)
	assert.Contains(t, dispatch[0].Message, "`switch:`")
	assert.Contains(t, dispatch[0].Message, `"pay", "decline", "chase"`)
}

// TestLintIsSilentOnTwoArms holds the dispatch suggestion to the three outcomes
// the decided-spellings table names.
func TestLintIsSilentOnTwoArms(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: two
steps:
  - id: outcome
    value: approved
  - id: pay
    if: ${steps.outcome.value == "approved"}
    log:
      message: paying
  - id: decline
    if: ${steps.outcome.value == "rejected"}
    log:
      message: declining
`)

	requireNoFindings(t, findingsFor(found, StyleEqualityDispatch))
}

// TestLintIsSilentOnUnrelatedSiblingConditions is the first negative the charter
// names by name: siblings whose conditions have nothing to do with each other.
func TestLintIsSilentOnUnrelatedSiblingConditions(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: unrelated
inputs:
  amount:
    type: int
  region:
    type: string
  urgent:
    type: bool
steps:
  - id: big
    if: ${inputs.amount == 100}
    log:
      message: big
  - id: local
    if: ${inputs.region == "eu"}
    log:
      message: local
  - id: rush
    if: ${inputs.urgent == true}
    log:
      message: rush
`)

	requireNoFindings(t, findingsFor(found, StyleEqualityDispatch))
}

// TestLintIsSilentOnAPartitionedComplement is the second negative the charter
// names: a condition and the named cases its complement is split into. A `!` is
// not an equality call, so the group never reaches the threshold.
func TestLintIsSilentOnAPartitionedComplement(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: complement
inputs:
  decision:
    type: string
steps:
  - id: approve
    if: ${inputs.decision == "approved"}
    log:
      message: approving
  - id: rejected
    if: ${!(inputs.decision == "approved") && inputs.decision == "rejected"}
    log:
      message: rejected
  - id: expired
    if: ${!(inputs.decision == "approved") && inputs.decision == "expired"}
    log:
      message: expired
`)

	requireNoFindings(t, findingsFor(found, StyleEqualityDispatch))
}

// TestLintIsSilentOnEqualityChainsInDifferentSiblingGroups keeps the unit the
// sibling group rather than the file: steps that never both exist at once are
// not arms of one dispatch.
func TestLintIsSilentOnEqualityChainsInDifferentSiblingGroups(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: groups
inputs:
  decision:
    type: string
  orders:
    type: list
steps:
  - id: pay
    if: ${inputs.decision == "approved"}
    log:
      message: paying
  - id: each
    for_each:
      items: ${inputs.orders}
      steps:
        - id: decline
          if: ${inputs.decision == "rejected"}
          log:
            message: declining
        - id: chase
          if: ${inputs.decision == "unknown"}
          log:
            message: chasing
`)

	requireNoFindings(t, findingsFor(found, StyleEqualityDispatch))
}

// TestLintIsSilentOnRepeatedLiteralsInADispatch is the third narrowing: two
// steps that run on one value are two things that both happen, and a `switch:`
// cannot express that at all.
func TestLintIsSilentOnRepeatedLiteralsInADispatch(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: repeated-literal
steps:
  - id: outcome
    value: approved
  - id: pay
    if: ${steps.outcome.value == "approved"}
    log:
      message: paying
  - id: notify
    if: ${steps.outcome.value == "approved"}
    log:
      message: notifying
  - id: decline
    if: ${steps.outcome.value == "rejected"}
    log:
      message: declining
`)

	requireNoFindings(t, findingsFor(found, StyleEqualityDispatch))
}

// TestLintIsSilentOnAGuardedEqualityChain keeps the check off a shape whose
// rewrite does not exist: a guard conjoined onto the equality has nowhere to go
// in a `switch:`.
func TestLintIsSilentOnAGuardedEqualityChain(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: guarded
inputs:
  order:
    type: struct
steps:
  - id: pay
    if: ${has(inputs.order.decision) && inputs.order.decision == "approved"}
    log:
      message: paying
  - id: decline
    if: ${has(inputs.order.decision) && inputs.order.decision == "rejected"}
    log:
      message: declining
  - id: chase
    if: ${has(inputs.order.decision) && inputs.order.decision == "unknown"}
    log:
      message: chasing
`)

	requireNoFindings(t, findingsFor(found, StyleEqualityDispatch))
}

// TestLintIsSilentOnTheCharterPositiveExample is R5's own worked example,
// compiled: the file the charter shows as what to write earns no finding at all.
//
// It is the closing claim of the whole tier, and the one that would fail first
// if a check drifted into firing on good style: docs/STYLE.md shows this file as
// the answer, and a lint disagreeing with the document it cites would be worse
// than either one alone.
func TestLintIsSilentOnTheCharterPositiveExample(t *testing.T) {
	found := lintOf(t, `edition: v2026.3
name: refund-dispatch
description: Settle a refund on the outcome a reviewer sent.
inputs:
  amount:
    type: int
    must: this > 0
steps:
  - id: review
    wait_for_signal:
      name: refund-reviewed
      timeout: 24h
      outputs:
        decision: ${payload.?decision.orValue("rejected")}
        timed_out: ${timed_out}
  - id: outcome
    value: '${steps.review.timed_out ? "no_response" : steps.review.decision}'
  - id: settle
    switch:
      value: ${steps.outcome.value}
      cases:
        - case: approved
          steps:
            - id: pay
              log:
                message: paying the refund
        - case: rejected
          steps:
            - id: decline
              log:
                message: declining the refund
      default:
        steps:
          - id: chase
            log:
              level: warn
              message: nobody reviewed the refund before the deadline
`)

	requireNoFindings(t, found)
}

// TestLintDoesNotReportAnUnresolvedComprehensionsTernaries reaches the one
// branch of the nesting walk that no Flowfile in this corpus reaches.
//
// A `ComprehensionExpr` has no surface syntax: it is what the parser expands a
// macro into, and `filter`'s expansion is `cond ? accu + [x] : accu` — a ternary
// nobody typed, sitting inside another one the moment the file's own expression
// wraps it. [resolveMacros] normally puts the macro back as the call it was
// written as, so the walk never meets one; when the record is missing or the
// resolution bottoms out at its depth bound, it does. Reporting the expander's
// own bookkeeping as an author's nested conditional is a suggestion to rewrite
// something the file does not contain.
//
// Built here rather than parsed, because a parse that produced this would be
// the bug. Removing the guard in [holdsNestedConditional] fails this and nothing
// else in the suite.
func TestLintDoesNotReportAnUnresolvedComprehensionsTernaries(t *testing.T) {
	ternary := func(then, els *expr.Expr) *expr.Expr {
		return &expr.Expr{ExprKind: &expr.Expr_CallExpr{CallExpr: &expr.Expr_Call{
			Function: operators.Conditional,
			Args: []*expr.Expr{
				{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: "cond"}}},
				then, els,
			},
		}}}
	}
	name := func(id string) *expr.Expr {
		return &expr.Expr{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: id}}}
	}

	nested := ternary(ternary(name("a"), name("b")), name("c"))

	budget := maxLintNodes
	require.True(t, holdsNestedConditional(nested, false, &budget),
		"the fixture is a nesting when nothing hides it")

	comprehension := &expr.Expr{ExprKind: &expr.Expr_ComprehensionExpr{
		ComprehensionExpr: &expr.Expr_Comprehension{
			IterVar:  "x",
			AccuVar:  "@result",
			LoopStep: nested,
			Result:   name("@result"),
		},
	}}

	budget = maxLintNodes
	assert.False(t, holdsNestedConditional(comprehension, false, &budget),
		"an expansion the walk cannot attribute to anything the author wrote is not reported")
}

// TestLintStopsAtItsNodeBudget reaches the bound rather than merely staying
// under it.
//
// [maxLintNodes] exists because the walk is over somebody else's document and
// nothing else caps the sum over a file of arbitrarily many expressions. A bound
// nothing reaches is a bound nothing tests, so this drives the walk with a
// budget of one over a nesting that would otherwise report: the answer past the
// bound is "not found", which is the missed suggestion the bound is willing to
// cost, and never a walk that keeps going.
func TestLintStopsAtItsNodeBudget(t *testing.T) {
	wf, _, err := Parse([]byte(`edition: v2026.3
name: nested
inputs:
  amount:
    type: int
steps:
  - id: band
    value: '${inputs.amount > 100 ? "high" : (inputs.amount > 10 ? "medium" : "low")}'
`))
	require.NoError(t, err)

	parsed := wf.GetSteps()[0].GetValue().GetExpr()
	require.NotNil(t, parsed.GetExpr(), "the fixture's expression is what this walks")

	written := resolveMacros(parsed.GetExpr(), parsed.GetSourceInfo().GetMacroCalls(), 0)

	spent := maxLintNodes
	assert.True(t, holdsNestedConditional(written, false, &spent),
		"with the real budget the nesting is found")
	assert.Less(t, spent, maxLintNodes, "the walk spends budget")

	exhausted := 1
	assert.False(t, holdsNestedConditional(written, false, &exhausted),
		"past the budget the walk stops looking rather than continuing")
}

// TestLintWithoutPositionsStillReports keeps the suggestions correct when there
// is nothing to point at, which is what a caller holding only a compiled
// workflow has.
func TestLintWithoutPositionsStillReports(t *testing.T) {
	wf, _, err := Parse([]byte(`edition: v2026.3
name: nested
inputs:
  amount:
    type: int
steps:
  - id: band
    value: '${inputs.amount > 100 ? "high" : (inputs.amount > 10 ? "medium" : "low")}'
`))
	require.NoError(t, err)

	found := Lint(wf, nil)
	require.Len(t, found, 1)
	assert.Zero(t, found[0].Line)
	assert.NotContains(t, found[0].String(), "0:", "an unknown position renders as no position")
}
