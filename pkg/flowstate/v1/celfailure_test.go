package flowstatev1

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestARuntimeExpressionFailureNamesWhatFailed pins #1551: the sentence a failed
// expression ends in says which operator or selection failed, the operand types
// it saw, and the subexpression, where it used to say `no such overload` and
// nothing else. The original words stay first and unchanged, so a reader or a
// test that matched them still does.
func TestARuntimeExpressionFailureNamesWhatFailed(t *testing.T) {
	t.Parallel()

	steps := map[string]any{
		"n":        map[string]any{"value": int64(2)},
		"approval": map[string]any{"approved": "yes"},
	}

	for _, test := range []struct {
		name string
		expr string
		want []string
	}{
		{
			name: "a binary operator over mismatched types",
			expr: `steps.n.value + "x"`,
			want: []string{"no such overload", `operator "+"`, "(int, string)", "steps.n.value + \"x\""},
		},
		{
			name: "a ternary over a string where a bool was expected",
			expr: `steps.approval.approved ? "approved" : "rejected"`,
			want: []string{"no such overload", "(string, string, string)", "steps.approval.approved"},
		},
		{
			name: "a missing output names the outputs that exist",
			expr: `steps.n.valu`,
			want: []string{"no such key: valu", `selecting "valu"`, "available: value"},
		},
		{
			name: "a missing step names the steps that exist",
			expr: `steps.nope.value`,
			want: []string{"no such key: nope", "available: approval, n"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalInProfile(t, test.expr, map[string]any{"steps": steps})
			require.Error(t, err)

			for _, fragment := range test.want {
				assert.Contains(t, err.Error(), fragment)
			}
			assert.Contains(t, err.Error(), "evaluate expression: ", "the original prefix is kept")
		})
	}
}

// TestAFailureOutsideStepsDoesNotListWhatItHolds is the negative direction: a
// map the run built from data it fetched is not the author's names, so a miss in
// it says what was selected and does not list the keys it held.
func TestAFailureOutsideStepsDoesNotListWhatItHolds(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `inputs.payload.missing`, map[string]any{
		"inputs": map[string]any{"payload": map[string]any{"secret-looking-key": "v"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such key: missing")
	assert.NotContains(t, err.Error(), "secret-looking-key")
	assert.NotContains(t, err.Error(), "available:")
}

// TestAFailureInsideAStepOutputDoesNotListItsKeys pins the reviewer's F1: a
// step's output value is data the step fetched, so a miss one level below
// `steps.<id>` names the selection and never the keys the data held.
func TestAFailureInsideAStepOutputDoesNotListItsKeys(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `steps.fetch.value.missing`, map[string]any{
		"steps": map[string]any{"fetch": map[string]any{"value": map[string]any{"alice@example.com": int64(1)}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such key: missing")
	assert.NotContains(t, err.Error(), "alice@example.com")
	assert.NotContains(t, err.Error(), "available:")
}

// TestAComprehensionVariableIsNotResolvedFromTheOuterScope pins F2: an operand
// that reads a comprehension variable cannot be re-evaluated standalone, so its
// type is reported as unknown rather than taken from a same-named outer value.
func TestAComprehensionVariableIsNotResolvedFromTheOuterScope(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `[1].map(x, x + "a")`, map[string]any{"x": "outer"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such overload")
	assert.Contains(t, err.Error(), "(?, string)")
	assert.NotContains(t, err.Error(), "(string, string)")
}

// TestAFailureBoundsTheSubexpressionItEchoes pins the Copilot finding on #2400:
// the echoed subexpression is cut, so an oversized authored expression cannot
// grow the durable failure text with it.
func TestAFailureBoundsTheSubexpressionItEchoes(t *testing.T) {
	t.Parallel()

	long := `"` + strings.Repeat("a", 5000) + `" + 1`
	_, err := evalInProfile(t, long, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such overload")
	assert.Less(t, len(err.Error()), 1024)
}

// TestAMissingKeyListsOnlyNamesSpelledLikeDeclarations pins the sweep's finding
// that candidates come from the runtime map: a key shaped like data is never
// listed, whichever map it sits in.
func TestAMissingKeyListsOnlyNamesSpelledLikeDeclarations(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `steps.fetch.nope`, map[string]any{
		"steps": map[string]any{"fetch": map[string]any{
			"value":             int64(1),
			"alice@example.com": int64(2),
			"/etc/passwd":       int64(3),
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "available: value")
	assert.NotContains(t, err.Error(), "alice@example.com")
	assert.NotContains(t, err.Error(), "/etc/passwd")

	_, err = evalInProfile(t, `steps.fetch.nope`, map[string]any{
		"steps": map[string]any{"fetch": map[string]any{"alice@example.com": int64(2)}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "none of its names can be shown", "a filtered map is not reported as empty")
	assert.NotContains(t, err.Error(), "there are none")
}

// TestFailureDiagnosticsShareOneCostBudget pins that re-evaluating operands is
// paid from one budget: with the cost limit spent by the failed evaluation, the
// operands are not re-run on fresh budgets, so the types read `?`.
func TestFailureDiagnosticsShareOneCostBudget(t *testing.T) {
	t.Parallel()

	work := newFailureWork(10)
	require.True(t, work.take())
	work.spend(nil)
	assert.Zero(t, work.remaining, "an evaluation that reported no cost spends what is left")
	assert.False(t, work.take(), "nothing is left for another operand")

	counted := newFailureWork(0)
	counted.evals = 2
	require.True(t, counted.take())
	require.True(t, counted.take())
	assert.False(t, counted.take(), "the evaluation count bounds work even with no cost limit")
}

// TestAHyphenatedKeyIsNotListed pins that the shape is the canonical CEL
// identifier the declarations are held to, so `api-token` is not listed.
func TestAHyphenatedKeyIsNotListed(t *testing.T) {
	t.Parallel()

	assert.True(t, declaredNameShape("value"))
	assert.True(t, declaredNameShape("_x9"))
	assert.False(t, declaredNameShape("api-token"))
	assert.False(t, declaredNameShape("9lives"))
	assert.False(t, declaredNameShape(strings.Repeat("a", maxFailureNameLen+1)))
}

// TestFailureDiagnosticsChargeTheCostAnEvaluationReports pins the other half of
// the shared budget: an operand evaluation that finishes is charged its actual
// cost, so the next one starts from what is left rather than a fresh limit.
func TestFailureDiagnosticsChargeTheCostAnEvaluationReports(t *testing.T) {
	t.Parallel()

	env, err := cel.NewEnv()
	require.NoError(t, err)
	ast, iss := env.Compile(`[1, 2, 3].map(x, x + 1).size() > 0`)
	require.NoError(t, iss.Err())
	prog, err := env.Program(ast, cel.EvalOptions(cel.OptTrackCost))
	require.NoError(t, err)
	_, details, err := prog.Eval(map[string]any{})
	require.NoError(t, err)
	require.NotNil(t, details.ActualCost())
	require.NotZero(t, *details.ActualCost())

	const budget = 1000
	work := newFailureWork(budget)
	require.True(t, work.take())
	work.spend(details)
	assert.Equal(t, uint64(budget)-*details.ActualCost(), work.remaining,
		"a finished evaluation is charged what it cost")
	assert.Equal(t, work.remaining, work.limits(Limits{Cost: budget}).Cost,
		"the next evaluation is limited to what is left")

	work.spend(&cel.EvalDetails{})
	assert.Zero(t, work.remaining, "an evaluation that reports no cost spends the rest")
}

// TestAnExpressionFailureHasItsFieldsBesideItsSentence pins the structured half
// of #1551: the same facts the sentence states, as fields a program reads.
func TestAnExpressionFailureHasItsFieldsBesideItsSentence(t *testing.T) {
	t.Parallel()

	steps := map[string]any{"n": map[string]any{"value": int64(2)}}

	_, err := evalInProfile(t, `steps.n.value + "x"`, map[string]any{"steps": steps})
	detail := ExpressionFailureOf(err)
	require.NotNil(t, detail)
	assert.Equal(t, "+", detail.GetOperator())
	assert.Equal(t, []string{"int", "string"}, detail.GetOperandTypes())
	assert.Equal(t, `steps.n.value + "x"`, detail.GetSubexpression())
	assert.Empty(t, detail.GetSelected())
	require.NotNil(t, detail.Offset, "the operator's position in the expression is reported")
	assert.Equal(t, int32(strings.Index(`steps.n.value + "x"`, "+")), detail.GetOffset())

	_, err = evalInProfile(t, `steps.n.valu`, map[string]any{"steps": steps})
	detail = ExpressionFailureOf(err)
	require.NotNil(t, detail)
	assert.Equal(t, "valu", detail.GetSelected())
	assert.Equal(t, []string{"value"}, detail.GetCandidates())
	assert.Empty(t, detail.GetOperator())

	_, err = evalInProfile(t, `inputs.payload.missing`, map[string]any{
		"inputs": map[string]any{"payload": map[string]any{"k": "v"}},
	})
	detail = ExpressionFailureOf(err)
	require.NotNil(t, detail)
	assert.Empty(t, detail.GetCandidates(), "a map outside steps lists nothing")
}

// TestTheStructuredAccountHonorsTheLimitsItsSchemaDeclares pins that an
// over-long selected key is cut to the bound service.proto declares, so the
// fields can be returned without a second check.
func TestTheStructuredAccountHonorsTheLimitsItsSchemaDeclares(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("k", 1000)
	_, err := evalInProfile(t, `inputs.payload.`+long, map[string]any{
		"inputs": map[string]any{"payload": map[string]any{}},
	})
	detail := ExpressionFailureOf(err)
	require.NotNil(t, detail)
	assert.LessOrEqual(t, len(detail.GetSelected()), maxFailureFieldBytes)
	assert.LessOrEqual(t, len(detail.GetSubexpression()), 512)
}

// TestAWithheldFailureDropsItsStructuredAccount pins the fail-closed rule: a
// response whose message is redacted or withheld for a sensitive value does not
// keep the expression fields, which quote the same text.
func TestAWithheldFailureDropsItsStructuredAccount(t *testing.T) {
	t.Parallel()

	failing := func() *GetResponse {
		return &GetResponse{Kind: &GetResponse_Error{Error: &RunResponse_Error{
			Message:    "evaluate expression: no such overload",
			Kind:       ErrorKindExpression.String(),
			Expression: &ExpressionFailure{Operator: "+", Subexpression: `inputs.pin + "x"`},
		}}}
	}

	withheld := failing()
	WithholdGetResponseFailures(withheld)
	assert.Nil(t, withheld.GetError().GetExpression())
	assert.Equal(t, FailureWithheldMarker, withheld.GetError().GetMessage())

	redacted := RedactGetResponseFailures(failing(), SensitiveInputValues(
		map[string]*Value{"pin": NewLiteral("hunter2")}, map[string]bool{"pin": true}))
	assert.Nil(t, redacted.GetError().GetExpression(), "a declared sensitive value drops the fields that quote the expression")
	assert.Equal(t, ErrorKindExpression.String(), redacted.GetError().GetKind(), "the classification stays")

	unchanged := RedactGetResponseFailures(failing(), SensitiveValues{})
	assert.NotNil(t, unchanged.GetError().GetExpression(), "no sensitive value declared, so nothing is dropped")
}

// TestAnExpressionFailureDrawsACaretUnderItsOperator pins the two-line excerpt a
// person reads: the failing subexpression and a caret under the operator, the
// method name, or the selected key, and no caret at all where the unparsed text
// does not place it exactly.
func TestAnExpressionFailureDrawsACaretUnderItsOperator(t *testing.T) {
	t.Parallel()

	activation := map[string]any{
		"steps":  map[string]any{"n": map[string]any{"value": int64(2)}},
		"inputs": map[string]any{"s": "text", "m": map[string]any{"k": "v"}},
	}

	for _, tc := range []struct {
		name, expression, want string
	}{
		{"binary", `steps.n.value + "x"`, "steps.n.value + \"x\"\n              ^"},
		{"select", `steps.n.valu`, "steps.n.valu\n        ^"},
		{"method", `inputs.s.startsWith(1)`, "inputs.s.startsWith(1)\n         ^"},
		{"ternary", `inputs.s ? 1 : 2`, "inputs.s ? 1 : 2\n         ^"},
		{"index", `inputs.m[1]`, "inputs.m[1]\n        ^"},
		// The unparser adds parentheses here, so the operands' own renderings
		// do not reassemble into the subexpression and no caret is drawn.
		{"parenthesized", `(1 + 2) * "x"`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := evalInProfile(t, tc.expression, activation)
			detail := ExpressionFailureOf(err)
			require.NotNil(t, detail)
			assert.Equal(t, tc.want, detail.Excerpt(""))
		})
	}

	assert.Empty(t, (*ExpressionFailure)(nil).Excerpt(""))
	// A wide or combining character before the caret misaligns terminal padding,
	// so no excerpt is drawn; one after it does not matter.
	assert.Empty(t, (&ExpressionFailure{Subexpression: "\"界\" + a", Caret: proto.Int32(4)}).Excerpt(""))
	assert.Empty(t, (&ExpressionFailure{Subexpression: "\"e\u0301\" + a", Caret: proto.Int32(5)}).Excerpt(""))
	assert.Equal(t, "a + \"界\"\n  ^", (&ExpressionFailure{Subexpression: "a + \"界\"", Caret: proto.Int32(2)}).Excerpt(""))
	// A caret a peer sent outside its text draws nothing instead of panicking
	// or allocating a column of the peer's choosing.
	assert.Empty(t, (&ExpressionFailure{Subexpression: "a", Caret: proto.Int32(-1)}).Excerpt(""))
	assert.Empty(t, (&ExpressionFailure{Subexpression: "a", Caret: proto.Int32(1)}).Excerpt(""))
	assert.Empty(t, (&ExpressionFailure{Subexpression: "a", Caret: proto.Int32(1 << 30)}).Excerpt(""))
	assert.Equal(t, "  ab\n   ^", (&ExpressionFailure{Subexpression: "ab", Caret: proto.Int32(1)}).Excerpt("  "))
}

// TestAttributeToStepKeepsTheInnermostStep pins that a failure raised in a
// nested step keeps that step when an enclosing one wraps it.
func TestAttributeToStepKeepsTheInnermostStep(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `steps.n.value + "x"`, map[string]any{
		"steps": map[string]any{"n": map[string]any{"value": int64(2)}},
	})
	require.Error(t, err)

	AttributeToStep(err, &Node{Id: "inner", Source: &SourceLocation{File: "w.yaml", Line: 3, Column: 5}})
	AttributeToStep(err, &Node{Id: "outer", Source: &SourceLocation{File: "w.yaml", Line: 9, Column: 1}})
	assert.Equal(t, "inner", ExpressionFailureOf(err).GetStep())
	assert.EqualValues(t, 3, ExpressionFailureOf(err).GetLocation().GetLine(), "the innermost step's location is kept with it")

	assert.NotPanics(t, func() { AttributeToStep(nil, &Node{Id: "x"}) })
	assert.Equal(t, "plain", AttributeToStep(errors.New("plain"), &Node{Id: "x"}).Error())
}

// TestQualifyStepMarksAFailureThatCrossedACall pins that a callee's step is
// prefixed with its workflow, so the caller's file cannot claim it, and that an
// enclosing step does not overwrite it afterwards.
func TestQualifyStepMarksAFailureThatCrossedACall(t *testing.T) {
	t.Parallel()

	_, err := evalInProfile(t, `steps.n.value + "x"`, map[string]any{
		"steps": map[string]any{"n": map[string]any{"value": int64(2)}},
	})
	require.Error(t, err)

	AttributeToStep(err, &Node{Id: "first"})
	QualifyStepWithin(err, "callee")
	AttributeToStep(err, &Node{Id: "called"})
	assert.Equal(t, "callee/first", ExpressionFailureOf(err).GetStep())

	var nilFailure *ExpressionFailure
	assert.NotPanics(t, func() { nilFailure.QualifyStep("x"); nilFailure.AttributeStep("x", nil) })
}
