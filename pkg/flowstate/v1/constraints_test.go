package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// constrainedWorkflow declares one input of the given shape, for the bind-time
// bite tests below — the choke point every caller of BindRunInputs shares.
func constrainedWorkflow(decl *v1.InputDeclaration) *v1.Workflow {
	return &v1.Workflow{
		Name:           "constrained",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{decl},
		Steps: []*v1.Node{{
			Id:   "a",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")}}},
		}},
	}
}

// TestBindRunInputsEnforcesStandardRules is the submit-time bite: a value that
// satisfies the declared *type* but not the declared *constraint* is refused
// before the run starts, which is BindRunInputs's whole job.
func TestBindRunInputsEnforcesStandardRules(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		decl  *v1.InputDeclaration
		value *v1.Value
		says  string
	}{
		{
			name:  "pattern",
			decl:  &v1.InputDeclaration{Name: "email", Type: v1.InputDeclaration_TYPE_STRING, Pattern: strPtr(`^[^@]+@[^@]+$`)},
			value: v1.NewLiteral("not-an-email"),
			says:  "must match pattern",
		},
		{
			name:  "min_len",
			decl:  &v1.InputDeclaration{Name: "name", Type: v1.InputDeclaration_TYPE_STRING, MinLen: u64Ptr(3)},
			value: v1.NewLiteral("ab"),
			says:  "at least 3 character",
		},
		{
			name:  "max_len",
			decl:  &v1.InputDeclaration{Name: "name", Type: v1.InputDeclaration_TYPE_STRING, MaxLen: u64Ptr(3)},
			value: v1.NewLiteral("abcd"),
			says:  "at most 3 character",
		},
		{
			name:  "min",
			decl:  &v1.InputDeclaration{Name: "replicas", Type: v1.InputDeclaration_TYPE_INT, Min: f64Ptr(1)},
			value: v1.NewLiteral(int64(0)),
			says:  "must be >= 1",
		},
		{
			name:  "max",
			decl:  &v1.InputDeclaration{Name: "replicas", Type: v1.InputDeclaration_TYPE_INT, Max: f64Ptr(50)},
			value: v1.NewLiteral(int64(51)),
			says:  "must be <= 50",
		},
		{
			name:  "min_items",
			decl:  &v1.InputDeclaration{Name: "regions", Type: v1.InputDeclaration_TYPE_LIST, MinItems: u64Ptr(1)},
			value: v1.NewLiteralList(),
			says:  "at least 1 item",
		},
		{
			name:  "max_items",
			decl:  &v1.InputDeclaration{Name: "regions", Type: v1.InputDeclaration_TYPE_LIST, MaxItems: u64Ptr(2)},
			value: v1.NewLiteralList("a", "b", "c"),
			says:  "at most 2 item",
		},
		{
			name:  "unique",
			decl:  &v1.InputDeclaration{Name: "regions", Type: v1.InputDeclaration_TYPE_LIST, Unique: true},
			value: v1.NewLiteralList("a", "b", "a"),
			says:  "unique",
		},
		{
			name:  "must",
			decl:  &v1.InputDeclaration{Name: "budget", Type: v1.InputDeclaration_TYPE_STRING, Must: strPtr(`this == "unlimited" || this.matches("^[0-9]+$")`)},
			value: v1.NewLiteral("lots"),
			says:  "must satisfy",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf := constrainedWorkflow(test.decl)
			_, err := v1.BindRunInputs(wf, map[string]*v1.Value{test.decl.GetName(): test.value})
			require.Error(t, err, "a value violating the constraint was accepted")
			assert.Contains(t, err.Error(), test.says)
			assert.Contains(t, err.Error(), test.decl.GetName(), "the refusal does not name the input")
		})
	}
}

// TestBindRunInputsAcceptsAConformingValue is the other direction: a value that
// satisfies every declared rule is not refused by any of them.
func TestBindRunInputsAcceptsAConformingValue(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "email", Type: v1.InputDeclaration_TYPE_STRING,
		Pattern: strPtr(`^[^@]+@[^@]+$`), MinLen: u64Ptr(3), MaxLen: u64Ptr(64),
	}
	wf := constrainedWorkflow(decl)

	bound, err := v1.BindRunInputs(wf, map[string]*v1.Value{"email": v1.NewLiteral("a@b.com")})
	require.NoError(t, err)
	assert.NotNil(t, bound["email"])
}

// TestFlowValidateCatchesAStaleLiteralExample is the author-time bite for a
// literal that no longer satisfies its own must:, proven directly against
// CheckInputExample rather than through the compiler, since that is the
// function flow validate and the compiler both call.
func TestCheckInputExampleCatchesAConstraintViolation(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name:    "region",
		Type:    v1.InputDeclaration_TYPE_STRING,
		Pattern: strPtr(`^(us|eu)-`),
		Example: v1.NewLiteral("mars-east-1"),
	}

	err := v1.CheckInputExample(decl)
	require.Error(t, err, "an example violating its own pattern was accepted")
	assert.Contains(t, err.Error(), "example:")
	assert.Contains(t, err.Error(), "must match pattern")
}

// TestCheckInputExampleAcceptsAConformingExample is the acceptance direction:
// an example that does satisfy its own declaration compiles clean.
func TestCheckInputExampleAcceptsAConformingExample(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name:    "region",
		Type:    v1.InputDeclaration_TYPE_STRING,
		Pattern: strPtr(`^(us|eu)-`),
		Example: v1.NewLiteral("us-east-1"),
	}

	assert.NoError(t, v1.CheckInputExample(decl))
}

// TestConstraintShapeRefusesAMismatchedKey is the load-time half of the
// fail-closed rule: a pattern declared on an int is refused when the
// declaration is checked, not left to silently never fire.
func TestConstraintShapeRefusesAMismatchedKey(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "replicas", Type: v1.InputDeclaration_TYPE_INT, Pattern: strPtr("^[0-9]+$")}

	err := v1.CheckInputConstraintShape(decl)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replicas")
	assert.Contains(t, err.Error(), "string input")
}

// TestConstraintShapeRefusesMinItemsAboveTheCap is Codex's first finding on
// #206: maxListElements (10,000) now refuses *every* bound input regardless
// of what it declares, so a declaration whose own min_items sits above that
// cap can never be satisfied — anything over 10,000 elements is refused
// before checkListConstraints ever runs, and anything at or under 10,000
// fails the declared minimum. This is the same class as the neighbouring
// min_items > max_items check: an impossible declaration caught when it
// loads rather than when a run happens to exercise it, so the message names
// both numbers.
func TestConstraintShapeRefusesMinItemsAboveTheCap(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "records", Type: v1.InputDeclaration_TYPE_LIST, MinItems: u64Ptr(10_001)}

	err := v1.CheckInputConstraintShape(decl)
	require.Error(t, err, "an unsatisfiable min_items above the server-wide element cap was accepted")
	assert.Contains(t, err.Error(), "records")
	assert.Contains(t, err.Error(), "10001")
	assert.Contains(t, err.Error(), "10000")
}

// TestConstraintShapeAcceptsAMinItemsAtTheCap is the boundary: exactly
// maxListElements is satisfiable (a list of exactly that many elements binds
// clean, per TestBindRunInputsAcceptsAnUnconstrainedListExactlyAtTheBound),
// so min_items at the cap must not be refused.
func TestConstraintShapeAcceptsAMinItemsAtTheCap(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "records", Type: v1.InputDeclaration_TYPE_LIST, MinItems: u64Ptr(10_000)}

	assert.NoError(t, v1.CheckInputConstraintShape(decl), "min_items exactly at the server-wide cap was refused")
}

// TestConstraintShapeAcceptsASaneMinItems is the ordinary non-regression
// case: a ordinary, small min_items must keep validating.
func TestConstraintShapeAcceptsASaneMinItems(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "records", Type: v1.InputDeclaration_TYPE_LIST, MinItems: u64Ptr(3)}

	assert.NoError(t, v1.CheckInputConstraintShape(decl), "a sane min_items was refused")
}

// TestCheckInputDefaultRefusesAnOversizedLiteral is Codex's second finding on
// #206: BindRunInputs' new element bound was not mirrored by the author-time
// literal validators, so a Flowfile with an oversized literal default: passed
// flow validate and only failed once a run actually started. CheckInputDefault
// now reaches the identical bound (through CheckInputConstraints), because an
// unconstrained list's element count was never walked by CheckInputValue or
// the old CheckInputConstraints on its own.
func TestCheckInputDefaultRefusesAnOversizedLiteral(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "records", Type: v1.InputDeclaration_TYPE_LIST,
		Default: v1.NewLiteralList(manyItems(10_001)...),
	}

	err := v1.CheckInputDefault(decl)
	require.Error(t, err, "a literal default over the element bound was accepted at author time")
	assert.Contains(t, err.Error(), "records")
	assert.Contains(t, err.Error(), "list elements")
}

// TestCheckInputDefaultAcceptsASaneLiteral is the non-regression case: an
// ordinary, small literal default must keep validating.
func TestCheckInputDefaultAcceptsASaneLiteral(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "records", Type: v1.InputDeclaration_TYPE_LIST,
		Default: v1.NewLiteralList("a", "b", "c"),
	}

	assert.NoError(t, v1.CheckInputDefault(decl), "a sane literal default was refused")
}

// TestBindRunInputsRefusesABadDeclarationBeforeAnyValue proves the shape check
// runs at BindRunInputs even for a specification that never passed through
// flow validate — a hand-built Workflow message reaching the server directly.
func TestBindRunInputsRefusesABadDeclarationBeforeAnyValue(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "replicas", Type: v1.InputDeclaration_TYPE_INT, Pattern: strPtr("^[0-9]+$")}
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"replicas": v1.NewLiteral(int64(3))})
	require.Error(t, err, "a mismatched constraint key reached bind time unrefused")
	assert.Contains(t, err.Error(), "string input")
}

// TestMustRefusesNow is the purity requirement: a must: expression reading the
// clock is refused at compile, with a diagnostic naming why, rather than
// evaluated inconsistently across replay.
func TestMustRefusesNow(t *testing.T) {
	t.Parallel()

	_, err := v1.CompileMustExpression(`this == now`, v1.InputDeclaration_TYPE_STRING)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
	assert.Contains(t, err.Error(), "wait_until")
}

// TestMustRefusesAnUnknownName is the general case behind the now-specific one:
// a constraint sees only `this`.
func TestMustRefusesAnUnknownName(t *testing.T) {
	t.Parallel()

	_, err := v1.CompileMustExpression(`this == steps.web.result`, v1.InputDeclaration_TYPE_STRING)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "steps")
}

// TestMustRefusesANonBoolExpression catches a must: that compiles but is not a
// predicate, which would otherwise be silently truthy-tested at the wrong
// moment.
func TestMustRefusesANonBoolExpression(t *testing.T) {
	t.Parallel()

	_, err := v1.CompileMustExpression(`this + 1`, v1.InputDeclaration_TYPE_INT)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bool")
}

// TestMustIsCostBounded proves must: expressions go through the same bounded
// evaluator every other CEL expression in this schema does, rather than a
// bespoke unbounded path — a comprehension over a huge literal list is refused
// rather than run to completion.
func TestMustIsCostBounded(t *testing.T) {
	t.Parallel()

	// Ten elements, six levels of nested `all()` over `this` — the identical
	// blowup shape TestEvaluatorCostLimit already proves trips fast for the
	// base evaluator, adapted to a `must:` over a small literal list rather
	// than a hand-built expression string, so this proves the *wiring*: a
	// `must:` reaches the same bounded [Evaluator.Eval] every other
	// expression in this schema does, not a bespoke unbounded path.
	decl := &v1.InputDeclaration{
		Name: "items",
		Type: v1.InputDeclaration_TYPE_LIST,
		Must: strPtr(`this.all(a, this.all(b, this.all(c, this.all(d, this.all(e, ` +
			`this.all(f, a + b + c + d + e + f >= 0))))))`),
	}
	wf := constrainedWorkflow(decl)

	start := time.Now()
	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"items": v1.NewLiteralList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)})
	elapsed := time.Since(start)

	require.Error(t, err, "an expensive must: expression ran to completion instead of being bounded")
	assert.Contains(t, strings.ToLower(err.Error()), "cost")
	assert.Less(t, elapsed, 5*time.Second, "the cost limit took %v to trip; expected it to fail fast", elapsed)
}

// TestOutputMustCatchesAViolatingAnswer proves the output half of "no
// undeclared name" — a workflow reporting a value that violates its own
// output contract fails the run rather than reporting the bad value.
func TestOutputMustCatchesAViolatingAnswer(t *testing.T) {
	t.Parallel()

	err := v1.CheckOutputConstraint(
		&v1.OutputDeclaration{Name: "tracking", Must: strPtr(`this.matches("^TRK-")`)},
		v1.NewLiteral("not-a-tracking-id"),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tracking")
	assert.Contains(t, err.Error(), "must satisfy")
}

// TestOutputMustAcceptsAConformingAnswer is the other direction.
func TestOutputMustAcceptsAConformingAnswer(t *testing.T) {
	t.Parallel()

	err := v1.CheckOutputConstraint(
		&v1.OutputDeclaration{Name: "tracking", Must: strPtr(`this.matches("^TRK-")`)},
		v1.NewLiteral("TRK-12345"),
	)
	assert.NoError(t, err)
}

// TestMustRefusesAnOversizedList proves the bound found while writing
// TestMustIsCostBounded: the CEL cost limit is abstract cost units tracked
// during evaluation, and does not reliably bound wall-clock time against a
// large enough Go-native list before the budget trips — so list length itself
// is bounded, ahead of ever reaching the evaluator, for a `unique:` or
// `must:` on a `type: list` input.
func TestMustRefusesAnOversizedList(t *testing.T) {
	t.Parallel()

	items := make([]any, 10_001)
	for i := range items {
		items[i] = i
	}

	decl := &v1.InputDeclaration{
		Name: "items", Type: v1.InputDeclaration_TYPE_LIST,
		Must: strPtr(`size(this) >= 0`),
	}
	wf := constrainedWorkflow(decl)

	start := time.Now()
	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"items": v1.NewLiteralList(items...)})
	elapsed := time.Since(start)

	require.Error(t, err, "a list past the element bound reached the evaluator")
	assert.Contains(t, err.Error(), "items")
	assert.Less(t, elapsed, time.Second, "the bound itself must be checked before any expensive work")
}

// manyItems returns a []any of n small ints, for building oversized list
// literals without repeating the loop at every call site.
func manyItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = i
	}
	return items
}

// nestedStruct builds a value nested depth levels deep in single-key maps,
// bottoming out in leaf — the shape a depth-bound test needs, distinct from
// the shape an element-count test needs, since neither bound can see the
// other's dimension.
func nestedStruct(depth int, leaf any) any {
	v := leaf
	for i := 0; i < depth; i++ {
		v = map[string]any{"child": v}
	}
	return v
}

// TestBindRunInputsRefusesAStructWithAnOversizedNestedList is Codex's exact
// finding: checkConstraintListBound used to run only when
// decl.GetType() == TYPE_LIST, so a type: struct input's must: reached an
// arbitrarily large list nested inside it uncounted. Gated on the declared
// type, this value was never bounded at all before BindRunInputs handed it
// to the evaluator.
func TestBindRunInputsRefusesAStructWithAnOversizedNestedList(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "payload", Type: v1.InputDeclaration_TYPE_STRUCT,
		Must: strPtr(`this.items.all(x, x >= 0)`),
	}
	wf := constrainedWorkflow(decl)

	value := v1.NewLiteralMap(map[string]any{"items": manyItems(10_001)})

	start := time.Now()
	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"payload": value})
	elapsed := time.Since(start)

	require.Error(t, err, "a struct whose nested list exceeds the element bound reached the evaluator")
	assert.Contains(t, err.Error(), "payload")
	assert.Contains(t, err.Error(), "list elements")
	assert.Less(t, elapsed, time.Second, "the bound itself must be checked before any expensive work")
}

// TestBindRunInputsRefusesManySmallListsSummingOverTheBound is the direction a
// per-list bound would let through: no single list here is anywhere near
// maxListElements, but the total across the struct is — which is
// exactly the case CLAUDE.md's billion-laughs reasoning says a per-list check
// misses.
func TestBindRunInputsRefusesManySmallListsSummingOverTheBound(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "payload", Type: v1.InputDeclaration_TYPE_STRUCT,
		Must: strPtr(`true`),
	}
	wf := constrainedWorkflow(decl)

	fields := map[string]any{}
	for i := 0; i < 20; i++ {
		fields[fmt.Sprintf("list%d", i)] = manyItems(600) // 20 * 600 = 12,000 > 10,000
	}
	value := v1.NewLiteralMap(fields)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"payload": value})
	require.Error(t, err, "many lists each individually under the bound, summing over it, were accepted")
	assert.Contains(t, err.Error(), "payload")
	assert.Contains(t, err.Error(), "list elements")
}

// TestBindRunInputsRefusesADeeplyNestedStruct is the depth bound, proven
// distinct from the element-count bound: this value never comes close to
// maxListElements, so only nesting depth can be what refuses it,
// and the message must say so rather than repeating the list-elements
// wording.
func TestBindRunInputsRefusesADeeplyNestedStruct(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "payload", Type: v1.InputDeclaration_TYPE_STRUCT,
		Must: strPtr(`true`),
	}
	wf := constrainedWorkflow(decl)

	value := v1.NewLiteralMap(map[string]any{
		"child": nestedStruct(40, "leaf"),
	})

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"payload": value})
	require.Error(t, err, "a value nested past the depth bound was accepted")
	assert.Contains(t, err.Error(), "payload")
	assert.Contains(t, err.Error(), "nests")
	assert.NotContains(t, err.Error(), "list elements",
		"a depth refusal must not be worded as an element-count refusal; they are different resources")
}

// TestBindRunInputsAcceptsAStructJustUnderBothBounds proves the bound is not
// simply refusing everything: a struct nested well under the depth bound,
// carrying well under the element bound's worth of list elements, binds
// clean.
func TestBindRunInputsAcceptsAStructJustUnderBothBounds(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name: "payload", Type: v1.InputDeclaration_TYPE_STRUCT,
		Must: strPtr(`true`),
	}
	wf := constrainedWorkflow(decl)

	// Depth 10, well under the 32-level bound; 9,000 list elements, well
	// under the 10,000 bound.
	value := v1.NewLiteralMap(map[string]any{
		"child": nestedStruct(10, manyItems(9_000)),
	})

	bound, err := v1.BindRunInputs(wf, map[string]*v1.Value{"payload": value})
	require.NoError(t, err, "a struct well under both bounds was refused")
	assert.NotNil(t, bound["payload"])
}

// TestCheckOutputConstraintRefusesAnOversizedNestedList is the output half of
// TestBindRunInputsRefusesAStructWithAnOversizedNestedList: a step can
// produce a large nested list just as a caller can send one, and
// CheckOutputConstraint applies the identical bound.
func TestCheckOutputConstraintRefusesAnOversizedNestedList(t *testing.T) {
	t.Parallel()

	decl := &v1.OutputDeclaration{Name: "result", Must: strPtr(`this.items.all(x, x >= 0)`)}
	value := v1.NewLiteralMap(map[string]any{"items": manyItems(10_001)})

	err := v1.CheckOutputConstraint(decl, value)
	require.Error(t, err, "an output whose nested list exceeds the element bound reached the evaluator")
	assert.Contains(t, err.Error(), "result")
	assert.Contains(t, err.Error(), "list elements")
}

// TestCheckOutputConstraintAcceptsAConformingNestedStruct is the output
// acceptance direction, mirroring the input one.
func TestCheckOutputConstraintAcceptsAConformingNestedStruct(t *testing.T) {
	t.Parallel()

	decl := &v1.OutputDeclaration{Name: "result", Must: strPtr(`true`)}
	value := v1.NewLiteralMap(map[string]any{"child": nestedStruct(10, manyItems(9_000))})

	assert.NoError(t, v1.CheckOutputConstraint(decl, value))
}

// unconstrainedListInput declares a list-typed input carrying no `must:` and
// no `unique:` at all — the exact shape #204 found the constraint-only bound
// left open, since [checkConstraintValueBound] only ran when one of those was
// declared.
func unconstrainedListInput(name string) *v1.InputDeclaration {
	return &v1.InputDeclaration{Name: name, Type: v1.InputDeclaration_TYPE_LIST}
}

// TestBindRunInputsRefusesAnOversizedUnconstrainedList is #204's exact gap: no
// must:, no unique:, nothing that would have routed this value through
// checkConstraintValueBound before this change — and BindRunInputs still
// refuses it, because the bound is now unconditional rather than gated on a
// declaration carrying a constraint.
func TestBindRunInputsRefusesAnOversizedUnconstrainedList(t *testing.T) {
	t.Parallel()

	decl := unconstrainedListInput("items")
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"items": v1.NewLiteralList(manyItems(10_001)...)})
	require.Error(t, err, "an oversized list with no declared constraint was accepted")
	assert.Contains(t, err.Error(), "items")
	assert.Contains(t, err.Error(), "list elements")
}

// TestBindRunInputsAcceptsAnUnconstrainedListExactlyAtTheBound and
// TestBindRunInputsRefusesAnUnconstrainedListOneOverTheBound together prove
// the bound is *reached*, not merely respected (CLAUDE.md: `<= bound` is also
// satisfied by a check that gives up early) — a value of exactly
// maxListElements elements binds clean, and one element more is refused,
// with no must:/unique: declared either way.
func TestBindRunInputsAcceptsAnUnconstrainedListExactlyAtTheBound(t *testing.T) {
	t.Parallel()

	decl := unconstrainedListInput("items")
	wf := constrainedWorkflow(decl)

	bound, err := v1.BindRunInputs(wf, map[string]*v1.Value{"items": v1.NewLiteralList(manyItems(10_000)...)})
	require.NoError(t, err, "a list of exactly the bound's worth of elements was refused")
	assert.NotNil(t, bound["items"])
}

func TestBindRunInputsRefusesAnUnconstrainedListOneOverTheBound(t *testing.T) {
	t.Parallel()

	decl := unconstrainedListInput("items")
	wf := constrainedWorkflow(decl)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"items": v1.NewLiteralList(manyItems(10_001)...)})
	require.Error(t, err, "one element over the bound, with no constraint declared, was accepted")
	assert.Contains(t, err.Error(), "list elements")
}

// TestBindRunInputsRefusesManyUnconstrainedListsSummingOverTheBound is the
// total-across-the-value direction with no must:/unique: anywhere: one
// struct-typed input nesting twenty lists, each individually far under
// maxListElements, whose sum across the whole value exceeds it — the
// unconstrained mirror of TestBindRunInputsRefusesManySmallListsSummingOverTheBound,
// which proves the identical shape but only for a declaration carrying a
// must:.
func TestBindRunInputsRefusesManyUnconstrainedListsSummingOverTheBound(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "payload", Type: v1.InputDeclaration_TYPE_STRUCT}
	wf := constrainedWorkflow(decl)

	fields := map[string]any{}
	for i := 0; i < 20; i++ {
		fields[fmt.Sprintf("list%d", i)] = manyItems(600) // 20 * 600 = 12,000 > 10,000
	}
	value := v1.NewLiteralMap(fields)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"payload": value})
	require.Error(t, err, "many unconstrained lists, each individually under the bound, summing over it, were accepted")
	assert.Contains(t, err.Error(), "payload")
	assert.Contains(t, err.Error(), "list elements")
}

// TestBindRunInputsRefusesAnUnconstrainedStructWithAnOversizedNestedList is
// the nested direction with no must: declared: a type: struct input carrying
// an oversized list nested inside it is refused exactly as the must:-gated
// path already was.
func TestBindRunInputsRefusesAnUnconstrainedStructWithAnOversizedNestedList(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{Name: "payload", Type: v1.InputDeclaration_TYPE_STRUCT}
	wf := constrainedWorkflow(decl)

	value := v1.NewLiteralMap(map[string]any{"items": manyItems(10_001)})

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"payload": value})
	require.Error(t, err, "an unconstrained struct whose nested list exceeds the element bound was accepted")
	assert.Contains(t, err.Error(), "payload")
	assert.Contains(t, err.Error(), "list elements")
}

// TestBindRunInputsAcceptsAnOrdinaryUnconstrainedList is the regression case:
// a normal, reasonably sized list input with no declared constraint must
// still bind and run end to end. The bound exists to stop a caller-chosen
// list from reaching the evaluator unbounded, not to make an everyday
// for_each fanout impossible.
func TestBindRunInputsAcceptsAnOrdinaryUnconstrainedList(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name:           "ordinary-fanout",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{unconstrainedListInput("regions")},
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewExpr("inputs.regions"),
				Iterator: "region",
				Body: []*v1.Node{{
					Id:   "log",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewExpr("region")}}},
				}},
			}},
		}},
	}

	regions := []any{"us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"}

	bound, err := v1.BindRunInputs(wf, map[string]*v1.Value{"regions": v1.NewLiteralList(regions...)})
	require.NoError(t, err, "an ordinary, reasonably sized fanout list was refused")
	assert.NotNil(t, bound["regions"])

	out, err := v1.RunWithInputs(t.Context(), wf, map[string]*v1.Value{"regions": v1.NewLiteralList(regions...)})
	require.NoError(t, err, "an ordinary fanout workflow did not run end to end")
	results := out.GetStepValues()["each"].GetNamedValues()["results"].GetLiteral().GetListValue().GetValues()
	assert.Len(t, results, len(regions), "the loop did not run once per region")
}

func strPtr(s string) *string   { return &s }
func u64Ptr(u uint64) *uint64   { return &u }
func f64Ptr(f float64) *float64 { return &f }
