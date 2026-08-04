package flowstatev1_test

import (
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

func strPtr(s string) *string   { return &s }
func u64Ptr(u uint64) *uint64   { return &u }
func f64Ptr(f float64) *float64 { return &f }
