package flowfile

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestRewriteOptionalReads pins the three decided shapes (issue #412) byte for
// byte, because "the output still validates" is exactly the assertion CLAUDE.md
// records letting two rewriter corruptions through.
func TestRewriteOptionalReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "guarded read",
			in:   `has(a.b) && a.b`,
			want: `a.?b.orValue(false)`,
		},
		{
			name: "guarded read at depth",
			in:   `has(steps.approval.payload.approved) && steps.approval.payload.approved`,
			want: `steps.approval.payload.?approved.orValue(false)`,
		},
		{
			name: "hand-negated twin keeps its negation",
			in:   `!(has(a.b) && a.b)`,
			want: `!a.?b.orValue(false)`,
		},
		{
			name: "ternary default",
			in:   `has(r.last_used.days) ? r.last_used.days : -1`,
			want: `r.last_used.?days.orValue(-1)`,
		},
		{
			name: "ternary default with a string default",
			in:   `has(a.b) ? a.b : "none"`,
			want: `a.?b.orValue("none")`,
		},
		{
			name: "ternary default whose default is itself a ternary",
			in:   `has(a.b) ? a.b : x ? y : z`,
			want: `a.?b.orValue(x ? y : z)`,
		},
		{
			name: "guarded read as a ternary condition",
			in:   `has(payload.approved) && payload.approved ? "signed_off" : "unsigned"`,
			want: `payload.?approved.orValue(false) ? "signed_off" : "unsigned"`,
		},
		{
			name: "guarded read inside a disjunction",
			in:   `x || has(a.b) && a.b`,
			want: `x || a.?b.orValue(false)`,
		},
		{
			name: "guarded read inside a macro body",
			in:   `xs.filter(r, has(r.probe.ok) && r.probe.ok)`,
			want: `xs.filter(r, r.probe.?ok.orValue(false))`,
		},
		{
			name: "two sites in one expression",
			in:   `has(a.b) && a.b && has(c.d) && c.d`,
			want: `a.?b.orValue(false) && c.?d.orValue(false)`,
		},
		{
			name: "negated twin beside a plain site",
			in:   `!(has(a.b) && a.b) || has(c.d) && c.d`,
			want: `!a.?b.orValue(false) || c.?d.orValue(false)`,
		},

		// Operand-boundary positives: each match sits beside an operator, and
		// what decides the rewrite is that the operator binds *looser* than the
		// match's own structure, so the replacement is the same subtree the
		// original was. The parse-tree comparison proves that per site; these
		// pin that it keeps proving it.
		{
			name: "guarded read before a disjunction",
			in:   `has(a.b) && a.b || z`,
			want: `a.?b.orValue(false) || z`,
		},
		{
			name: "guarded read before a conjunction tail",
			in:   `has(a.b) && a.b && z`,
			want: `a.?b.orValue(false) && z`,
		},
		{
			name: "guarded read after a conjunction head",
			in:   `z && has(a.b) && a.b`,
			want: `z && a.?b.orValue(false)`,
		},
		{
			name: "parenthesised guarded read keeps its parentheses",
			in:   `(has(a.b) && a.b) && z`,
			want: `(a.?b.orValue(false)) && z`,
		},
		{
			name: "negated twin before a disjunction",
			in:   `!(has(a.b) && a.b) || z`,
			want: `!a.?b.orValue(false) || z`,
		},
		{
			name: "negated twin compared: the negation binds tighter than ==",
			in:   `!(has(a.b) && a.b) == z`,
			want: `!a.?b.orValue(false) == z`,
		},
		{
			name: "twin reached through a select: the inner conjunction is whole",
			in:   `!(has(a.b) && a.b).size()`,
			want: `!(a.?b.orValue(false)).size()`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, changed := rewriteOptionalReads(tt.in)
			assert.True(t, changed, "the idiom must be rewritten")
			assert.Equal(t, tt.want, got)

			// The rewrite is a fixed point: its own output holds no idiom.
			again, changedAgain := rewriteOptionalReads(got)
			assert.False(t, changedAgain)
			assert.Equal(t, got, again)
		})
	}
}

// TestRewriteOptionalReadsLeavesNearMissesAlone is the mutation half: every
// case is one edit away from a shape the rewriter acts on, and each is the
// difference between a rewrite and a corruption.
func TestRewriteOptionalReadsLeavesNearMissesAlone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
	}{
		{"presence alone stays has()", `has(a.b)`},
		{"negated presence stays", `!has(a.b)`},
		{"paths differ", `has(a.b) && a.c`},
		{"guard negated, read bare: not the twin", `!has(a.b) && a.b`},
		{"read negated: asks answered-no, not not-answered-yes", `has(a.b) && !a.b`},
		{"read goes deeper than the guard", `has(a.b) && a.b.c`},
		{"guard goes deeper than the read", `has(a.b.c) && a.b`},
		{"read is a longer identifier", `has(a.b) && a.bc`},
		{"read is a call", `has(a.b) && a.b(1)`},
		{"read is indexed", `has(a.b) && a.b[0] == 1`},
		{"the idiom inside a string literal is prose", `"has(a.b) && a.b"`},
		{"ternary with differing paths", `has(a.b) ? a.c : d`},
		{"ternary not the whole expression", `(has(a.b) ? a.b : d) && x`},
		{"ternary arms swapped", `has(a.b) ? d : a.b`},
		{"ternary whose true arm compares", `has(a.b) ? a.b == x : d`},
		{"source that does not parse", `has(a.b) &&`},
		{"has of a bare identifier does not parse", `has(a) && a`},

		// The operand-boundary class (PR #483's P1). In each of these the
		// substring `has(P) && P` is not a node of the parse tree: an operator
		// binding tighter than `&&` extends one of its operands, so the textual
		// match is a fragment, and rewriting it would reverse meaning —
		// `has(a.b) && a.b == false` with `b` absent is false, and the corrupted
		// rewrite `a.?b.orValue(false) == false` is true (see
		// TestOperandBoundaryReversalIsReal). Spaced and unspaced spellings are
		// both here because the adjacent-byte screen this class defeated saw
		// only the touching neighbour; reverting the parse-tree comparison to
		// that screen fails the spaced cases.
		{"read compared, spaced (the reported reversal)", `has(a.b) && a.b == false`},
		{"read compared, unspaced", `has(a.b) && a.b==false`},
		{"read compared with not-equals", `has(a.b) && a.b != x`},
		{"read ordered", `has(a.b) && a.b < 5`},
		{"read membership-tested", `has(a.b) && a.b in xs`},
		{"read summed into a comparison", `has(a.b) && a.b + 1 > 0`},
		{"read selected past a space", `has(a.b) && a.b .size() > 0`},
		{"guard on a comparison's right side", `x == has(a.b) && a.b`},
		{"guard on a comparison's right side, unspaced", `x==has(a.b) && a.b`},
		{"guard under a membership test", `x in has(a.b) && a.b`},
		{"guard under a unary minus", `-has(a.b) && a.b`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, changed := rewriteOptionalReads(tt.in)
			assert.False(t, changed, "a near miss must be left alone")
			assert.Equal(t, tt.in, got, "left alone means byte for byte")
		})
	}
}

// TestRewriteOptionalReadsPreservesMeaning is the semantic half of the byte
// comparisons above: whatever this rewriter does to an expression — rewrite it
// or leave it — the result must evaluate to what the original evaluates to,
// with the guarded field absent, present-and-true, and present-and-false.
//
// It exists because of the operand-boundary reversal (PR #483's P1): the
// corrupted rewrite of `has(a.b) && a.b == false` still parses, still
// validates, and answers *true* where the original answers *false* on an
// absent field — exactly the "the file simply computes something else" failure
// CLAUDE.md's rewriter section documents. A byte comparison catches a wrong
// splice; only evaluation catches a wrong meaning, so both run.
func TestRewriteOptionalReadsPreservesMeaning(t *testing.T) {
	t.Parallel()

	exprs := []string{
		// Shapes the rewriter modernises, including every operator-adjoined
		// position the byte tables above accept.
		`has(a.b) && a.b`,
		`!(has(a.b) && a.b)`,
		`has(a.b) ? a.b : -1`,
		`has(a.b) ? a.b : x ? 1 : 0`,
		`has(a.b) ? a.b : x == z`,
		`has(a.b) && a.b ? "y" : "n"`,
		`x || has(a.b) && a.b`,
		`has(a.b) && a.b || x`,
		`has(a.b) && a.b && z`,
		`z && has(a.b) && a.b`,
		`!(has(a.b) && a.b) || x`,
		`!(has(a.b) && a.b) && z`,
		`!(has(a.b) && a.b) == x`,
		`x == !(has(a.b) && a.b)`,
		`rs.filter(r, has(r.b) && r.b)`,
		// Shapes the rewriter must refuse — the operand-boundary class.
		// Evaluating them too proves "left alone" and "means the same" are one
		// claim, and that a refusal is never the lesser corruption.
		`has(a.b) && a.b == false`,
		`has(a.b) && a.b==false`,
		`has(a.b) && a.b != x`,
		`has(a.b) && a.b in xs`,
		`x == has(a.b) && a.b`,
		`has(a.b) && !a.b`,
		`!has(a.b) && a.b`,
		`has(a.b) ? a.b == x : true`,
	}

	activations := map[string]map[string]any{
		"absent":        {"a": map[string]any{}},
		"present true":  {"a": map[string]any{"b": true}},
		"present false": {"a": map[string]any{"b": false}},
	}
	for _, activation := range activations {
		activation["x"] = false
		activation["z"] = true
		activation["xs"] = []any{true, false}
		activation["rs"] = []any{
			map[string]any{"b": true},
			map[string]any{},
			map[string]any{"b": false},
		}
	}

	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()

			rewritten, _ := rewriteOptionalReads(expr)
			for name, activation := range activations {
				want, wantErr := evalForProof(t, expr, activation)
				got, gotErr := evalForProof(t, rewritten, activation)
				if wantErr != gotErr {
					t.Fatalf("%s: original errored=%v, result errored=%v — the rewrite changed evaluability", name, wantErr, gotErr)
				}
				if !wantErr {
					assert.Equal(t, want, got,
						"%s: %q evaluates to %v but its result %q evaluates to %v", name, expr, want, rewritten, got)
				}
			}
		})
	}
}

// TestOperandBoundaryReversalIsReal documents why the boundary class refuses:
// the corrupted rewrite of the reported case is not merely different bytes, it
// is the opposite gate. If this test ever fails, the two spellings have become
// equivalent and the refusal is only costing modernisations — worth knowing,
// because today it is preventing corruption.
func TestOperandBoundaryReversalIsReal(t *testing.T) {
	t.Parallel()

	absent := map[string]any{"a": map[string]any{}}

	original, err1 := evalForProof(t, `has(a.b) && a.b == false`, absent)
	corrupted, err2 := evalForProof(t, `a.?b.orValue(false) == false`, absent)
	if err1 || err2 {
		t.Fatal("both spellings must evaluate for the comparison to mean anything")
	}
	assert.Equal(t, false, original, "the guard short-circuits on an absent field")
	assert.Equal(t, true, corrupted, "the corrupted rewrite opens the gate the guard closed")
}

// evalForProof evaluates one expression in the profile's environment and
// reports the native value, or that evaluation errored.
func evalForProof(t *testing.T, src string, activation map[string]any) (any, bool) {
	t.Helper()

	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	if err != nil {
		t.Fatal(err)
	}
	out, err := v1.DefaultEvaluator().EvalString(t.Context(), src, libs, activation)
	if err != nil {
		return nil, true
	}
	return out.Value(), false
}

// TestMaskCELLiterals pins the property the matcher depends on: same length,
// literal contents blanked, code untouched.
func TestMaskCELLiterals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{`a + "has(x.y)" + b`, `a + "        " + b`},
		{`a + 'has(x.y)' + b`, `a + '        ' + b`},
		{`"a \" b" + c`, `"      " + c`},
		{`r"raw \ has(a.b)" + x`, `r"              " + x`},
		{`has(a.b) && a.b`, `has(a.b) && a.b`},
	}
	for _, tt := range tests {
		got := maskCELLiterals(tt.in)
		assert.Equal(t, tt.want, got)
		assert.Len(t, got, len(tt.in), "masking must preserve offsets")
	}
}
