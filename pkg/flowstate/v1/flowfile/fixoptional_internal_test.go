package flowfile

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		{"source that does not parse", `has(a.b) &&`},
		{"has of a bare identifier does not parse", `has(a) && a`},
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
