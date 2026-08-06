package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The negation-drift lint (issue #207, decision comment, staged plan step 1)
// exists because a workflow has no way to name "the gate passed" once, so it
// writes "the gate did not pass" as a hand-written negation instead — and
// nothing checks the two stay in sync. See negation.go for what this catches
// and, at greater length, for why it stays silent everywhere it is not certain.
//
// Per CLAUDE.md's "false diagnostics are worse than missing ones," most of
// these cases assert silence. The one case that must be caught is a pair that
// was an exact negation and drifted by one clause; everything else — an exact
// negation, an unrelated pair, a lone `if:`, a shared condition with no
// negation in sight, a three-way partition of one condition's complement — must
// stay quiet.
func TestNegationDrift(t *testing.T) {
	tests := []struct {
		name string
		src  string
		// want, when non-empty, is a substring every reported negation-drift
		// diagnostic must contain; empty means none may be reported.
		want []string
	}{
		{
			name: "exact negation, bare !",
			src: gateSteps(
				`${steps.gate.value}`,
				`${!steps.gate.value}`,
			),
		},
		{
			name: "exact negation, parenthesized",
			src: gateSteps(
				`${steps.gate.a == "x" && steps.gate.b == "y"}`,
				`${!(steps.gate.a == "x" && steps.gate.b == "y")}`,
			),
		},
		{
			// The shape examples/approval-gate/workflow.yaml actually ships: a
			// shared guard (`has(...) && steps.gate.value`), then the tail
			// negated as one parenthesized clause. Healthy, and it is exactly
			// this shape that a prefix-blind comparison would misread as
			// unrelated (different total clause count) or as drifted (the
			// guard clauses would look unmatched).
			name: "exact negation behind a shared guard",
			src: gateSteps(
				`${has(steps.gate.payload) && steps.gate.value &&
					steps.gate.sender.subject == "alice" && steps.gate.sender.issuer == "idp"}`,
				`${has(steps.gate.payload) && steps.gate.value &&
					!(steps.gate.sender.subject == "alice" && steps.gate.sender.issuer == "idp")}`,
			),
		},
		{
			name: "drifted: one clause edited on the negated side",
			src: gateSteps(
				`${steps.gate.sender.subject == "alice"}`,
				`${!(steps.gate.sender.subject == "bob")}`,
			),
			want: []string{
				`exact negations`, `"alice"`, `"bob"`,
			},
		},
		{
			// The same drift, but behind the shared guard the healthy case
			// above proved this lint can see past. A false negative here would
			// mean the guard-stripping that makes the healthy case quiet also
			// hides real drift, which would be worse than not stripping at all.
			name: "drifted behind a shared guard",
			src: gateSteps(
				`${has(steps.gate.payload) && steps.gate.value &&
					steps.gate.sender.subject == "alice" && steps.gate.sender.issuer == "idp"}`,
				`${has(steps.gate.payload) && steps.gate.value &&
					!(steps.gate.sender.subject == "alice" && steps.gate.sender.issuer == "other-idp")}`,
			),
			want: []string{
				`exact negations`, `"idp"`, `"other-idp"`,
			},
		},
		{
			name: "unrelated conditions",
			src: gateSteps(
				`${steps.gate.a == "x"}`,
				`${steps.gate.b == "y"}`,
			),
		},
		{
			// examples/enterprise-fund-transfer/workflow.yaml's shape: several
			// steps read the identical condition verbatim. Real repetition —
			// the issue names it — but there is no negation here to drift
			// apart from, so this lint has nothing to say about it.
			name: "identical conditions, no negation",
			src: gateSteps(
				`${steps.gate.value}`,
				`${steps.gate.value}`,
			),
		},
		{
			// examples/enterprise-fund-transfer/workflow.yaml's other shape:
			// `rejected`/`expired`/`refused_unauthorized` each state one slice
			// of a complement rather than negate the whole condition. None of
			// the three, alone, is a single `!(...)` whose flattened length
			// matches the guarded condition's, so this must stay silent on
			// each pairing.
			name: "three-way partition of a complement",
			src: `
edition: v2026.2
name: partition
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: cleared
    if: >-
      ${steps.gate.amount < 100 ||
        (has(steps.gate.payload) && steps.gate.value && steps.gate.sender.subject == "alice")}
    log:
      message: cleared
  - id: rejected
    if: >-
      ${steps.gate.amount >= 100 && has(steps.gate.payload) && !steps.gate.value}
    log:
      message: rejected
  - id: expired
    if: ${steps.gate.amount >= 100 && steps.gate.timed_out}
    log:
      message: expired
  - id: refused
    if: >-
      ${steps.gate.amount >= 100 && has(steps.gate.payload) && steps.gate.value &&
        !(steps.gate.sender.subject == "alice")}
    log:
      message: refused
`,
		},
		{
			name: "lone if, no sibling condition to compare",
			src: `
edition: v2026.2
name: lone
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: a
    if: ${steps.gate.value}
    log:
      message: hi
`,
		},
		{
			// Different clause counts on the two sides: the negated side has
			// four clauses and the other side has one. Guessing which clause
			// "corresponds" would be inventing a relationship the file never
			// stated, so this must stay silent.
			name: "clause counts do not match",
			src: gateSteps(
				`${steps.gate.value}`,
				`${!(steps.gate.a == "x" && steps.gate.b == "y" && steps.gate.c == "z" && steps.gate.d == "w")}`,
			),
		},
		{
			// Two clauses differ, not one. A single "closest differing clause"
			// guess would be arbitrary, so this stays silent too.
			name: "more than one clause differs",
			src: gateSteps(
				`${steps.gate.a == "x" && steps.gate.b == "y"}`,
				`${!(steps.gate.a == "p" && steps.gate.b == "q")}`,
			),
		},
		{
			// Nested inside a for_each body: siblings there are compared the
			// same way, and a step outside the body is not compared against
			// them at all.
			name: "drift inside a for_each body",
			src: `
edition: v2026.2
name: nested
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: items
    for_each:
      items: ${[1, 2]}
      as: item
      steps:
        - id: yes_branch
          if: ${steps.gate.sender.subject == "alice"}
          log:
            message: hi
        - id: no_branch
          if: ${!(steps.gate.sender.subject == "bob")}
          log:
            message: hi
`,
			want: []string{
				`exact negations`, `"alice"`, `"bob"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := negationDiagnostics(t, tt.src)

			if len(tt.want) == 0 {
				assert.Empty(t, got, "expected no negation-drift diagnostic")
				return
			}

			require.NotEmpty(t, got, "expected a negation-drift diagnostic and got none")
			for _, msg := range got {
				for _, want := range tt.want {
					assert.Contains(t, msg, want)
				}
			}
		})
	}
}

// gateSteps builds a minimal Flowfile with two sibling steps, "a" and "b",
// carrying the given `if:` expressions, following a step named "gate" both
// conditions may reference.
func gateSteps(ifA, ifB string) string {
	return strings.Join([]string{
		"edition: v2026.2",
		"name: gated",
		"steps:",
		"  - id: gate",
		"    wait_for_signal:",
		"      name: go",
		"      timeout: 1h",
		"  - id: a",
		"    if: " + strings.ReplaceAll(ifA, "\n", "\n      "),
		"    log:",
		"      message: yes",
		"  - id: b",
		"    if: " + strings.ReplaceAll(ifB, "\n", "\n      "),
		"    log:",
		"      message: no",
		"",
	}, "\n")
}

// negationDiagnostics validates src and returns the rendered diagnostics that
// came from the negation-drift lint specifically, distinguished from anything
// else Validate might also say about the same file by the sentence this lint
// always uses.
func negationDiagnostics(t *testing.T, src string) []string {
	t.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err, "the file does not compile, so this says nothing about the lint")

	var out []string
	for _, d := range ds {
		if strings.Contains(d.Error(), "exact negations of each other") {
			out = append(out, d.Error())
		}
	}
	return out
}

// TestNegationDriftReportsBothPositions pins that a drifted pair is reported
// at both steps' own `if:` — one diagnostic per side, each positioned at that
// step's own line, so an editor can place a squiggle under either clause
// without having to infer the other step's location from the message text.
func TestNegationDriftReportsBothPositions(t *testing.T) {
	src := `edition: v2026.2
name: positions
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: a
    if: ${steps.gate.sender.subject == "alice"}
    log:
      message: yes
  - id: b
    if: ${!(steps.gate.sender.subject == "bob")}
    log:
      message: no
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	var found []flowfile.Diagnostic
	for _, d := range ds {
		if strings.Contains(d.Message, "exact negations of each other") {
			found = append(found, d)
		}
	}
	require.Len(t, found, 2, "expected one diagnostic per side of the drifted pair:\n%s", ds.Error())

	byStep := map[string]flowfile.Diagnostic{}
	for _, d := range found {
		byStep[d.Step] = d
	}

	require.Contains(t, byStep, "a")
	require.Contains(t, byStep, "b")

	// "if:" on step "a" is line 9; on step "b" it is line 13.
	assert.Equal(t, 9, byStep["a"].Line, "step a's diagnostic should point at step a's own if:")
	assert.Equal(t, 13, byStep["b"].Line, "step b's diagnostic should point at step b's own if:")

	assert.Contains(t, byStep["a"].Message, `"b"`, "the diagnostic on a names the other step, b")
	assert.Contains(t, byStep["b"].Message, `"a"`, "the diagnostic on b names the other step, a")
}

// TestNegationDriftIsQuietOnTheCorpus is the measurement this lint was built
// against: examples/approval-gate/ and examples/enterprise-fund-transfer/ are
// exactly the files issue #207 named as writing a gate and its hand-negated
// complement, repeatedly — real load-bearing negation pairs the lint must not
// flag, alongside every other shipped example.
func TestNegationDriftIsQuietOnTheCorpus(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(path)
			require.NoError(t, err)

			ds, err := flowfile.ValidateSourceAt(data, path)
			require.NoError(t, err)

			for _, d := range ds {
				assert.NotContains(t, d.Message, "exact negations of each other",
					"a shipped example was flagged by the negation-drift lint, and it runs in CI, "+
						"so either the example genuinely drifted (fix the example) or the lint is "+
						"reporting a false positive (fix the lint): %s", d.Error())
			}
		})
	}
}
