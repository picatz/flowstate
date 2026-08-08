package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A wait's `outputs:` shaping expressions are reference-checked like every other
// expression position, but until #318 a diagnostic about one landed on `- id:` —
// the step-wide fallback — rather than on the expression at fault. The message
// was right, including the `outputs.<name>` path; only the position was coarse.
// These tests pin the position the way the #313-era tests pin the loop keys':
// exact line and column, asserted against the value the author wrote.

// unknownBareNameHelp is the tail every unresolved-bare-name diagnostic carries.
// Asserted byte for byte because the fix moves the position and must not touch
// the message.
const unknownBareNameHelp = "; a bare name is a loop's iterator, a name this step declares in its own `vars:`, or `now`, and a step output is written `steps.<id>.<output>`"

// TestWaitShapingDiagnosticLandsOnTheExpression is the #318 reproduction. The
// error sits in the LAST of three entries, because a first-entry fixture can pass
// by accident: the step's own fallback span and the first entry's line are close
// enough for a wrong mechanism to look right.
func TestWaitShapingDiagnosticLandsOnTheExpression(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: waitshape
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
      outputs:
        first: ${payload.a}
        second: ${sender}
        ok: ${p}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one bad reference is one diagnostic:\n%s", ds.Error())

	d := ds[0]
	assert.Equal(t, "gate", d.Step)
	assert.Equal(t, "outputs.ok", d.Field)
	assert.Equal(t, `references unknown name "p"`+unknownBareNameHelp, d.Message,
		"only the position may move; the message is pinned byte for byte")

	// The `${p}` the author wrote: line 11, column 13. Not line 4, which is
	// `- id: gate` — the step-wide fallback this test exists to forbid.
	if d.Line != 11 || d.Column != 13 {
		t.Errorf("position = %d:%d, want 11:13 (the ${p} expression)\nreported: %s",
			d.Line, d.Column, d.Error())
	}
}

// TestWaitShapingDiagnosticMidExpressionPointsAtTheValue pins how precise the
// position is when the bad reference is not the first token: the value's own
// start, exactly as a loop key's is. The position model records the span of the
// value, not of each reference inside it, so claiming more would overclaim —
// what matters is that the position is the offending entry's, not the step's.
func TestWaitShapingDiagnosticMidExpressionPointsAtTheValue(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: waitshape
steps:
  - id: gate
    wait_for_signal:
      name: go
      outputs:
        first: ${payload.a}
        ok: ${sender != '' && nope}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one bad reference is one diagnostic:\n%s", ds.Error())

	d := ds[0]
	assert.Equal(t, "gate", d.Step)
	assert.Equal(t, "outputs.ok", d.Field)

	// Column 13 is the `$` opening the entry's value on line 9 — the same
	// precision an unresolved name in a loop key or a `wait_until:` gets.
	if d.Line != 9 || d.Column != 13 {
		t.Errorf("position = %d:%d, want 9:13 (the entry's value)\nreported: %s",
			d.Line, d.Column, d.Error())
	}
}

// TestWaitTimeoutExpressionDiagnosticLandsOnTheExpression covers the sibling the
// shaping fix uncovered: a signal's computed `timeout:` was reference-checked
// with the same coarse fallback, because its compile path recorded no value span
// at all. `sleep:` and `wait_until:` were already exact — they are step-level
// keys, recorded when the kind is, so [flowfile.Positions.Locate] finds them
// without a wait-specific candidate.
func TestWaitTimeoutExpressionDiagnosticLandsOnTheExpression(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: waitshape
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: ${q}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one bad reference is one diagnostic:\n%s", ds.Error())

	d := ds[0]
	assert.Equal(t, "gate", d.Step)
	assert.Equal(t, "wait_for_signal.timeout", d.Field)
	assert.Equal(t, `references unknown name "q"`+unknownBareNameHelp, d.Message)

	// The `${q}` on line 7, column 16 — not `- id: gate` at 4:5.
	if d.Line != 7 || d.Column != 16 {
		t.Errorf("position = %d:%d, want 7:16 (the ${q} expression)\nreported: %s",
			d.Line, d.Column, d.Error())
	}
}

// TestWaitShapingValidBlockIsClean is the control: the fixture above with the
// bad reference corrected produces nothing, so the precise position cannot be a
// new diagnostic firing on legal files.
func TestWaitShapingValidBlockIsClean(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: waitshape
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
      outputs:
        first: ${payload.a}
        second: ${sender}
        ok: ${!timed_out}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "a legal shaping block must stay clean:\n%s", ds.Error())
}
