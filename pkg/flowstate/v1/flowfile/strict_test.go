package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The Flowfile grammar is a strict subset of YAML: anchors, aliases, and merge
// keys are refused. These cover the three refusals in both directions — the
// construct is rejected with a positioned diagnostic, and the equivalent value
// spelled out is accepted — plus the billion-laughs shape the refusal exists to
// stop, which must be refused without ever expanding. See #653 and strict.go.

// firstDiagnostic pulls the earliest-positioned diagnostic out of a parse error,
// so a test can assert the exact line, column, and text a construct is refused
// at rather than only that something failed.
func firstDiagnostic(t *testing.T, err error) flowfile.Diagnostic {
	t.Helper()
	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	require.NotEmpty(t, ds)
	return ds[0]
}

func TestStrictYAMLRefusesAnchor(t *testing.T) {
	t.Parallel()

	// The anchor sits on the first step. `&shared` is at column 5, under the
	// two-space list indent and the `- ` marker.
	src := `edition: v2026.3
name: t
steps:
  - &shared
    id: a
    log:
      message: hi
`
	_, _, err := flowfile.Parse([]byte(src))
	d := firstDiagnostic(t, err)
	assert.Equal(t, 4, d.Line)
	assert.Equal(t, 5, d.Column)
	assert.Contains(t, d.Message, "an anchor (`&shared`) is not part of the Flowfile grammar")
	assert.Contains(t, d.Message, "write the value out", "the diagnostic must say what to do instead")
}

func TestStrictYAMLRefusesAlias(t *testing.T) {
	t.Parallel()

	// `*base` is the value of `message:`, at column 16.
	src := `edition: v2026.3
name: t
vars:
  base: hi
steps:
  - id: a
    log:
      message: *base
`
	_, _, err := flowfile.Parse([]byte(src))
	d := firstDiagnostic(t, err)
	assert.Equal(t, 8, d.Line)
	assert.Equal(t, 16, d.Column)
	assert.Contains(t, d.Message, "an alias (`*base`) is not part of the Flowfile grammar")
	assert.Contains(t, d.Message, "write the value out here", "the diagnostic must say what to do instead")
}

func TestStrictYAMLRefusesMergeKey(t *testing.T) {
	t.Parallel()

	// The `<<:` is on the second step, at column 5.
	src := `edition: v2026.3
name: t
steps:
  - &policy
    id: a
    timeout: 30s
    log:
      message: one
  - id: b
    <<: *policy
    log:
      message: two
`
	_, _, err := flowfile.Parse([]byte(src))
	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)

	// All three constructs in this file are named, so an author is not sent back
	// one recompile at a time.
	joined := ds.Error()
	assert.Contains(t, joined, "an anchor (`&policy`)")
	assert.Contains(t, joined, "an alias (`*policy`)")

	// The merge key itself, positioned.
	var merge *flowfile.Diagnostic
	for i := range ds {
		if strings.Contains(ds[i].Message, "a merge key (`<<:`)") {
			merge = &ds[i]
			break
		}
	}
	require.NotNil(t, merge, "the merge key must be reported")
	assert.Equal(t, 10, merge.Line)
	assert.Equal(t, 5, merge.Column)
	assert.Contains(t, merge.Message, "write each key it would merge in directly")
}

// TestStrictYAMLAcceptsTheSpelledOutEquivalent is the other direction: the value
// a merge key would have shared, written directly on each step, compiles and
// means what the merge would have meant.
func TestStrictYAMLAcceptsTheSpelledOutEquivalent(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: shared
steps:
  - id: a
    timeout: 30s
    continue_on_error: true
    log:
      message: one
  - id: b
    timeout: 30s
    continue_on_error: true
    log:
      message: two
`
	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Len(t, wf.GetSteps(), 2)
	for _, step := range wf.GetSteps() {
		assert.True(t, step.GetPolicy().GetContinueOnError(), "step %q", step.GetId())
	}
}

// TestStrictYAMLRefusesBillionLaughsWithoutExpanding is the security property.
//
// A billion-laughs document has depth one per alias and multiplies breadth at
// every level, so a bound that expanded first would allocate an enormous tree
// before it could refuse. The refusal reads only the nodes the author wrote — a
// handful of anchors and aliases — and never follows one, so the document is
// rejected in the time it takes to walk what is on disk. Run under a tight
// memory limit by `go test`, this proves the refusal precedes expansion: were an
// alias ever followed, the nested lists would exhaust the budget rather than
// return a diagnostic.
func TestStrictYAMLRefusesBillionLaughsWithoutExpanding(t *testing.T) {
	t.Parallel()

	// Nine levels, nine references each: were this expanded it would be 9^9 ≈ 387
	// million leaf nodes. It is a few hundred bytes on disk.
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: boom\n")
	b.WriteString("l0: &l0 \"lol\"\n")
	for i := 1; i <= 9; i++ {
		b.WriteString("l" + strconv.Itoa(i) + ": &l" + strconv.Itoa(i) + " [")
		for j := 0; j < 9; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString("*l" + strconv.Itoa(i-1))
		}
		b.WriteString("]\n")
	}
	b.WriteString("steps:\n  - id: s\n    log:\n      message: hi\n")

	src := []byte(b.String())
	require.Less(t, len(src), 4096, "premise: the bomb is tiny on disk; only expansion makes it large")

	_, _, err := flowfile.Parse(src)
	var ds flowfile.Diagnostics
	require.ErrorAs(t, err, &ds)
	// Reached the refusal, not some unrelated failure: the anchors and aliases
	// themselves are what is named.
	assert.Contains(t, ds.Error(), "not part of the Flowfile grammar")
}

// TestFixRefusesStrictYAML is the fixer's half: a file with an anchor is refused
// rather than rewritten, in the same words, so `flow fix` never emits a file the
// compiler then rejects. Mechanical inlining is a follow-up (#653).
func TestFixRefusesStrictYAML(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - &shared
    id: a
    log:
      message: hi
`
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, result.Refusals, "an anchor must be refused, not silently passed through")
	assert.False(t, result.Complete())
	assert.Equal(t, src, string(result.Source), "a refused file is left byte for byte alone")
	assert.Contains(t, result.Refusals[0].Message, "an anchor (`&shared`) is not part of the Flowfile grammar")
}
