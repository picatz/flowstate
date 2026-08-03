package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Diagnostics are a feature, and a compensation is the block where a wrong file is
// most expensive: a saga written incorrectly is a workload that believes it is
// transactional and is not, and the way an author finds out is a run that failed
// and left half a world behind. So each of the ways `undo:` can be written wrong is
// answered by name and at a position, and each one is pinned here.
//
// Written against the source rather than the compiled workflow, because the
// position is half the diagnostic — a message that says the compensation is in the
// wrong place and does not say *where* has moved the search rather than ended it.

// TestUndoCompiles pins that the shape an author writes is the shape that arrives.
func TestUndoCompiles(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: make
    log:
      message: made
    undo:
      log:
        message: unmade
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	undo := wf.GetSteps()[0].GetUndo()
	require.NotNil(t, undo, "`undo:` compiled to nothing")
	assert.Equal(t, "log", undo.GetTask().GetName())
	assert.Equal(t, "unmade", undo.GetTask().GetInputs()["message"].GetLiteral().GetStringValue())

	// Marshal is the inverse, which is what keeps `flow fix` and `flow fmt` from
	// dropping a compensation on their way through a file they were only asked to
	// reformat.
	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Contains(t, string(out), "undo:")

	back, err := flowfile.Unmarshal(out)
	require.NoError(t, err)
	assert.Equal(t, "log", back.GetSteps()[0].GetUndo().GetTask().GetName())
}

// TestUndoReadsItsOwnStepsOutputs is the reference that resolves nowhere else.
//
// A step naming itself is a forward reference everywhere in a Flowfile and is
// refused as one. Inside its own `undo:` it is the ordinary case, and it is the
// reference a compensation almost always needs — the thing to delete is named by
// the step that created it. So the validator has to model what the engine does
// rather than apply the general rule, and this is the pin on that.
func TestUndoReadsItsOwnStepsOutputs(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: make
    http:
      url: https://example.com/create
      outputs: '${ {"id": response.body} }'
    undo:
      log:
        message: ${steps.make.id}
`

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "a compensation reading its own step's output was reported as a bad reference")
}

// TestUndoDiagnostics covers every way the block can be written wrong.
func TestUndoDiagnostics(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		line int
		want string
	}{
		{
			name: "nothing under it",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
`,
			line: 7,
			want: "must name the task that takes this step back",
		},
		{
			name: "two tasks",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        message: one
      http:
        url: https://example.com
`,
			line: 10,
			want: "a compensation is a single task",
		},
		{
			name: "control flow under it",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      for_each:
        items: ${[1]}
        steps:
          - id: b
            log:
              message: hi
`,
			line: 8,
			want: "is control flow rather than a task",
		},
		{
			name: "the retired spelling",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      task:
        name: log
`,
			line: 8,
			want: "a step names its task directly now",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := flowfile.Parse([]byte(tt.src))
			require.Error(t, err, "the file compiled and should not have")

			var ds flowfile.Diagnostics
			require.ErrorAs(t, err, &ds, "the compiler's error is not diagnostics")
			require.NotEmpty(t, ds)

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.want) {
					found = true
					assert.Equal(t, tt.line, d.Line,
						"the diagnostic is on the wrong line:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic said %q; got:\n%s", tt.want, ds.Error())
		})
	}
}

// TestUndoValidationDiagnostics covers what only the validator can see: a task
// that does not exist, an input it does not have, and a reference that cannot
// resolve. Each is positioned on the `undo:` key, which is the one token the whole
// block hangs from.
func TestUndoValidationDiagnostics(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		line int
		want string
	}{
		{
			name: "unknown task",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      shell:
        command: rm -rf /
`,
			line: 7,
			want: `unknown task "shell"`,
		},
		{
			name: "an input the task does not have",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        mesage: typo
`,
			line: 7,
			want: `input "mesage"`,
		},
		{
			name: "a reference to a step that does not exist",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        message: ${steps.nope.thing}
`,
			line: 7,
			want: `"nope"`,
		},
		{
			name: "a reference to a step that has not run yet",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        message: ${steps.later.thing}
  - id: later
    log:
      message: hi
`,
			line: 7,
			want: `"later"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(tt.src))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "the file validated and should not have")

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.want) {
					found = true
					assert.Equal(t, tt.line, d.Line,
						"the diagnostic is on the wrong line:\n%s", d.Error())
					assert.Contains(t, d.Error(), `undo`,
						"the diagnostic does not say the problem is in the compensation:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic said %q; got:\n%s", tt.want, ds.Error())
		})
	}
}

// TestUndoPlacementIsRefusedWithAPosition covers the two shapes this version does
// not support, at the position an author wrote them.
//
// The refusals themselves are [v1.CheckUndoPlacement]'s, which both execution
// drivers also call — one rule, three enforcement points, and the only difference
// between them is that this one has a line number.
func TestUndoPlacementIsRefusedWithAPosition(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		line int
		want string
	}{
		{
			name: "inside a loop body",
			src: `edition: v2026.2
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          log:
            message: hi
          undo:
            log:
              message: bye
`,
			line: 11,
			want: "only supported on a top-level step",
		},
		{
			name: "inside a parallel branch",
			src: `edition: v2026.2
name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: inner
            log:
              message: hi
            undo:
              log:
                message: bye
`,
			line: 10,
			want: "only supported on a top-level step",
		},
		{
			name: "on control flow",
			src: `edition: v2026.2
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          log:
            message: hi
    undo:
      log:
        message: bye
`,
			line: 11,
			want: "only supported on a step that runs a task",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(tt.src))
			require.NoError(t, err)

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.want) {
					found = true
					assert.Equal(t, tt.line, d.Line,
						"the refusal is on the wrong line:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic said %q; got:\n%s", tt.want, ds.Error())
		})
	}
}

// TestUndoIsReservedAgainstTaskNames keeps the grammar unambiguous.
//
// A step key is a property or a task name, told apart by asking the registry,
// which only works while the two sets are disjoint. `undo` moved from the words
// held for later into the words the grammar speaks, and a plugin registering a
// task by that name would make `undo:` mean two incompatible things in one
// position.
func TestUndoIsReservedAgainstTaskNames(t *testing.T) {
	t.Parallel()

	assert.True(t, v1.IsReservedStepKey("undo"),
		"`undo:` is grammar and a task could still be registered under the name")
	assert.False(t, v1.IsFutureStepKey("undo"),
		"`undo:` is built, so reporting it as held for a later version would refuse a file that works")
}
