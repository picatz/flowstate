package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// These tests pin the positional model, because every feature's precision depends
// on it: a range that is off by one puts a squiggle under the wrong character and
// makes hover resolve to the wrong token.

func TestParseRecordsExactRanges(t *testing.T) {
	t.Parallel()

	// A positional fixture rather than a workflow: nothing here is compiled, and
	// `outputs:` is written as a scalar on purpose. The `${steps.first.result}`
	// below is read for its span and never resolved — `log:` declares no outputs,
	// so it names nothing, which is exactly why it is safe to measure against.
	const src = `edition: v2026.3
name: model
steps:
  - id: first
    log:
      message: "quoted value"
  - id: second
    http:
      url: https://example.com
      headers:
        X-Trace: ${steps.first.result}
      outputs: "{'code': status_code}"
`
	doc := newDocument("file:///model.yaml", 1, src, nil)
	require.NoError(t, doc.parseErr)
	require.NotNil(t, doc.parsed)

	t.Run("top level keys", func(t *testing.T) {
		require.NotNil(t, doc.parsed.nameEntry)
		assert.Equal(t, "name", textInRange(src, doc.parsed.nameEntry.keyRange))
		assert.Equal(t, "model", textInRange(src, doc.parsed.nameEntry.valueRange()))

		require.NotNil(t, doc.parsed.stepsEntry)
		assert.Equal(t, "steps", textInRange(src, doc.parsed.stepsEntry.keyRange))
	})

	require.Len(t, doc.parsed.steps, 2)
	first, second := doc.parsed.steps[0], doc.parsed.steps[1]

	t.Run("step identity", func(t *testing.T) {
		assert.Equal(t, "first", first.id)
		assert.Equal(t, "log", first.taskName)
		assert.Equal(t, "first", textInRange(src, first.idEntry.valueRange()))
		assert.Equal(t, "log", textInRange(src, first.taskEntry.keyRange))
	})

	t.Run("a quoted value's range includes its quotes", func(t *testing.T) {
		// The range is what an editor underlines, and underlining the text inside
		// the quotes but not the quotes reads as an off-by-one bug.
		message := first.input("message")
		require.NotNil(t, message)
		assert.Equal(t, `"quoted value"`, textInRange(src, message.valueRange()))
		// The decoded text is what the compiler sees.
		assert.Equal(t, "quoted value", message.value.text)
	})

	t.Run("an expression nested in a mapping is located", func(t *testing.T) {
		headers := second.input("headers")
		require.NotNil(t, headers)
		require.Equal(t, kindMapping, headers.value.kind)
		require.Len(t, headers.value.entries, 1)

		nested := headers.value.entries[0]
		assert.Equal(t, "X-Trace", nested.key)
		require.NotNil(t, nested.value)
		require.True(t, nested.value.fenced)
		assert.Equal(t, "steps.first.result", nested.value.expr)
		assert.Equal(t, "${steps.first.result}", textInRange(src, nested.value.exprRange))
		// exprOffset must point at the first character of the expression source,
		// which is where a CEL error location is measured from.
		assert.Equal(t, "steps.first.result}", src[nested.value.exprOffset:nested.value.exprOffset+len("steps.first.result}")])
	})

	t.Run("a quoted expression input reports its content offset", func(t *testing.T) {
		outputs := second.input("outputs")
		require.NotNil(t, outputs)
		// textOffset skips the opening quote, so a CEL error inside lands inside.
		assert.Equal(t, byte('{'), src[outputs.value.textOffset])
		assert.Equal(t, `"{'code': status_code}"`, textInRange(src, outputs.valueRange()))
	})

	t.Run("step ranges cover whole steps and do not overlap", func(t *testing.T) {
		// The boundaries are each step's `- id:` line and its last written line.
		// They are lower than they once were because the flat form spends one line
		// on the task where the nested form spent three, not because a range now
		// stops somewhere else: the end of `first` is still the `message:` line and
		// the end of `second` is still the `outputs:` line.
		//
		// Each is one greater than it reads in the flat-form commit, because this
		// fixture carries the `edition:` marker *first*. Most fixtures in this package
		// append it instead — see [editionSuffix] — but a step's range runs to the line
		// before the next step's dash, and the last step's end is walked back over
		// trailing blank lines only, so a top-level key written after the steps extends
		// it. Any fixture asserting the last step's end has to write the marker above.
		assert.Equal(t, 3, first.rng.Start.Line)
		assert.Equal(t, 5, first.rng.End.Line)
		assert.Equal(t, 6, second.rng.Start.Line)
		assert.Equal(t, 11, second.rng.End.Line)
	})
}

func TestParseHandlesShapesTheDSLDoesNotExpect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		// check runs against the model, which must be usable in every case.
		check func(t *testing.T, doc *document)
	}{
		{
			name: "a block scalar is recorded by whole lines and not by columns",
			src: `name: block
steps:
  - id: a
    log:
      message: |
        1 + 1
edition: v2026.3
`,
			check: func(t *testing.T, doc *document) {
				require.Len(t, doc.parsed.steps, 1)
				expr := doc.parsed.steps[0].input("message")
				require.NotNil(t, expr)
				require.NotNil(t, expr.value)
				// The range runs from the header line to the last content line.
				// Whole lines are as fine as it gets: a folded reconstruction
				// cannot be measured against the source column by column.
				assert.Equal(t, 4, expr.value.rng.Start.Line)
				assert.Equal(t, 5, expr.value.rng.End.Line)
				// `1 + 1` is expression source but it is not written as a fence,
				// and the compiler asks the same question of a block scalar as of
				// any other — so this one is not an expression to either of them.
				assert.False(t, expr.value.fenced)
				// Nothing derived from a block scalar carries inner positions,
				// whether or not it turned out to be fenced.
				assert.False(t, expr.value.inline)
			},
		},
		{
			name: "a single-key mapping is the same shape as a multi-key one",
			// The parser returns a bare MappingValueNode for one key and a
			// MappingNode for two, and the model must not care which. The flat form
			// makes that the common case rather than a corner: a task's value is its
			// inputs, and a one-input task is an ordinary thing to write.
			src: `name: single
steps:
  - id: a
    log:
      message: only
edition: v2026.3
`,
			check: func(t *testing.T, doc *document) {
				require.Len(t, doc.parsed.steps, 1)
				assert.Len(t, doc.parsed.steps[0].inputs, 1)
				assert.Equal(t, "only", doc.parsed.steps[0].input("message").valueText())
			},
		},
		{
			name: "a task with no value is still a task",
			src: `name: pending
steps:
  - id: a
    log:
edition: v2026.3
`,
			// `log:` on a line by itself names the task and gives it no inputs,
			// which is a complete step as far as the grammar is concerned — whether
			// log can run without a message is the registry's question, answered
			// by the validator. The key is the name, so a half-written step still
			// has somewhere for a diagnostic to land.
			check: func(t *testing.T, doc *document) {
				require.Len(t, doc.parsed.steps, 1)
				step := doc.parsed.steps[0]
				require.NotNil(t, step.taskEntry, "the key itself is still recorded")
				assert.Nil(t, step.taskEntry.value)
				assert.Equal(t, "log", step.taskName)
				assert.Empty(t, step.inputs)
				assert.Equal(t, step.taskEntry.keyRange, step.taskEntry.valueRange())
			},
		},
		{
			name: "an alias value is recorded but not interpreted",
			// The alias moved with the grammar rather than staying where it was: the
			// old form could alias the whole `task:` block, and the flat form has no
			// such block to alias. What a step can still be handed by reference is
			// the task's *inputs*, so that is what this now aliases — the same
			// question (a value the DSL cannot see inside), asked where it can be
			// asked.
			src: `name: alias
base: &b
  message: hi
steps:
  - id: a
    log: *b
edition: v2026.3
`,
			check: func(t *testing.T, doc *document) {
				require.Len(t, doc.parsed.steps, 1)
				step := doc.parsed.steps[0]
				require.NotNil(t, step.taskEntry)
				require.NotNil(t, step.taskEntry.value)
				assert.Equal(t, kindOther, step.taskEntry.value.kind)
				// Nothing is claimed about a shape the DSL does not describe.
				assert.Empty(t, step.inputs)
			},
		},
		{
			name: "steps that is not a sequence yields no steps",
			src:  "name: x\nsteps: nope\n" + editionSuffix,
			check: func(t *testing.T, doc *document) {
				assert.Empty(t, doc.parsed.steps)
				assert.NotNil(t, doc.parsed.stepsEntry)
			},
		},
		{
			name: "an empty document has a usable model",
			src:  "",
			check: func(t *testing.T, doc *document) {
				require.NotNil(t, doc.parsed)
				assert.Nil(t, doc.parsed.nameEntry)
				assert.Empty(t, doc.parsed.steps)
			},
		},
		{
			name: "a flow-style step is modelled like a block one",
			src:  "name: flow\nsteps: [{id: a, log: {message: hi}}]\n" + editionSuffix,
			check: func(t *testing.T, doc *document) {
				require.Len(t, doc.parsed.steps, 1)
				step := doc.parsed.steps[0]
				assert.Equal(t, "a", step.id)
				assert.Equal(t, "log", step.taskName)
				assert.Equal(t, "hi", step.input("message").valueText())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := newDocument("file:///shape.yaml", 1, tt.src, nil)
			require.NoError(t, doc.parseErr, "this shape should parse as YAML")
			require.NotNil(t, doc.parsed)
			tt.check(t, doc)

			// Whatever the shape, analysis must complete without panicking.
			assert.NotPanics(t, func() { diagnose(doc) })
		})
	}
}

func TestStepLookupPrefersTheFirstDeclaration(t *testing.T) {
	t.Parallel()

	// Duplicate ids are a diagnostic, but until the author fixes one the model has
	// to resolve references somehow. The first declaration wins, matching the order
	// the engine would write outputs in.
	doc := newDocument("file:///dupe.yaml", 1, `name: dupe
steps:
  - id: a
    log:
      message: one
  - id: a
    http:
      url: https://example.com
edition: v2026.3
`, nil)
	require.NoError(t, doc.parseErr)
	step := doc.parsed.step("a")
	require.NotNil(t, step)
	assert.Equal(t, 0, step.index)
	assert.Equal(t, "log", step.taskName)
}

func TestOutlineScannerTolerantOfBrokenText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []outlineStep
	}{
		{
			name: "a half-typed input key",
			src: `name: x
steps:
  - id: first
    log:
      mes
edition: v2026.3
`,
			want: []outlineStep{{id: "first", taskName: "log"}},
		},
		{
			name: "nested values are not mistaken for input names",
			src: `name: x
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        X-One: a
        X-Two: b
edition: v2026.3
`,
			want: []outlineStep{{
				id:        "a",
				taskName:  "http",
				inputKeys: []string{"url", "headers"},
			}},
		},
		{
			name: "a comment does not end a step",
			// The comment sits between the step's keys rather than between `task:`
			// and `name:`, there being no level in between any more. It is the same
			// hazard either way: a skipped line must not be allowed to fix the
			// column the scanner reads a step's own keys at.
			src: `name: x
steps:
  - id: a
    # which task to run
    log:
      message: hi
edition: v2026.3
`,
			want: []outlineStep{{id: "a", taskName: "log", inputKeys: []string{"message"}}},
		},
		{
			name: "no steps key at all",
			src:  "name: x\n" + editionSuffix,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scanOutline(newLineIndex(tt.src), v1.DefaultRegistry())
			require.Len(t, got, len(tt.want))
			for i, want := range tt.want {
				assert.Equal(t, want.id, got[i].id, "step %d id", i)
				assert.Equal(t, want.taskName, got[i].taskName, "step %d task", i)
				if want.inputKeys != nil {
					assert.Equal(t, want.inputKeys, got[i].inputKeys, "step %d input keys", i)
				}
			}
		})
	}
}

func TestKeyPath(t *testing.T) {
	t.Parallel()

	const src = `name: x
steps:
  - id: a
    log:
      message: hi
      headers:
        X: y
    retry:
      attempts: 3
edition: v2026.3
`
	ix := newLineIndex(src)
	// The path a task's inputs sit under is the task's own name now, because that
	// is the key they are written under — there is no `task` or `inputs` level left
	// to name. Two cases became one for the same reason: `inside task` and `inside
	// inputs` asked about two levels that are now a single key. The remaining lines
	// moved up with the levels the flat form dropped, and each still asks about the
	// line of the fixture it always did.
	tests := []struct {
		name string
		line int
		want []string
	}{
		{name: "top level", line: 0, want: []string{}},
		{name: "a step's own key", line: 3, want: []string{"steps"}},
		{name: "inside a task's inputs", line: 4, want: []string{"steps", "log"}},
		{name: "inside a nested input value", line: 6, want: []string{"steps", "log", "headers"}},
		{name: "inside retry", line: 8, want: []string{"steps", "retry"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, keyPath(ix, tt.line))
		})
	}
}
