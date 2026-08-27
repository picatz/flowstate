package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The mapping spelling of a folded structure, held to bytes and to the compiled
// workflow.
//
// `compiler.composite` folds a mapping or a sequence holding a `${...}` into one
// CEL literal, and until #850 `Marshal` wrote that back as one fenced line — the
// rewrite that put a 778-character `statements:` in `examples/` and left
// `docs/DSL.md`'s worked example refused. `unfoldedStructure` offers the mapping
// back as a candidate and verifies it by re-compiling, so what has to be tested
// is both halves of that: the shapes it takes, and the shapes it declines.
//
// Every case asserts bytes *and* [proto.Equal] over the compiled workflow, and
// then formats the output a second time. That is CLAUDE.md's rewriter rule —
// never "it still validates", which is the check both `flow fix` corruptions
// passed — plus the fix-point the ratification asked for: a candidate that
// verifies once but does not settle churns the corpus on every run.
func TestMarshalUnfoldsAStructureItCanVerify(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a mapping of expressions is written as a mapping",
			src: `edition: v2026.3
name: w
steps:
  - id: submit
    log:
      message: hi
  - id: notify
    log:
      fields:
        deployment: ${steps.submit.deployment}
        approved_by: ${steps.submit.who}
      message: hello
`,
		},
		{
			name: "a comment inside one survives, because the key it names is written back",
			src: `edition: v2026.3
name: w
steps:
  - id: submit
    log:
      message: hi
  - id: notify
    log:
      fields:
        deployment: ${steps.submit.deployment}
        # sender, not payload.
        approved_by: ${steps.submit.who}
      message: hello
`,
		},
		{
			name: "a sequence of mappings, which is the shape examples/plugins/sql writes",
			src: `edition: v2026.3
name: w
inputs:
  key:
    type: string
    required: true
steps:
  - id: write
    log:
      fields:
        statements:
          - sql: INSERT INTO ledger (key) VALUES ($1)
            params:
              - ${inputs.key}
      message: hi
`,
		},
		{
			name: "literal entries beside an expression stay literals",
			src: `edition: v2026.3
name: w
steps:
  - id: call
    http:
      json:
        attempt: 1
        enabled: true
        ratio: 1.5
        missing: null
        name: prod
        who: ${inputs.who}
      url: https://example.com
`,
		},
		{
			name: "entry order is the author's, because a map literal written in another order is another value",
			src: `edition: v2026.3
name: w
steps:
  - id: call
    http:
      headers:
        B: ${inputs.b}
        A: "1"
      url: https://example.com
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := tc.want
			if want == "" {
				// The source is what the formatter should write for it: the
				// point of the unfold is that an authored mapping is already
				// canonical, so a case with no separate expectation is asserting
				// exactly that.
				want = tc.src
			}

			workflow, _, err := flowfile.Parse([]byte(tc.src))
			require.NoError(t, err)

			once, err := flowfile.Format([]byte(tc.src), workflow)
			require.NoError(t, err, "the formatter refused a file it has to be able to write")
			assert.Equal(t, want, string(once))

			after, _, err := flowfile.Parse(once)
			require.NoError(t, err, "the formatter wrote a document that no longer compiles")
			if !proto.Equal(workflow, after) {
				assert.Equal(t, workflow.String(), after.String(),
					"the unfolded mapping compiles to a different workflow, which is the formatter "+
						"rewriting what the file says rather than how it is written")
			}

			twice, err := flowfile.Format(once, after)
			require.NoError(t, err)
			assert.Equal(t, string(once), string(twice),
				"formatting twice is not formatting once, so `flow fmt` would rewrite its own output")
		})
	}
}

// The other half: the shapes the candidate is offered for and rejected on.
//
// Each of these keeps the one-line fenced form, and each keeps it for a reason
// the verification finds rather than a rule written down twice — which is the
// point of verifying by re-compiling at all. A test that only covered the
// accepted shapes would be green while the emitter unfolded something that means
// something else, which is exactly what it did to a shaping `outputs:` before
// [TestMarshalKeepsAShapingOutputsFenced] below existed.
func TestMarshalKeepsAStructureItCannotVerifyFenced(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a computed key has no YAML spelling",
			src: `edition: v2026.3
name: w
steps:
  - id: call
    http:
      url: https://example.com
      json: '${{"a" + "b": 1, "c": inputs.c}}'
`,
			want: `      json: '${{"a" + "b": 1, "c": inputs.c}}'`,
		},
		{
			name: "a macro cannot be written back as source at all",
			src: `edition: v2026.3
name: w
steps:
  - id: call
    http:
      url: https://example.com
      json: '${{"names": [1, 2].map(v, v + 1)}}'
`,
			want: `      json: '${{"names": [1, 2].map(v, v + 1)}}'`,
		},
		{
			name: "an all-constant mapping would compile back to a literal, not to this expression",
			src: `edition: v2026.3
name: w
steps:
  - id: call
    http:
      url: https://example.com
      json: '${{"a": 1, "b": 2}}'
`,
			want: `      json: '${{"a": 1, "b": 2}}'`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			workflow, _, err := flowfile.Parse([]byte(tc.src))
			require.NoError(t, err)

			once, err := flowfile.Format([]byte(tc.src), workflow)
			require.NoError(t, err)
			assert.Contains(t, string(once), tc.want,
				"a structure the unfold cannot verify was not written back in the fenced form")

			after, _, err := flowfile.Parse(once)
			require.NoError(t, err)
			if !proto.Equal(workflow, after) {
				assert.Equal(t, workflow.String(), after.String(), "the round trip changed the workflow")
			}
		})
	}
}

// A shaping task's `outputs:` is the one input whose mapping means something
// else, and the formatter has to know it.
//
// `compiler.shapedOutputs` compiles a mapping there entry by entry into a
// `Value_Structure`, so the names a step produces survive into the
// specification; a fenced expression there is the older spelling that cannot say
// what it produces. Unfolding one into a mapping would therefore hand the step a
// shaped output set it did not have — a formatter changing what a file says, and
// the defect `FuzzRoundTrip`'s third seed caught while this was being built.
func TestMarshalKeepsAShapingOutputsFenced(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: w
steps:
  - id: a
    http:
      url: https://example.com
      outputs: "${ {'status': status_code, 'body': body} }"
`

	workflow, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.NotNil(t, workflow.GetSteps()[0].GetTask().GetInputs()["outputs"].GetExpr(),
		"the fenced spelling is supposed to compile to one expression; this test is about writing that back")

	once, err := flowfile.Format([]byte(src), workflow)
	require.NoError(t, err)
	assert.Contains(t, string(once), `outputs: '${{"status": status_code, "body": body}}'`,
		"a shaping `outputs:` was unfolded into a mapping, which is a different value: the mapping form "+
			"compiles to a shaped output set and the fenced form to one expression")

	after, _, err := flowfile.Parse(once)
	require.NoError(t, err)
	if !proto.Equal(workflow, after) {
		assert.Equal(t, workflow.String(), after.String(),
			"formatting turned an http step's fenced `outputs:` into a shaped one")
	}
}

// The line the issue opened with, gone.
//
// `examples/plugins/sql/transfer.yaml` held a single 778-character line because
// the formatter wrote a four-entry `statements:` sequence back as one CEL list
// literal. This asserts the corpus has no line of that shape left, which is a
// claim about the reformat rather than about the emitter — the emitter's own
// claim is the fixed point in formatcorpus_test.go.
//
// The bound is a round number well above the longest line the corpus has, not a
// budget anybody tunes: it is here to catch the flattened spelling coming back,
// and a `message:` that is genuinely long prose is not what it is looking for.
func TestNoExampleHoldsAFlattenedStructure(t *testing.T) {
	t.Parallel()

	const tooLong = 240

	for _, example := range corpusWorkflows(t) {
		for i, line := range strings.Split(string(example.source), "\n") {
			if len(line) <= tooLong {
				continue
			}
			assert.NotContains(t, line, "${{",
				"examples/%s:%d is a %d-character line holding a folded structure, which is the "+
					"spelling #850 removed", example.rel, i+1, len(line))
			assert.NotContains(t, line, "${[",
				"examples/%s:%d is a %d-character line holding a folded structure, which is the "+
					"spelling #850 removed", example.rel, i+1, len(line))
		}
	}
}
