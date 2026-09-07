package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The schema's rules — `max_items` on a step list, `max_len` on an id — used to
// run only in the server. `flow validate` said ok and `flow run local` completed
// a 4,000-step file that `flow run` refused at submit (#1757): the local driver
// executing a program the durable one cannot accept. These pin that the rules
// now run where the author is, in the server's words, at a position.

// chainSource writes a Flowfile of n `log` steps, three lines each, after a
// three-line header — so the step at index i begins on line 4+3i.
func chainSource(n int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: chain\nsteps:\n")
	for i := range n {
		fmt.Fprintf(&b, "  - id: s%d\n    log:\n      message: hi\n", i)
	}
	return b.String()
}

// stepLine is the line the step at index i begins on in [chainSource].
func stepLine(i int) int { return 4 + 3*i }

// schemaSentence is the words the server refuses the same specification with,
// so the test asserts the author reads what the server would say rather than a
// paraphrase of it.
func schemaSentence(t *testing.T, wf *v1.Workflow) string {
	t.Helper()

	var invalid *v1.ValidationError
	require.ErrorAs(t, v1.Validate(&v1.RunRequest{Workflow: wf}), &invalid,
		"the server accepts this specification, so the diagnostic has nothing to agree with")
	require.Len(t, invalid.Violations, 1)

	return invalid.Violations[0].Message
}

func TestATooLongStepListIsRefusedWhereTheAuthorIs(t *testing.T) {
	t.Parallel()

	src := chainSource(101)

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err, "the file does not compile, so this test is about something else")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "expected exactly the schema's refusal:\n%s", ds.Error())

	d := ds[0]
	assert.Contains(t, d.Message, schemaSentence(t, wf),
		"the diagnostic does not say what the server would")
	assert.Contains(t, d.Message, "steps: must contain no more than 100 item(s)")
	assert.Equal(t, v1.DiagnosticCodeConstraintViolation, d.Code)

	// Placed on the 101st step: the element past the bound, which is the one an
	// author has to do something about.
	assert.Equal(t, "s100", d.Step, "the diagnostic names a step other than the first past the bound")
	assert.Equal(t, stepLine(100), d.Line, "the diagnostic is not on the 101st step's line:\n%s", d.Error())
}

func TestAStepListAtTheBoundIsAccepted(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(chainSource(100)))
	require.NoError(t, err)
	assert.Empty(t, ds, "a file exactly at the bound was refused:\n%s", ds.Error())
}

func TestATooLongStepIDIsRefusedWhereItIsWritten(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 129)
	src := "edition: v2026.3\nname: long-id\nsteps:\n  - id: " + long + "\n    log:\n      message: hi\n"

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "expected exactly the schema's refusal:\n%s", ds.Error())

	d := ds[0]
	assert.Contains(t, d.Message, schemaSentence(t, wf))
	assert.Contains(t, d.Message, "must be at most 128 characters")
	assert.Equal(t, long, d.Step)
	assert.Equal(t, 4, d.Line, "the diagnostic is not on the id's line:\n%s", d.Error())
}

func TestATooLongLoopBodyIsRefusedInsideTheLoop(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: body\nsteps:\n  - id: again\n    loop:\n      until: ${true}\n      max_iterations: 1\n      steps:\n")
	for i := range 101 {
		fmt.Fprintf(&b, "        - id: b%d\n          log:\n            message: hi\n", i)
	}
	src := b.String()

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "expected exactly the schema's refusal:\n%s", ds.Error())

	d := ds[0]
	assert.Contains(t, d.Message, schemaSentence(t, wf))
	assert.Contains(t, d.Message, "steps[0].loop.body: must contain no more than 100 item(s)")
	assert.Equal(t, "b100", d.Step, "the diagnostic names a step other than the first body step past the bound")
	assert.Equal(t, 9+3*100, d.Line, "the diagnostic is not on the 101st body step's line:\n%s", d.Error())
}

// TestASchemaRuleTheCompilerAlreadyReportsIsSaidOnce keeps one fault from being
// reported in two voices: the compiler's, which offers a name to paste, and the
// schema's, which quotes a regular expression.
func TestASchemaRuleTheCompilerAlreadyReportsIsSaidOnce(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.3\nname: my workflow\nsteps:\n  - id: a\n    log:\n      message: hi\n"

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "one illegal name was reported more than once:\n%s", ds.Error())
	assert.Contains(t, ds[0].Message, "my-workflow", "the compiler's own diagnostic, with its suggestion, was the one dropped")
}

// TestADefaultTheCompilerWritesIsNotRefusedAtSubmit is the other side of the
// join, found the moment the schema's rules ran here: a `for_each:` without
// `iterator:` compiles to an empty iterator and a `retry:` without `backoff:` to
// a zero coefficient — both drivers substitute their default for either — and the
// schema's pattern and floor refused both at submit. `docs/DSL.md`'s first
// example was one of them. The rules now skip the unset value, and this pins
// that what the compiler writes for an omitted key is what the server accepts.
func TestADefaultTheCompilerWritesIsNotRefusedAtSubmit(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"for_each without iterator": "edition: v2026.3\nname: a\nsteps:\n  - id: each\n    for_each:\n      items: \"${['a']}\"\n      steps:\n        - id: act\n          log:\n            message: ${item}\n",
		"retry without backoff":     "edition: v2026.3\nname: a\nsteps:\n  - id: fetch\n    retry:\n      attempts: 3\n    log:\n      message: hi\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(src))
			require.NoError(t, err)

			require.NoError(t, v1.Validate(&v1.RunRequest{Workflow: wf}),
				"the server refuses what the compiler wrote for an omitted key")

			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err)
			assert.Empty(t, ds, "flow validate refuses a file the drivers run:\n%s", ds.Error())
		})
	}

	// And a written value is still held to the rule: the floor exists so a
	// shrinking delay cannot be asked for.
	src := "edition: v2026.3\nname: a\nsteps:\n  - id: fetch\n    retry:\n      attempts: 3\n      backoff: 0.5\n    log:\n      message: hi\n"
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Len(t, ds, 1, "a backoff below one was not refused, or was refused twice:\n%s", ds.Error())
	assert.Contains(t, ds[0].Message, "must be greater than or equal to 1")
	assert.Equal(t, "fetch", ds[0].Step)
}
