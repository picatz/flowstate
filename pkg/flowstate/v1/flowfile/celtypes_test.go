package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every file below is a mistake the run refuses with a sentence of its own, hours
// in on a durable run, and which `flow validate` said `ok` to while the checker
// declared every name `dyn` (#1634). The text of a refusal is pinned where it is
// the fix an author needs; the code always is, because an agent branches on it.
func TestTypedScopeRefusesWhatCannotRun(t *testing.T) {
	t.Parallel()

	const header = `edition: v2026.3
name: t
inputs:
  count:
    type: int
    default: 3
  port:
    type: int
    default: 80
  names:
    type: list
    default: [a, b]
  label:
    type: string
    default: x
  mode:
    type: enum
    values: [fast, slow]
    default: fast
`

	for _, test := range []struct {
		name     string
		steps    string
		contains string
	}{
		{
			name: "an int input iterated",
			steps: `
steps:
  - id: loop
    for_each:
      items: ${inputs.count}
      steps:
        - id: x
          log:
            message: hi
`,
			contains: "`items:` is the list to iterate, so this expression must be a list, but it is typed int",
		},
		{
			name: "an int input as a condition",
			steps: `
steps:
  - id: a
    if: ${inputs.count}
    log:
      message: hi
`,
			contains: "`if:` is a condition, so this expression must be a bool, but it is typed int",
		},
		{
			name: "a string literal as an until",
			steps: `
steps:
  - id: l
    loop:
      until: ${"done"}
      max_iterations: 2
      steps:
        - id: x
          log:
            message: hi
`,
			contains: "`until:` is a condition, so this expression must be a bool, but it is typed string",
		},
		{
			name: "a string method on an int input",
			steps: `
steps:
  - id: a
    value: ${inputs.port.startsWith("8")}
`,
			contains: "startsWith",
		},
		{
			name: "an int added to a string value step",
			steps: `
steps:
  - id: s
    value: ${"abc"}
  - id: n
    value: ${steps.s.value + 1}
`,
			contains: "no matching overload",
		},
		{
			name: "a value step read as the wrong type in a condition",
			steps: `
steps:
  - id: s
    value: ${"abc"}
  - id: a
    if: ${steps.s.value}
    log:
      message: hi
`,
			contains: "must be a bool, but it is typed string",
		},
		{
			name: "a list method on a string input",
			steps: `
steps:
  - id: a
    value: ${inputs.label.map(c, c)}
`,
			contains: "map",
		},
		{
			name: "an enum input is a string, not a number",
			steps: `
steps:
  - id: a
    value: ${inputs.mode + 1}
`,
			contains: "no matching overload",
		},
		{
			name: "a call-free number compared with a string",
			steps: `
steps:
  - id: a
    value: ${inputs.port == "80"}
`,
			contains: "no matching overload",
		},
		{
			name: "a value step through another",
			steps: `
steps:
  - id: s
    value: ${inputs.count}
  - id: t
    value: ${steps.s.value}
  - id: u
    value: ${steps.t.value.lowerAscii()}
`,
			contains: "lowerAscii",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds := validateSource(t, header+test.steps)
			require.NotEmpty(t, ds, "validate said ok to a file that cannot run")
			assert.Contains(t, ds.Error(), test.contains)
			for _, d := range ds {
				assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code,
					"an agent branches on the code, so a type mistake must not arrive as `general`: %v", d)
			}
		})
	}
}

// TestTypedScopeIsSilentWhereTheFileDecidesNothing is the direction that would make
// the checker unusable if it were wrong. Each file runs.
func TestTypedScopeIsSilentWhereTheFileDecidesNothing(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "a list input iterated, a bool value step as a condition",
			source: `edition: v2026.3
name: t
inputs:
  names:
    type: list
    default: [a, b]
steps:
  - id: ok
    value: ${true}
  - id: a
    if: ${steps.ok.value}
    for_each:
      items: ${inputs.names}
      steps:
        - id: x
          log:
            message: hi
`,
		},
		{
			name: "a presence test and an optional read of a typed input",
			source: `edition: v2026.3
name: t
inputs:
  tag:
    type: string
steps:
  - id: a
    value: '${has(inputs.tag) ? inputs.tag : inputs.?tag.orValue("none")}'
`,
		},
		{
			name: "a struct input read through a selection",
			source: `edition: v2026.3
name: t
inputs:
  config:
    type: struct
    default:
      host: a
steps:
  - id: a
    value: ${inputs.config.host.lowerAscii() + string(inputs.config.port + 1)}
`,
		},
		{
			name: "a map value read for a key its literal never had",
			source: `edition: v2026.3
name: t
steps:
  - id: prefs
    value: '${{"volume": 7}}'
  - id: a
    value: ${steps.prefs.value.?muted.orValue("loud")}
`,
		},
		{
			name: "an id used twice in different loops is not typed from either",
			source: `edition: v2026.3
name: t
steps:
  - id: one
    for_each:
      items: ${[1, 2]}
      steps:
        - id: page
          value: ${1}
  - id: two
    for_each:
      items: ${["a", "b"]}
      steps:
        - id: page
          value: ${"x"}
        - id: use
          value: ${steps.page.value + "y"}
`,
		},
		{
			name: "a value read from a response stays dyn",
			source: `edition: v2026.3
name: t
steps:
  - id: s
    value: '${json_parse("{\"n\": 1}").n}'
  - id: u
    value: ${steps.s.value.size()}
`,
		},
		{
			name: "an optional is not a type to refuse as a list",
			source: `edition: v2026.3
name: t
steps:
  - id: prefs
    value: '${{"volume": 7}}'
  - id: loop
    for_each:
      items: ${steps.prefs.value.?stations.orValue(["a"])}
      steps:
        - id: x
          log:
            message: hi
`,
		},
		{
			name: "a value step that reads itself is left to the reference walk",
			source: `edition: v2026.3
name: t
steps:
  - id: s
    value: ${steps.s.value}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			for _, d := range validateSource(t, test.source) {
				// A self-reference is the reference walk's own diagnostic; what must
				// not appear is a type one.
				assert.NotEqual(t, v1.DiagnosticCodeTypeMismatch, d.Code, "a type nothing in the file decides was refused: %v", d)
			}
		})
	}
}

// TestValueStepTypesReachLaterExpressions is #1636's second half: an output's
// declared type is checked against the type of the `value:` step it reads.
func TestValueStepTypesReachLaterExpressions(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: t
steps:
  - id: s
    value: ${"abc"}
outputs:
  n:
    type: int
    value: ${steps.s.value}
`
	ds := validateSource(t, source)
	d, ok := findDiagnostic(ds, "outputs.n")
	require.True(t, ok, "no diagnostic against the output: %v", ds)
	assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code)
	assert.Contains(t, d.Message, `output "n" is declared int, but this expression always produces string`)

	assert.Empty(t, validateSource(t, `edition: v2026.3
name: t
steps:
  - id: s
    value: ${"abc"}
outputs:
  n:
    type: string
    value: ${steps.s.value}
`))
}

// TestCallArgumentsAreCheckedAgainstTypedScope: a `with:` argument is evaluated in
// the caller's scope, so a value step's type is what the callee's declaration is
// held to.
func TestCallArgumentsAreCheckedAgainstTypedScope(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
inputs:
  n:
    type: int
steps:
  - id: a
    value: ${inputs.n}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: s
    value: ${"abc"}
  - id: c
    call: ./callee.yaml
    with:
      n: ${steps.s.value}
`)

	ds, err := flowfile.ValidateSourceFile(caller)
	require.NoError(t, err)
	require.NotEmpty(t, ds)
	assert.Contains(t, ds.Error(), "declared int by workflow")
	assert.Contains(t, ds.Error(), "always produces string")
}

// TestValueStepTypesFollowWrittenOrder: a value step's type is declared only for a
// position written after it. A forward reference, a self reference and a read of
// a loop-body step from outside it are the reference walk's to report once, in its
// own sentence, and must not also be refused as a type mismatch.
func TestValueStepTypesFollowWrittenOrder(t *testing.T) {
	t.Parallel()

	for name, source := range map[string]string{
		"a forward reference": `edition: v2026.3
name: t
steps:
  - id: early
    value: ${steps.late.value + 1}
  - id: late
    value: ${"abc"}
`,
		"a self reference": `edition: v2026.3
name: t
steps:
  - id: s
    value: ${steps.s.value.size()}
`,
		"a loop-body step read from outside": `edition: v2026.3
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          value: ${"abc"}
  - id: after
    value: ${steps.inner.value + 1}
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds := validateSource(t, source)
			require.NotEmpty(t, ds, "the reference walk must still report it")
			for _, d := range ds {
				assert.Equal(t, v1.DiagnosticCodeUnresolvedReference, d.Code, "reported twice: %v", d)
			}
		})
	}
}

// TestCyclicDeclarationTypeIsBounded: a hand-built input whose structural type
// points back at itself reaches the checker before the declaration bounds do.
func TestCyclicDeclarationTypeIsBounded(t *testing.T) {
	t.Parallel()

	cyclic := &v1.Type{}
	cyclic.Kind = &v1.Type_List{List: cyclic}

	wf := &v1.Workflow{
		Name: "t",
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "xs", Type: v1.InputDeclaration_TYPE_LIST, ValueType: cyclic,
		}},
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Value{Value: &v1.Value{Kind: &v1.Value_Literal{
				Literal: &exprpb.Value{Kind: &exprpb.Value_Int64Value{Int64Value: 1}},
			}}},
		}},
	}

	assert.NotPanics(t, func() { _ = flowfile.Validate(wf) })
}

// TestTaskAndCallOutputsAreTyped: a task's declared fields and a called workflow's
// declared outputs reach later expressions with their types (#1383, #1643). Each
// file is refused by the checker, where it used to run until the step read it.
func TestTaskAndCallOutputsAreTyped(t *testing.T) {
	t.Parallel()

	const http = `edition: v2026.3
name: t
steps:
  - id: get
    http:
      method: GET
      url: https://example.com
`

	for _, test := range []struct {
		name     string
		source   string
		contains string
	}{
		{
			name: "an http status compared with a string",
			source: http + `  - id: n
    value: ${steps.get.status_code == "200"}
`,
			contains: "no matching overload",
		},
		{
			name: "an http status used as a condition",
			source: http + `  - id: a
    if: ${steps.get.status_code}
    log:
      message: hi
`,
			contains: "must be a bool, but it is typed int",
		},
		{
			name: "an http body treated as a map",
			source: http + `  - id: n
    value: ${steps.get.body.size() + steps.get.status_code.size()}
`,
			contains: "size",
		},
		{
			name: "an http header read as a number",
			source: http + `  - id: n
    value: ${steps.get.headers["X-Count"] + 1}
`,
			contains: "no matching overload",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds := validateSource(t, test.source)
			require.NotEmpty(t, ds, "validate said ok to a file that cannot run")
			assert.Contains(t, ds.Error(), test.contains)
			for _, d := range ds {
				assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code, "%v", d)
			}
		})
	}

	t.Run("a called workflow's declared output", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
steps:
  - id: a
    value: ${3}
outputs:
  count:
    type: int
    value: ${steps.a.value}
`)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: c
    call: ./callee.yaml
  - id: n
    value: ${steps.c.count.startsWith("x")}
`)

		ds, err := flowfile.ValidateSourceFile(caller)
		require.NoError(t, err)
		require.NotEmpty(t, ds)
		assert.Contains(t, ds.Error(), "startsWith")
	})
}

// TestTaskAndCallOutputsStaySilentWhereNothingIsKnown is the direction that would
// refuse a file that runs. Each of these reads an output the definition says
// nothing about, or one the step does not have in scope.
func TestTaskAndCallOutputsStaySilentWhereNothingIsKnown(t *testing.T) {
	t.Parallel()

	for name, source := range map[string]string{
		"a response json is dyn": `edition: v2026.3
name: t
steps:
  - id: get
    http:
      method: GET
      url: https://example.com
      parse_json: true
  - id: n
    value: ${steps.get.json.items[0].id + 1}
`,
		"a shaped output is the author's expression": `edition: v2026.3
name: t
steps:
  - id: get
    http:
      method: GET
      url: https://example.com
      outputs:
        code: ${response.status_code}
  - id: n
    value: ${steps.get.code.startsWith("2")}
`,
		"a status compared with an int": `edition: v2026.3
name: t
steps:
  - id: get
    http:
      method: GET
      url: https://example.com
  - id: n
    value: ${steps.get.status_code == 200 && steps.get.body.startsWith("{")}
`,
		"a header read as a string": `edition: v2026.3
name: t
steps:
  - id: get
    http:
      method: GET
      url: https://example.com
  - id: n
    value: ${steps.get.headers["Content-Type"].startsWith("text")}
`,
		"a forward read of a task output": `edition: v2026.3
name: t
steps:
  - id: early
    value: ${steps.get.status_code.size()}
  - id: get
    http:
      method: GET
      url: https://example.com
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds := validateSource(t, source)
			for _, d := range ds {
				assert.NotEqual(t, v1.DiagnosticCodeTypeMismatch, d.Code,
					"a file that runs was refused: %v", d)
			}
		})
	}
}

// A declared output with no `type:` promised nothing, so a caller's read of it is
// as free as it always was.
func TestUntypedCallOutputStaysDyn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
steps:
  - id: a
    value: ${3}
outputs:
  count:
    value: ${steps.a.value}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: c
    call: ./callee.yaml
  - id: n
    value: ${steps.c.count.startsWith("x")}
`)

	ds, err := flowfile.ValidateSourceFile(caller)
	require.NoError(t, err)
	for _, d := range ds {
		assert.NotEqual(t, v1.DiagnosticCodeTypeMismatch, d.Code, "%v", d)
	}
}

// An output may be named `value`, which only a `value:` step's own output means
// to the checker; a call's declared `value` output is typed by its declaration.
func TestAnOutputNamedValueIsTypedByItsStep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
steps:
  - id: a
    value: ${3}
outputs:
  value:
    type: int
    value: ${steps.a.value}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: c
    call: ./callee.yaml
  - id: n
    value: ${steps.c.value.startsWith("x")}
`)

	ds, err := flowfile.ValidateSourceFile(caller)
	require.NoError(t, err)
	require.NotEmpty(t, ds)
	assert.Contains(t, ds.Error(), "startsWith")
}
