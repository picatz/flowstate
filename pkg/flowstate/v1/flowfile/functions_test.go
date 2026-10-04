package flowfile_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `functions:` block, from the file's side: that a use compiles to the body
// and no call, that the file writes back with the definition and the use as
// written, and that each way a definition can be wrong is reported once, at the
// definition. Inlining, checking and bounds are [v1.FunctionSet]'s, tested in
// package v1.

const slugFile = `edition: v2026.4
name: slugs
functions:
  slug:
    description: A title as it appears in a URL.
    params:
      title: string
    returns: string
    body: ${title.trim().lowerAscii().replace(" ", "-")}
  path:
    params:
      base: string
      title: string
    returns: string
    body: ${base + "/" + slug(title)}
inputs:
  title:
    type: string
    required: true
steps:
  - id: a
    value: ${slug(inputs.title)}
  - id: b
    value: ${path("/posts", inputs.title)}
  - id: c
    value: ${[slug(inputs.title), slug("X Y")].join(",")}
outputs:
  result:
    value: ${steps.b.value}
`

// calledNames returns every function name any expression in wf calls.
func calledNames(wf *v1.Workflow) map[string]bool {
	names := map[string]bool{}
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		parsed := site.Value.GetExpr()
		if parsed == nil {
			return
		}
		stack := []*exprpb.Expr{parsed.GetExpr()}
		for len(stack) > 0 {
			e := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			switch kind := e.GetExprKind().(type) {
			case *exprpb.Expr_CallExpr:
				names[kind.CallExpr.GetFunction()] = true
				stack = append(stack, kind.CallExpr.GetTarget())
				stack = append(stack, kind.CallExpr.GetArgs()...)
			case *exprpb.Expr_SelectExpr:
				stack = append(stack, kind.SelectExpr.GetOperand())
			case *exprpb.Expr_ListExpr:
				stack = append(stack, kind.ListExpr.GetElements()...)
			case *exprpb.Expr_StructExpr:
				for _, entry := range kind.StructExpr.GetEntries() {
					stack = append(stack, entry.GetMapKey(), entry.GetValue())
				}
			case *exprpb.Expr_ComprehensionExpr:
				c := kind.ComprehensionExpr
				stack = append(stack, c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult())
			}
		}
	}})

	return names
}

func TestFunctionsCompileToTheBodyAndNoCall(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(slugFile))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))
	require.NoError(t, v1.Validate(wf))

	require.Len(t, wf.GetDeclaredFunctions(), 2)
	slug := wf.GetDeclaredFunctions()[0]
	assert.Equal(t, "slug", slug.GetName())
	assert.Equal(t, "A title as it appears in a URL.", slug.GetDescription())
	assert.Equal(t, "string", v1.TypeString(slug.GetParameters()[0].GetType()))
	assert.Equal(t, "string", v1.TypeString(slug.GetResult()))

	// A use is the body: no step calls `slug` or `path`, and the arguments are
	// bound once, which is `cel.bind`'s comprehension in the stored tree.
	called := calledNames(wf)
	assert.False(t, called["slug"], "a call to slug survived compilation")
	assert.False(t, called["path"], "a call to path survived compilation")
	assert.True(t, called["lowerAscii"], "the body was not inlined")
}

func TestFunctionsWriteBackWithTheUseAsWritten(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(slugFile))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, slugFile, string(written),
		"the definitions are kept and each use is written as the author wrote it, not as its expansion")

	formatted, err := flowfile.Format([]byte(slugFile), wf)
	require.NoError(t, err)
	assert.Equal(t, slugFile, string(formatted))

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again), "the written file compiles to a different workflow")
}

func TestFunctionsRunTheSameAsTheirExpansionByHand(t *testing.T) {
	t.Parallel()

	withFunctions, _, err := flowfile.Parse([]byte(slugFile))
	require.NoError(t, err)
	byHand, _, err := flowfile.Parse([]byte(`edition: v2026.4
name: slugs
inputs:
  title:
    type: string
    required: true
steps:
  - id: a
    value: ${inputs.title.trim().lowerAscii().replace(" ", "-")}
  - id: b
    value: ${"/posts" + "/" + inputs.title.trim().lowerAscii().replace(" ", "-")}
  - id: c
    value: ${[inputs.title.trim().lowerAscii().replace(" ", "-"), "X Y".trim().lowerAscii().replace(" ", "-")].join(",")}
outputs:
  result:
    value: ${steps.b.value}
`))
	require.NoError(t, err)

	run := func(wf *v1.Workflow) *v1.Workflow_StepOutputs {
		t.Helper()
		require.NoError(t, v1.ResolveTaskCapabilities(wf, v1.DefaultRegistry()))
		out, err := v1.RunWithInputs(t.Context(), wf, map[string]*v1.Value{"title": v1.NewLiteral("  Hello World ")})
		require.NoError(t, err)

		return out
	}
	assert.True(t, proto.Equal(run(byHand), run(withFunctions)))
}

func TestAFunctionIsRefusedAtItsDefinition(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		// functions is the block, written under `functions:`.
		functions string
		line      int
		want      string
	}{
		{
			name: "recursion",
			functions: `  loopy:
    params:
      n: int
    returns: int
    body: ${loopy(n - 1)}
`,
			line: 4,
			want: "is recursive: loopy calls loopy",
		},
		{
			name: "mutual recursion",
			functions: `  ping:
    params:
      n: int
    returns: int
    body: ${pong(n)}
  pong:
    params:
      n: int
    returns: int
    body: ${ping(n)}
`,
			line: 4,
			want: "ping calls pong calls ping",
		},
		{
			name: "a body that does not check",
			functions: `  bad:
    params:
      n: int
    returns: int
    body: ${n + "x"}
`,
			line: 8,
			want: "body does not type-check",
		},
		{
			name: "a body that reads the workflow",
			functions: `  leaky:
    returns: string
    body: ${inputs.title}
`,
			line: 6,
			want: "a function sees only its parameters, so pass the value in as an argument",
		},
		{
			name: "a result of another type",
			functions: `  wrong:
    params:
      n: int
    returns: string
    body: ${n}
`,
			line: 4,
			want: "declares result string but its body produces int",
		},
		{
			name: "a name the language has",
			functions: `  size:
    returns: int
    body: ${1}
`,
			line: 4,
			want: "a name the language already has",
		},
		{
			name: "a name that is not lowerCamel",
			functions: `  Slug:
    returns: int
    body: ${1}
`,
			line: 4,
			want: "is not a function name",
		},
		{
			name: "no returns",
			functions: `  untyped:
    body: ${1}
`,
			line: 4,
			want: "has no `returns:`",
		},
		{
			name: "no body",
			functions: `  empty:
    returns: int
`,
			line: 4,
			want: "has no `body:`",
		},
		{
			name: "an unknown parameter type",
			functions: `  odd:
    params:
      n: integer
    returns: int
    body: ${n}
`,
			line: 6,
			want: "is not a type",
		},
		{
			name: "an unknown key",
			functions: `  odd:
    returns: int
    body: ${1}
    pure: true
`,
			line: 7,
			want: `unknown key "pure"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := "edition: v2026.4\nname: t\nfunctions:\n" + test.functions + `inputs:
  title:
    type: string
steps:
  - id: a
    log:
      message: hi
`
			_, _, err := flowfile.Parse([]byte(source))
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)

			var ds flowfile.Diagnostics
			require.True(t, errors.As(err, &ds))
			require.Len(t, ds, 1, "a mistake in one definition is reported once: %v", err)
			assert.Equal(t, test.line, ds[0].Line, "the diagnostic lands on the definition: %v", ds[0])
		})
	}
}

func TestACallIsCheckedAgainstTheDeclaredSignature(t *testing.T) {
	t.Parallel()

	source := func(use string) string {
		return `edition: v2026.4
name: t
functions:
  twice:
    params:
      n: int
    returns: int
    body: ${n * 2}
steps:
  - id: a
    value: ` + use + `
`
	}

	for use, want := range map[string]string{
		"${twice('x')}":     "no matching overload",
		"${twice(1, 2)}":    "no matching overload",
		"${twice(1) + 'x'}": "no matching overload",
	} {
		_, _, err := flowfile.Parse([]byte(source(use)))
		require.Error(t, err, use)
		assert.Contains(t, err.Error(), want, use)
	}

	wf, _, err := flowfile.Parse([]byte(source("${twice(twice(3))}")))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))

	// A name nobody declared is the checker's to refuse, as it always was.
	wf, _, err = flowfile.Parse([]byte(source("${nosuchfunction(1)}")))
	require.NoError(t, err)
	assert.Contains(t, flowfile.Validate(wf).Error(), "nosuchfunction")
}

func TestAFunctionIsUsableWhereverAnExpressionIs(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: t
functions:
  isBig:
    params:
      n: int
    returns: bool
    body: ${n > 100}
  label:
    params:
      n: int
    returns: string
    body: ${"n=" + string(n)}
inputs:
  count:
    type: int
    required: true
vars:
  limit: ${isBig(500)}
steps:
  - id: a
    if: ${isBig(inputs.count)}
    log:
      message: big ${label(inputs.count)}
  - id: b
    value: '${isBig(1) ? label(1) : label(2)}'
    if: ${vars.limit}
outputs:
  text:
    value: ${label(inputs.count)}
`
	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))

	called := calledNames(wf)
	assert.False(t, called["isBig"])
	assert.False(t, called["label"])

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Contains(t, string(written), "isBig(inputs.count)")
	assert.Contains(t, string(written), "string(label(inputs.count))", "an interpolation is written as the concatenation it compiled to, calls as written")
}

func TestFunctionsLeaveAFileWithoutThemAlone(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(typesSource("", "string")))
	require.NoError(t, err)
	assert.Empty(t, wf.GetDeclaredFunctions())

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.False(t, strings.Contains(string(written), "functions:"))
}

// The expression a function expands into stays a fixed point of the round trip
// through source, which is what keeps Marshal an exact inverse for a file that
// uses functions inside comprehensions and with nested calls.
func TestAnExpandedExpressionIsAFixedPointOfTheRoundTrip(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.4
name: t
functions:
  inc:
    params:
      k: int
    returns: int
    body: ${k + 1}
  both:
    params:
      a: int
      b: int
    returns: int
    body: ${a + a + b}
inputs:
  xs:
    type: list(int)
    required: true
steps:
  - id: a
    value: ${inputs.xs.map(x, inc(x))}
  - id: b
    value: ${both(inc(1), inc(inc(2)))}
  - id: c
    value: ${inputs.xs.map(a, both(7, a))}
`
	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, source, string(written))

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again))

	for _, step := range wf.GetSteps() {
		text, err := cel.AstToString(cel.ParsedExprToAst(step.GetValue().GetExpr()))
		require.NoError(t, err)
		assert.NotContains(t, text, "__", step.GetId())
	}
}

// Each expansion is within its own bound, so the file's total is what stops a few
// hundred uses of a large composed function from spending the compiler's memory.
func TestFunctionExpansionIsBoundedAcrossTheFile(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: t\nfunctions:\n  f0:\n    params:\n      k: int\n    returns: int\n    body: ${k + k}\n")
	for i := 1; i <= 11; i++ {
		fmt.Fprintf(&b, "  f%d:\n    params:\n      k: int\n    returns: int\n    body: ${f%d(k) + f%d(k + 1)}\n", i, i-1, i-1)
	}
	b.WriteString("steps:\n")
	for i := range 300 {
		fmt.Fprintf(&b, "  - id: s%d\n    value: ${f11(%d)}\n", i, i)
	}

	_, _, err := flowfile.Parse([]byte(b.String()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expand past 100000 CEL nodes in this file altogether")

	var ds flowfile.Diagnostics
	require.True(t, errors.As(err, &ds))
	assert.Len(t, ds, 1, "the limit is reported once, not at every later use")
}

// An argument the file can type is held to the declared parameter, which the
// expanded tree (a bind over an argument of unknown type) can no longer say.
func TestACallIsCheckedAgainstTheTypesTheFileStatesForItsArguments(t *testing.T) {
	t.Parallel()

	source := func(use, slot string) string {
		extra := ""
		if slot == "if" {
			extra = "\n    value: 1"
		}

		return `edition: v2026.4
name: t
functions:
  identity:
    params:
      n: int
    returns: int
    body: ${n}
inputs:
  title:
    type: string
    required: true
  count:
    type: int
    required: true
steps:
  - id: a
    ` + slot + `: ` + use + extra + `
`
	}

	wf, _, err := flowfile.Parse([]byte(source("${identity(inputs.title)}", "value")))
	require.NoError(t, err, "the compiler cannot type an input; validation can")
	err = flowfile.Validate(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity")

	wf, _, err = flowfile.Parse([]byte(source("${identity(inputs.count) + 1}", "value")))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf), "a conforming call is not refused")

	// The declared result is the call's type, so a position that wants a bool
	// refuses an int-returning function by name rather than by what its body did.
	wf, _, err = flowfile.Parse([]byte(source("${identity(inputs.count)}", "if")))
	require.NoError(t, err)
	err = flowfile.Validate(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "condition")
}

// A record parameter's fields are the record's, in the body as at an input.
func TestAFunctionBodyIsHeldToTheFieldsOfItsRecordParameter(t *testing.T) {
	t.Parallel()

	source := func(body string) string {
		return `edition: v2026.4
name: t
types:
  User:
    fields:
      id: {type: string}
      email: {type: string}
functions:
  handle:
    params:
      user: User
    returns: dyn
    body: ` + body + `
inputs:
  who:
    type: User
    required: true
steps:
  - id: a
    value: ${handle(inputs.who)}
`
	}

	wf, _, err := flowfile.Parse([]byte(source("${user.email}")))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))

	wf, _, err = flowfile.Parse([]byte(source("${user.emial}")))
	require.NoError(t, err)
	err = flowfile.Validate(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `the record User has no field "emial"`)
	assert.Contains(t, err.Error(), `Did you mean "email"?`)
}
