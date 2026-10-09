package flowfile_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// A constrained scalar, from the file's side: a type with `type:` and `must:` that
// every use lowers to its base and a rule, so the run reads neither the name nor
// the type, and `flow fmt` writes the name back.

const scalarFile = `edition: v2026.4
name: scalars
types:
  Code:
    description: A three-letter, two-digit code.
    type: string
    must: isCode(this)
    example: abc-12
  Tag:
    fields:
      code:
        type: Code
        required: true
functions:
  isCode:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[a-z]{3}-[0-9]{2}$")}
inputs:
  id:
    type: Code
    required: true
    must: this != "abc-00"
  alias:
    type: Code
  tag:
    type: Tag
steps:
  - id: show
    value: ${inputs.id}
outputs:
  result:
    value: ${steps.show.value}
    type: Code
`

func TestAScalarTypeLowersAtEachUse(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(scalarFile))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))
	require.NoError(t, v1.Validate(wf))

	code := wf.GetDeclaredTypes()[0]
	assert.Equal(t, v1.InputDeclaration_TYPE_STRING, code.GetBase())
	assert.True(t, code.IsScalar())
	assert.Equal(t, conformance.FunctionMustExpansion, code.GetMust(), "the type's rule is stored expanded, like any must")
	assert.Equal(t, "isCode(this)", code.GetMustSource())
	assert.Equal(t, "abc-12", code.GetExample().GetLiteral().GetStringValue())
	assert.NotContains(t, v1.TypesOf(wf), "Code", "a scalar is not a record")

	id, alias := wf.GetDeclaredInputs()[0], wf.GetDeclaredInputs()[1]
	assert.Equal(t, v1.InputDeclaration_TYPE_STRING, id.GetType())
	assert.Nil(t, id.GetValueType(), "the use carries the base, not a reference to the type")
	assert.Equal(t, "Code", id.GetTypeSource())
	assert.Equal(t, conformance.ScalarTypeLoweredMust, id.GetMust(), "the type's rule and the use's own, conjoined")
	assert.Equal(t, conformance.ScalarTypeOwnMust, id.GetMustSource(), "the use's own rule as written")

	assert.Equal(t, "Code", alias.GetTypeSource())
	assert.Equal(t, conformance.FunctionMustExpansion, alias.GetMust(), "a use with no rule of its own takes the type's alone")
	assert.Nil(t, alias.MustSource, "no rule was written there, so none is kept")

	field := wf.GetDeclaredTypes()[1].GetFields()[0]
	assert.Equal(t, v1.InputDeclaration_TYPE_STRING, field.GetType())
	assert.Equal(t, "Code", field.GetTypeSource())
	assert.Equal(t, conformance.FunctionMustExpansion, field.GetMust())

	result := wf.GetDeclaredOutputs()[0]
	assert.Equal(t, v1.InputDeclaration_TYPE_STRING, result.GetType())
	assert.Equal(t, "Code", result.GetTypeSource())
	assert.Equal(t, conformance.FunctionMustExpansion, result.GetMust())

	assert.Nil(t, wf.GetDeclaredInputs()[2].TypeSource, "a record use is a reference and is not lowered")
	assert.Equal(t, "Tag", wf.GetDeclaredInputs()[2].GetValueType().GetMessage())
}

func TestAScalarTypeWritesBackAsAuthoredAndIsAFixedPoint(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(scalarFile))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, scalarFile, string(written), "the type name and the use's own rule are written, not what lowering made")

	formatted, err := flowfile.Format([]byte(scalarFile), wf)
	require.NoError(t, err)
	assert.Equal(t, scalarFile, string(formatted))

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again), "the written file compiles to a different workflow")
}

// Every scalar base lowers, and a double is written back as `double` however it
// was spelled in the specification.
func TestEveryScalarBaseLowers(t *testing.T) {
	t.Parallel()

	for base, want := range map[string]v1.InputDeclaration_Type{
		"string":    v1.InputDeclaration_TYPE_STRING,
		"int":       v1.InputDeclaration_TYPE_INT,
		"double":    v1.InputDeclaration_TYPE_FLOAT,
		"bool":      v1.InputDeclaration_TYPE_BOOL,
		"timestamp": v1.InputDeclaration_TYPE_TIMESTAMP,
		"duration":  v1.InputDeclaration_TYPE_DURATION,
		"bytes":     v1.InputDeclaration_TYPE_BYTES,
	} {
		t.Run(base, func(t *testing.T) {
			t.Parallel()

			source := fmt.Sprintf("edition: v2026.4\nname: t\ntypes:\n  T:\n    type: %s\n    must: this == this\ninputs:\n  x:\n    type: T\nsteps:\n  - id: a\n    value: ${1}\n", base)
			wf, _, err := flowfile.Parse([]byte(source))
			require.NoError(t, err)
			require.Empty(t, flowfile.Validate(wf))

			assert.Equal(t, want, wf.GetDeclaredInputs()[0].GetType())
			assert.Equal(t, "T", wf.GetDeclaredInputs()[0].GetTypeSource())

			written, err := flowfile.Marshal(wf)
			require.NoError(t, err)
			assert.Contains(t, string(written), "    type: "+base+"\n")
		})
	}
}

func TestAScalarTypeIsRefusedWhereItIsNotOne(t *testing.T) {
	t.Parallel()

	const rule = "    must: isCode(this)\n"
	for _, test := range []struct {
		name     string
		from, to string
		want     string
	}{
		{
			name: "a rule that reads outside this",
			from: rule, to: "    must: inputs.id == this\n",
			want: "inputs",
		},
		{
			name: "a rule that reads the clock",
			from: rule, to: "    must: timestamp(this) < now\n",
			want: "now",
		},
		{
			name: "a rule that is not a predicate",
			from: rule, to: "    must: size(this)\n",
			want: "rather than a bool",
		},
		{
			name: "no rule at all",
			from: "    must: isCode(this)\n    example: abc-12\n", to: "",
			want: "no `must:`",
		},
		{
			name: "a list base",
			from: "    type: string\n    must: isCode(this)", to: "    type: list(string)\n    must: isCode(this)",
			want: "not a scalar",
		},
		{
			name: "an enum base",
			from: "    type: string\n    must: isCode(this)", to: "    type: enum\n    must: isCode(this)",
			want: "not a scalar",
		},
		{
			name: "a record base",
			from: "    type: string\n    must: isCode(this)", to: "    type: Tag\n    must: isCode(this)",
			want: "another declared type",
		},
		{
			name: "itself as the base",
			from: "    type: string\n    must: isCode(this)", to: "    type: Code\n    must: isCode(this)",
			want: "another declared type",
		},
		{
			name: "fields beside the base",
			from: rule, to: rule + "    fields:\n      x: {type: string}\n",
			want: "both a constrained scalar",
		},
		{
			name: "an example the rule refuses",
			from: "example: abc-12", to: "example: nope",
			want: "type \"Code\" example",
		},
		{
			name: "an example of the wrong type",
			from: "example: abc-12", to: "example: 12",
			want: "example",
		},
		{
			name: "an example on a record",
			from: "      code:\n        type: Code\n        required: true\n", to: "      code:\n        type: Code\n        required: true\n    example: x\n",
			want: "has no `example:`",
		},
		{
			name: "a list of a scalar type",
			from: "  alias:\n    type: Code\n", to: "  alias:\n    type: list(Code)\n",
			want: "inside a container",
		},
		{
			name: "a map of a scalar type",
			from: "  alias:\n    type: Code\n", to: "  alias:\n    type: \"map(string, Code)\"\n",
			want: "inside a container",
		},
		{
			name: "a function parameter typed by a scalar type",
			from: "      s: string\n", to: "      s: Code\n",
			want: "constrained scalar Code",
		},
		{
			name: "a function result typed by a scalar type",
			from: "    returns: bool\n", to: "    returns: Code\n",
			want: "constrained scalar Code",
		},
		{
			name: "an enum's values on a scalar use",
			from: "  alias:\n    type: Code\n", to: "  alias:\n    type: Code\n    values: [a, b]\n",
			want: "values",
		},
		{
			name: "a default the type refuses",
			from: "  alias:\n    type: Code\n", to: "  alias:\n    type: Code\n    default: nope\n",
			want: "default",
		},
		{
			name: "a recursive function in the rule",
			from: `body: ${s.matches("^[a-z]{3}-[0-9]{2}$")}`, to: `body: ${isCode(s)}`,
			want: "is recursive",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := replaceOnce(t, scalarFile, test.from, test.to)
			wf, _, err := flowfile.Parse([]byte(source))
			if err == nil {
				err = flowfile.Validate(wf).Err()
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

// A specification that never was a Flowfile is held to the same rules, because the
// runtime has nothing to resolve a scalar type's name with.
func TestASpecificationThatStillNamesAScalarTypeIsRefused(t *testing.T) {
	t.Parallel()

	for name, edit := range map[string]func(*v1.Workflow){
		"an input typed by the name": func(wf *v1.Workflow) {
			in := wf.DeclaredInputs[1]
			in.Type = v1.InputDeclaration_TYPE_STRUCT
			in.ValueType = &v1.Type{Kind: &v1.Type_Message{Message: "Code"}}
		},
		"a list of the name": func(wf *v1.Workflow) {
			in := wf.DeclaredInputs[1]
			in.Type = v1.InputDeclaration_TYPE_LIST
			in.ValueType = &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Message{Message: "Code"}}}}
		},
		"a field typed by the name": func(wf *v1.Workflow) {
			f := wf.DeclaredTypes[1].Fields[0]
			f.Type = v1.InputDeclaration_TYPE_STRUCT
			f.ValueType = &v1.Type{Kind: &v1.Type_Message{Message: "Code"}}
		},
		"an output typed by the name": func(wf *v1.Workflow) {
			out := wf.DeclaredOutputs[0]
			out.Type = v1.InputDeclaration_TYPE_STRUCT
			out.ValueType = &v1.Type{Kind: &v1.Type_Message{Message: "Code"}}
		},
		"a function parameter typed by the name": func(wf *v1.Workflow) {
			wf.DeclaredFunctions[0].Parameters[0].Type = &v1.Type{Kind: &v1.Type_Message{Message: "Code"}}
		},
		"a function result typed by the name": func(wf *v1.Workflow) {
			wf.DeclaredFunctions[0].Result = &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Message{Message: "Code"}}}}
		},
		"a base with no rule":          func(wf *v1.Workflow) { wf.DeclaredTypes[0].Must = nil },
		"a base and fields":            func(wf *v1.Workflow) { wf.DeclaredTypes[0].Fields = wf.DeclaredTypes[1].Fields },
		"a base that is not a scalar":  func(wf *v1.Workflow) { wf.DeclaredTypes[0].Base = v1.InputDeclaration_TYPE_LIST.Enum() },
		"a base that is an enum":       func(wf *v1.Workflow) { wf.DeclaredTypes[0].Base = v1.InputDeclaration_TYPE_ENUM.Enum() },
		"a rule that reads the inputs": func(wf *v1.Workflow) { wf.DeclaredTypes[0].Must = proto.String("inputs.id == this") },
		"an example on a record":       func(wf *v1.Workflow) { wf.DeclaredTypes[1].Example = wf.DeclaredTypes[0].Example },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(scalarFile))
			require.NoError(t, err)
			edit(wf)

			require.Error(t, flowfile.Validate(wf).Err())
		})
	}
}

// The type's rule is copied to each use, so each use spends the file's expansion
// budget, and the refusal is made once where the budget runs out.
func TestAScalarTypeSharesTheFilesExpansionBudget(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: t\nfunctions:\n  f0:\n    params:\n      k: int\n    returns: int\n    body: ${k + k}\n")
	for i := 1; i <= 8; i++ {
		fmt.Fprintf(&b, "  f%d:\n    params:\n      k: int\n    returns: int\n    body: ${f%d(k) + f%d(k + 1)}\n", i, i-1, i-1)
	}
	b.WriteString("types:\n  Big:\n    type: int\n    must: f8(this) > 0\ninputs:\n")
	for i := range 400 {
		fmt.Fprintf(&b, "  n%d:\n    type: Big\n", i)
	}
	b.WriteString("steps:\n  - id: a\n    value: ${1}\n")

	_, _, err := flowfile.Parse([]byte(b.String()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expand past 100000 CEL nodes in this file altogether")

	var ds flowfile.Diagnostics
	require.True(t, errors.As(err, &ds))
	assert.Len(t, ds, 1, "the limit is reported once, not at every later use")

	// The same file with few enough uses is accepted, so it is the count of uses
	// and not the type that spends the budget.
	few := strings.Replace(b.String(), "  n399:\n    type: Big\n", "", 1)
	for i := 10; i < 400; i++ {
		few = strings.Replace(few, fmt.Sprintf("  n%d:\n    type: Big\n", i), "", 1)
	}
	_, _, err = flowfile.Parse([]byte(few))
	require.NoError(t, err)
}
