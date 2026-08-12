package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The spelling this feature ships under, written once here so a change to it fails
// a test rather than quietly becoming a second grammar.
//
// It is the file from docs/DSL.md's design note, which is what makes this a test of
// the contract rather than of whatever the parser happens to accept.
const declaringSource = `edition: v2026.3
name: deploy
inputs:
  region:
    type: string
    required: true
    description: which region to deploy to
  retries:
    type: int
    default: 3
vars:
  service: api
steps:
  - id: deploy
    log:
      message: ${'deploying ' + vars.service + ' to ' + inputs.region}
outputs:
  where:
    value: ${inputs.region}
    description: where the thing ended up
  attempts:
    value: ${inputs.retries}
`

// TestParsingTheDeclaredBlocks pins what the two blocks compile to.
func TestParsingTheDeclaredBlocks(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(declaringSource))
	require.NoError(t, err)

	require.Len(t, workflow.GetDeclaredInputs(), 2)

	region := workflow.GetDeclaredInputs()[0]
	require.Equal(t, "region", region.GetName(), "declarations are not in the order they were written")
	require.Equal(t, v1.InputDeclaration_TYPE_STRING, region.GetType())
	require.True(t, region.GetRequired())
	require.Equal(t, "which region to deploy to", region.GetDescription())
	require.Nil(t, region.GetDefault())

	retries := workflow.GetDeclaredInputs()[1]
	require.Equal(t, "retries", retries.GetName())
	require.Equal(t, v1.InputDeclaration_TYPE_INT, retries.GetType())
	require.False(t, retries.GetRequired())
	require.Equal(t, int64(3), retries.GetDefault().GetLiteral().GetInt64Value())

	require.Len(t, workflow.GetDeclaredOutputs(), 2)
	require.Equal(t, "where", workflow.GetDeclaredOutputs()[0].GetName())
	require.NotNil(t, workflow.GetDeclaredOutputs()[0].GetValue().GetExpr(),
		"an output's value compiled to something other than an expression")
	require.Equal(t, "where the thing ended up", workflow.GetDeclaredOutputs()[0].GetDescription())

	// Positions, because a diagnostic about a declaration has to be able to name the
	// line it is written on — which is what every check in declarations.go depends
	// on to be worth reporting at all.
	for _, path := range []string{
		"inputs", "inputs.region", "inputs.region.type", "inputs.retries.default",
		"outputs", "outputs.where", "outputs.where.value",
	} {
		_, ok := positions.At(path)
		require.True(t, ok, "no source position was recorded for %q", path)
	}

	// And the file validates, which is the house gate's first half: a capability
	// lands when a Flowfile can express it and `flow validate` accepts it.
	diagnostics, err := flowfile.ValidateSource([]byte(declaringSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "the spelling this feature ships under does not validate")
}

// TestTheDeclaredBlocksRoundTrip pins that writing the workflow back out produces
// the document it was read from.
//
// Byte comparison rather than "it still validates", per the rewriter lesson: a
// document that validates can still mean something else, and a block Marshal does
// not write is a block `flow fix` deletes.
func TestTheDeclaredBlocksRoundTrip(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse([]byte(declaringSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)

	round, err := flowfile.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, string(written), string(round), "a second round trip changed the document")

	// The declarations survive whole, in order, rather than merely surviving.
	require.Len(t, again.GetDeclaredInputs(), 2)
	require.Equal(t, "region", again.GetDeclaredInputs()[0].GetName())
	require.Equal(t, "retries", again.GetDeclaredInputs()[1].GetName())
	require.True(t, again.GetDeclaredInputs()[0].GetRequired())
	require.Equal(t, int64(3), again.GetDeclaredInputs()[1].GetDefault().GetLiteral().GetInt64Value())
	require.Len(t, again.GetDeclaredOutputs(), 2)
	require.Equal(t, "where", again.GetDeclaredOutputs()[0].GetName())
	require.Equal(t, "attempts", again.GetDeclaredOutputs()[1].GetName())
}

// TestDeclarationDiagnostics covers every check the schema defers to the compiler,
// plus the misspellings the house rule says must be reported rather than ignored.
func TestDeclarationDiagnostics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		// want is a substring the diagnostics must contain; empty means the file
		// must validate cleanly.
		want string
	}{
		{
			name: "a misspelled key inside a declaration is reported",
			src: `
inputs:
  region:
    type: string
    requried: true
`,
			want: `unknown key "requried"`,
		},
		{
			name: "a misspelled key inside an output is reported",
			src: `
outputs:
  where:
    value: ${steps.a.body}
    describe: nope
`,
			want: `unknown key "describe"`,
		},
		{
			name: "an input with no type is reported",
			src: `
inputs:
  region:
    required: true
`,
			want: "has no `type:`",
		},
		{
			name: "an unknown type names the ones that exist",
			src: `
inputs:
  region:
    type: text
`,
			want: "is not a type an input can have",
		},
		{
			name: "an output with no value is reported",
			src: `
outputs:
  where:
    description: nowhere
`,
			want: "has no `value:`",
		},
		{
			// The four CEL lexer tokens, which rooting cannot rescue.
			name: "an input named for a CEL token is refused",
			src: `
inputs:
  "in":
    type: string
`,
			want: "is punctuation in CEL",
		},
		{
			// And the seventeen it does rescue, which is the point of the root:
			// `inputs.namespace` is a field selection, so the word is fine here.
			name: "an input may be named for a word CEL reserves in identifier position",
			src: `
inputs:
  namespace:
    type: string
steps:
  - id: a
    log:
      message: ${inputs.namespace}
`,
		},
		{
			// Refused by the YAML parser's own key rule, before the declaration walk
			// can see it. Kept as a case because what matters is that the file is
			// refused and the message names the key — where the refusal comes from is
			// an implementation detail an author never has to know. The compiler's
			// own duplicate check answers for a specification that never was a
			// Flowfile; see TestDeclarationChecksOnAHandBuiltSpecification.
			name: "two inputs may not share a name",
			src: `
inputs:
  region:
    type: string
  region:
    type: int
`,
			want: `mapping key "region" already defined`,
		},
		{
			name: "a default may not be an expression",
			src: `
inputs:
  region:
    type: string
    default: ${vars.region}
`,
			want: "a default must be a value rather than an expression",
		},
		{
			name: "a default may not be a secret",
			src: `
inputs:
  token:
    type: string
    default: ${secret('env:TOKEN')}
`,
			want: "may not be a secret reference",
		},
		{
			name: "a required input with a default is a contradiction",
			src: `
inputs:
  region:
    type: string
    required: true
    default: eu-west-1
`,
			want: "which contradict",
		},
		{
			name: "a default of the wrong type is reported where it is written",
			src: `
inputs:
  retries:
    type: int
    default: "three"
`,
			want: `is declared int but was given string`,
		},
		{
			// #206's first finding: a min_items above the server-wide element cap
			// (10,000) can never be satisfied, since every list over 10,000 is
			// refused before this constraint runs. Caught by flow validate the same
			// way min_items > max_items already is.
			name: "min_items above the server-wide element cap is reported",
			src: `
inputs:
  records:
    type: list
    min_items: 10001
`,
			want: "min_items (10001) is greater than 10000",
		},
		{
			// #206's second finding: a literal default over the element cap used to
			// pass flow validate and only fail once BindRunInputs saw it at submit.
			src: `
inputs:
  records:
    type: list
    default: [` + strings.Repeat("0, ", 10_000) + `0]
`,
			name: "a literal default over the server-wide element cap is reported",
			want: "list elements",
		},
		{
			name: "an undeclared input reference is reported",
			src: `
inputs:
  region:
    type: string
steps:
  - id: a
    log:
      message: ${inputs.reigon}
`,
			want: `references unknown input "reigon"; did you mean "region"?`,
		},
		{
			name: "a file with no inputs says what the block is",
			src: `
steps:
  - id: a
    log:
      message: ${inputs.region}
`,
			want: "this workflow declares no `inputs:`",
		},
		{
			name: "a var may not read an input",
			src: `
inputs:
  region:
    type: string
vars:
  where: ${inputs.region}
`,
			want: "a var may not read an input",
		},
		{
			name: "an output may read the last step",
			src: `
steps:
  - id: a
    http:
      url: https://example.com
outputs:
  body:
    value: ${steps.a.body}
`,
		},
		{
			name: "an output may not read a loop's binding",
			src: `
steps:
  - id: each
    for_each:
      items: ${[1, 2]}
      as: item
      steps:
        - id: body
          log:
            message: ${string(item)}
outputs:
  last:
    value: ${item}
`,
			want: `references unknown name "item"`,
		},
		{
			name: "an output may not read an undeclared input",
			src: `
steps:
  - id: a
    log:
      message: hello
outputs:
  where:
    value: ${inputs.region}
`,
			want: `references unknown input "region"`,
		},
		{
			// A name that differs from its neighbour only by whitespace, which is
			// what a duplicate looks like in a file somebody wrote. Refused because
			// it is not an identifier — the check that would call it a duplicate
			// cannot see it, since to YAML these are two different keys.
			name: "an output name that is not an identifier is refused",
			src: `
steps:
  - id: a
    log:
      message: hello
outputs:
  where:
    value: ${'a'}
  "where ":
    value: ${'b'}
`,
			want: "is not a valid identifier",
		},
		{
			name: "a step may not be called inputs",
			src: `
steps:
  - id: inputs
    log:
      message: hello
`,
			want: "is the root the run's inputs are named under",
		},
		{
			name: "a loop may not bind inputs",
			src: `
steps:
  - id: each
    for_each:
      items: ${[1]}
      as: inputs
      steps:
        - id: body
          log:
            message: hello
`,
			want: "is the root the run's inputs are named under",
		},
		{
			name: "a step var may not be called vars",
			src: `
vars:
  region: eu-west-1
steps:
  - id: a
    vars:
      vars: ${1}
    log:
      message: hello
`,
			want: "is the root the workflow's vars are named under",
		},
		{
			// #206: `run` carries the run's starter identity. The same collision
			// rule as `inputs`, `vars` and `steps`, for the identical reason — a
			// step of that id would hide it from every expression after it.
			name: "a step may not be called run",
			src: `
steps:
  - id: run
    log:
      message: hello
`,
			want: "is the root the run's own address and starter identity are named under",
		},
		{
			name: "a loop may not bind run",
			src: `
steps:
  - id: each
    for_each:
      items: ${[1]}
      as: run
      steps:
        - id: body
          log:
            message: hello
`,
			want: "is the root the run's own address and starter identity are named under",
		},
		{
			name: "a step var may not be called run",
			src: `
steps:
  - id: a
    vars:
      run: ${1}
    log:
      message: hello
`,
			want: "is the root the run's own address and starter identity are named under",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := "edition: " + flowfile.CurrentEdition + "\nname: declaring\n" + tt.src
			if !strings.Contains(tt.src, "steps:") {
				src += "\nsteps:\n  - id: a\n    log:\n      message: hello\n"
			}

			diagnostics, err := flowfile.ValidateSource([]byte(src))
			if err != nil {
				// A compile failure carries its own diagnostics, which is where the
				// parser's half of these checks lands.
				require.NotEmpty(t, tt.want, "the file did not compile: %v", err)
				require.Contains(t, err.Error(), tt.want)

				return
			}

			if tt.want == "" {
				require.Empty(t, diagnostics, "the file was refused")

				return
			}

			require.Contains(t, diagnostics.Error(), tt.want)

			// Every diagnostic about a declaration names where it is written. A
			// message with no position sends an author looking through the file for
			// the line it is about, which for a block of ten declarations is the
			// whole of the work the diagnostic was meant to save.
			for _, d := range diagnostics {
				if strings.Contains(d.Message, tt.want) {
					require.NotZero(t, d.Line, "the diagnostic has no position: %s", d.Error())
				}
			}
		})
	}
}

// TestDeclarationChecksOnAHandBuiltSpecification covers the checks a Flowfile
// cannot reach.
//
// Two declarations sharing a name is refused by the YAML parser's own key rule
// before the compiler sees it, so the compiler's check answers for the other way a
// specification arrives: built by hand and submitted over the RPC, which is a path
// with no YAML in it at all. A rule that is only enforced by the file format is not
// enforced.
func TestDeclarationChecksOnAHandBuiltSpecification(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name:    "hand-built",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "region", Type: v1.InputDeclaration_TYPE_STRING},
			{Name: "region", Type: v1.InputDeclaration_TYPE_INT},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "where", Value: v1.NewExpr("inputs.region")},
			{Name: "where", Value: v1.NewExpr("inputs.region")},
		},
		Steps: []*v1.Node{{
			Id:   "a",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")}}},
		}},
	}

	reported := flowfile.Validate(workflow).Error()
	require.Contains(t, reported, `duplicate input "region"`)
	require.Contains(t, reported, `duplicate output "where"`)
}

// TestFixRewritesInsideAnOutputAndNowhereElse pins `flow fix` against the two new
// blocks, by bytes.
//
// The rewriter's failures have all been the same shape — it knew less about scope
// than the language does — and a new expression position is exactly where that
// happens next. An `outputs:` value is an ordinary expression, so a legacy bare
// step reference in one must be rooted; a declaration's `type:`, `default:` and
// `description:` are not expressions at all, so nothing in them may be touched
// however much a scalar there looks like a name.
//
// Compared as bytes rather than by validating the result, per the lesson: a
// corrupted file still validates, it simply computes something else.
func TestFixRewritesInsideAnOutputAndNowhereElse(t *testing.T) {
	t.Parallel()

	src := `edition: ` + flowfile.CurrentEdition + `
name: fixing
inputs:
  region:
    type: string
    default: fetch
steps:
  - id: fetch
    http:
      url: ${'https://example.com/' + inputs.region}
outputs:
  body:
    value: ${fetch.body}
  where:
    value: ${inputs.region}
`

	want := strings.Replace(src, "${fetch.body}", "${steps.fetch.body}", 1)

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Equal(t, want, string(result.Source),
		"`flow fix` rewrote something other than the one bare step reference")

	diagnostics, err := flowfile.ValidateSource(result.Source)
	require.NoError(t, err)
	require.Empty(t, diagnostics, "`flow fix` produced a file the validator rejects")
}
