package lsp

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const extractHeader = "edition: " + flowfile.CurrentEdition + "\nname: x\n"

const extractSteps = "steps:\n  - id: s\n    log:\n      message: hi\n"

// extractBlockSource repeats one rule on two inputs, an output and a record field.
const extractBlockSource = extractHeader + `inputs:
  customer_id:
    type: string
    must: size(this) > 2
    required: true
  owner:
    type: string
    must: size(this) > 2
  other:
    type: string
    must: size(this) > 5
outputs:
  result:
    value: ${inputs.owner}
    type: string
    must: size(this) > 2
types:
  # the record
  Record:
    fields:
      slug:
        type: string
        must: size(this) > 2
` + extractSteps

// extractActionsAt returns the extract-type actions offered over a line.
func extractActionsAt(t *testing.T, src string, line int) []codeAction {
	t.Helper()
	const uri = "file:///extract.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, src)

	var out []codeAction
	for _, a := range c.codeAction(uri, atLine(line), nil, nil) {
		if strings.HasPrefix(a.Title, "Extract type") {
			out = append(out, a)
		}
	}
	return out
}

func lineOf(t *testing.T, src, needle string) int {
	t.Helper()
	for i, l := range strings.Split(src, "\n") {
		if strings.Contains(l, needle) {
			return i
		}
	}
	require.Failf(t, "line not found", "%q", needle)
	return 0
}

func applyExtract(t *testing.T, src string, a codeAction) string {
	t.Helper()
	edits := a.Edit.Changes["file:///extract.yaml"]
	require.NotEmpty(t, edits)
	return applyAll(t, src, edits)
}

func TestExtractTypeRewritesEveryIdenticalOccurrence(t *testing.T) {
	t.Parallel()

	actions := extractActionsAt(t, extractBlockSource, lineOf(t, extractBlockSource, "customer_id:"))
	require.Len(t, actions, 1)
	assert.Equal(t, "Extract type 'CustomerId'", actions[0].Title)

	got := applyExtract(t, extractBlockSource, actions[0])
	assert.Equal(t, extractHeader+`inputs:
  customer_id:
    type: CustomerId
    required: true
  owner:
    type: CustomerId
  other:
    type: string
    must: size(this) > 5
outputs:
  result:
    value: ${inputs.owner}
    type: CustomerId
types:
  CustomerId:
    type: string
    must: size(this) > 2
  # the record
  Record:
    fields:
      slug:
        type: CustomerId
`+extractSteps, got)

	// The edited file validates, and compiles to the same lowered rules.
	before, _, bd, err := flowfile.ParseAndValidateSourceAt([]byte(extractBlockSource), "")
	require.NoError(t, err)
	after, _, ad, err := flowfile.ParseAndValidateSourceAt([]byte(got), "")
	require.NoError(t, err)
	assert.Empty(t, bd)
	assert.Empty(t, ad)
	for i, in := range before.DeclaredInputs {
		assert.Equal(t, in.GetMust(), after.DeclaredInputs[i].GetMust(), in.GetName())
		assert.Equal(t, in.GetType(), after.DeclaredInputs[i].GetType(), in.GetName())
	}
	assert.Equal(t, before.DeclaredOutputs[0].GetMust(), after.DeclaredOutputs[0].GetMust())
}

func TestExtractTypeAddsATypesBlockWhenThereIsNone(t *testing.T) {
	t.Parallel()

	src := extractHeader + "inputs:\n  port:\n    type: int\n    must: this > 0\n" + extractSteps
	actions := extractActionsAt(t, src, lineOf(t, src, "port:"))
	require.Len(t, actions, 1)
	assert.Equal(t, extractHeader+"types:\n  Port:\n    type: int\n    must: this > 0\ninputs:\n  port:\n    type: Port\n"+extractSteps,
		applyExtract(t, src, actions[0]))
}

func TestExtractTypeOffersNothingWhenUnsafeOrUnsure(t *testing.T) {
	t.Parallel()

	rule := "    type: string\n    must: size(this) > 2\n"
	cases := map[string]string{
		"name collides with a type":        extractHeader + "types:\n  Port:\n    type: int\n    must: this > 0\ninputs:\n  port:\n" + rule + extractSteps,
		"name collides with a second type": extractHeader + "types:\n  Other:\n    type: int\n    must: this > 0\ninputs:\n  other:\n" + rule + extractSteps,
		"trailing comment on the rule":     extractHeader + "inputs:\n  port:\n    type: string\n    must: size(this) > 2 # keep\n" + extractSteps,
		"anchor on the declaration":        extractHeader + "inputs:\n  port: &p\n" + rule + extractSteps,
		"anchor on the type":               extractHeader + "inputs:\n  port:\n    type: &t string\n    must: size(this) > 2\n" + extractSteps,
		"flow-style identical sibling":     extractHeader + "inputs:\n  port:\n" + rule + "  host: {type: string, must: size(this) > 2}\n" + extractSteps,
		"folded rule":                      extractHeader + "inputs:\n  port:\n    type: string\n    must: >-\n      size(this) > 2\n" + extractSteps,
		"merge key":                        extractHeader + "inputs:\n  port:\n" + rule + "  base: {<<: {}}\n" + extractSteps,
		"not a scalar base":                extractHeader + "inputs:\n  port:\n    type: list(string)\n    must: size(this) > 2\n" + extractSteps,
		"name cannot be derived":           extractHeader + "inputs:\n  \"é\":\n" + rule + extractSteps,
		"document does not validate":       extractHeader + "inputs:\n  port:\n    type: string\n    must: size(this) >\n" + extractSteps,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for line := range strings.Split(src, "\n") {
				assert.Empty(t, extractActionsAt(t, src, line), "line %d", line)
			}
		})
	}
}

func TestExtractTypeNeedsTheRequestOnTheDeclaration(t *testing.T) {
	t.Parallel()

	// Different rules are different groups, each offered where its own lines are.
	other := extractActionsAt(t, extractBlockSource, lineOf(t, extractBlockSource, "other:"))
	require.Len(t, other, 1)
	got := applyExtract(t, extractBlockSource, other[0])
	assert.Contains(t, got, "  Other:\n    type: string\n    must: size(this) > 5\n")
	assert.Equal(t, 4, strings.Count(got, "must: size(this) > 2"), "the other group is untouched")

	// Not on an input, output or field: nothing, including on a step.
	assert.Empty(t, extractActionsAt(t, extractBlockSource, lineOf(t, extractBlockSource, "message: hi")))
	assert.Empty(t, extractActionsAt(t, extractBlockSource, 0))
}

func TestExtractTypeKeepsCRLFLineEndings(t *testing.T) {
	t.Parallel()

	src := strings.ReplaceAll(extractHeader+"inputs:\n  port:\n    type: int\n    must: this > 0\n"+extractSteps, "\n", "\r\n")
	actions := extractActionsAt(t, src, lineOf(t, src, "port:"))
	require.Len(t, actions, 1)
	got := applyExtract(t, src, actions[0])
	assert.NotContains(t, strings.ReplaceAll(got, "\r\n", ""), "\n")
	assert.Contains(t, got, "types:\r\n  Port:\r\n    type: int\r\n    must: this > 0\r\n")
}

func TestExtractTypeRefusesPastTheOccurrenceBound(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString(extractHeader + "inputs:\n")
	for i := range maxExtractOccurrences + 1 {
		b.WriteString("  f" + strings.Repeat("a", i) + ":\n    type: string\n    must: size(this) > 2\n")
	}
	b.WriteString(extractSteps)
	assert.Empty(t, extractActionsAt(t, b.String(), 3))
}

func TestExtractTypeHonorsTheKindFilter(t *testing.T) {
	t.Parallel()

	const uri = "file:///extract.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, extractBlockSource)
	for _, a := range c.codeAction(uri, wholeOf(extractBlockSource), []lsp.CodeActionKind{codeActionKindSourceFixAll}, nil) {
		assert.NotContains(t, a.Title, "Extract type")
	}
}

// manySites declares n inputs, each with a rule of its own.
func manySites(n int, tail string) string {
	var b strings.Builder
	b.WriteString(extractHeader + "inputs:\n")
	for i := range n {
		fmt.Fprintf(&b, "  in%c%c:\n    type: string\n    must: size(this) > %d\n", 'a'+i/26, 'a'+i%26, i)
	}
	b.WriteString(tail)
	return b.String()
}

func TestExtractTypeParsesTheOriginalOnceAndRefusesAnInvalidOne(t *testing.T) {
	t.Parallel()

	src := manySites(200, extractSteps)
	doc := newDocument("file:///extract.yaml", 0, src, nil)
	parses := 0
	actions := extractTypeActionsWith(doc, codeActionParams{Range: wholeOf(src)}, func(data []byte, path string) (*v1.Workflow, *flowfile.Positions, flowfile.Diagnostics, error) {
		parses++
		return nil, nil, nil, errors.New("does not compile")
	})
	assert.Empty(t, actions)
	assert.Equal(t, 1, parses, "an original that does not compile stops before any candidate is parsed")
}

func TestExtractTypeBoundsVerificationAttempts(t *testing.T) {
	t.Parallel()

	src := manySites(200, extractSteps)
	doc := newDocument("file:///extract.yaml", 0, src, nil)
	parses := 0
	actions := extractTypeActionsWith(doc, codeActionParams{Range: wholeOf(src)}, func(data []byte, path string) (*v1.Workflow, *flowfile.Positions, flowfile.Diagnostics, error) {
		parses++
		if parses > 1 {
			return nil, nil, nil, errors.New("candidate rejected")
		}
		return flowfile.ParseAndValidateSourceAt(data, path)
	})
	assert.Empty(t, actions)
	assert.Equal(t, 1+maxExtractVerifications, parses, "failed attempts count against the bound")
}

func TestExtractTypeInAModuleWithNoTrailingNewline(t *testing.T) {
	t.Parallel()

	src := "edition: " + flowfile.CurrentEdition + "\nname: ids\ntypes:\n  Rec:\n    fields:\n      slug:\n        type: string\n        must: size(this) > 2"
	actions := extractActionsAt(t, src, lineOf(t, src, "slug:"))
	require.Len(t, actions, 1)
	edits := actions[0].Edit.Changes["file:///extract.yaml"]
	ix := newLineIndex(src)
	for _, e := range edits {
		assert.Less(t, e.Range.End.Line, ix.lineCount(), "%v is inside the document", e.Range)
		assert.LessOrEqual(t, e.Range.End.Character, utf16Len(ix.line(e.Range.End.Line)), "%v is inside its line", e.Range)
	}
	assert.Contains(t, applyExtract(t, src, actions[0]), "  Slug:\n    type: string\n")
}

func TestExtractTypeRefusesAFileThatAlreadyFailsItsOwnRule(t *testing.T) {
	t.Parallel()

	src := extractHeader + "inputs:\n  port:\n    type: string\n    must: size(this) > 2\n    default: a\n" + extractSteps
	doc := newDocument("file:///extract.yaml", 0, src, nil)
	_, _, ds, err := flowfile.ParseAndValidateSourceAt([]byte(src), "")
	require.NoError(t, err)
	require.NotEmpty(t, ds, "the fixture is invalid: its default breaks its rule")
	assert.Empty(t, extractTypeActions(doc, codeActionParams{Range: wholeOf(src)}))
}
