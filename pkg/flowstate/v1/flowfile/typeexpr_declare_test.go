package flowfile_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

func typedInputFile(typeText, defaultLine, valueExpr string) string {
	return fmt.Sprintf(`edition: v2026.4
name: elems
inputs:
  ids:
    type: %s
%ssteps:
  - id: up
    value: %s
`, typeText, defaultLine, valueExpr)
}

// TestATypedListInputReachesTheChecker is the issue's reproduction: with a
// spelling for "list of what", the file is a validate refusal instead of a
// run-time failure.
func TestATypedListInputReachesTheChecker(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(typedInputFile(
		`list(string)`, "", "${inputs.ids.map(i, i.lowerAscii())}")))
	require.NoError(t, err)
	require.Empty(t, diags, "a list of strings may be lowercased")

	diags, err = flowfile.ValidateSource([]byte(typedInputFile(
		`list(int)`, "", "${inputs.ids.map(i, i.lowerAscii())}")))
	require.NoError(t, err)
	require.NotEmpty(t, diags)
	require.Contains(t, fmt.Sprint(diags), "lowerAscii")
}

func TestATypedListDefaultIsHeldToItsType(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(typedInputFile(
		`list(string)`, "    default: [1, 2, 3]\n", `${"ok"}`)))
	require.NoError(t, err)
	require.NotEmpty(t, diags)
	require.Contains(t, fmt.Sprint(diags), "default")

	diags, err = flowfile.ValidateSource([]byte(typedInputFile(
		`list(string)`, "    default: [a, b]\n", `${"ok"}`)))
	require.NoError(t, err)
	require.Empty(t, diags)
}

func TestTypedDeclarationsCompileToBothRepresentations(t *testing.T) {
	t.Parallel()

	for typeText, want := range map[string]v1.InputDeclaration_Type{
		"list(string)":           v1.InputDeclaration_TYPE_LIST,
		"map(string, list(int))": v1.InputDeclaration_TYPE_STRUCT,
		"double":                 v1.InputDeclaration_TYPE_FLOAT,
	} {
		wf, _, err := flowfile.Parse([]byte(typedInputFile(typeText, "", `${"ok"}`)))
		require.NoError(t, err, typeText)
		in := wf.GetDeclaredInputs()[0]
		require.Equal(t, want, in.GetType(), typeText)
		printed, err := flowfile.FormatType(in.GetValueType())
		require.NoError(t, err)
		require.Equal(t, typeText, printed)
		require.NoError(t, v1.Validate(in), typeText)
	}

	// The legacy words that survive the edition compile with no structural type.
	wf, _, err := flowfile.Parse([]byte(typedInputFile("string", "", `${"ok"}`)))
	require.NoError(t, err)
	require.Nil(t, wf.GetDeclaredInputs()[0].GetValueType())
}

// TestTheRetiredTypeWordsAreRefusedWithTheirReplacement holds the edition
// boundary: `list`, `struct` and `float` are second spellings of type
// expressions, so each is refused naming what it meant.
func TestTheRetiredTypeWordsAreRefusedWithTheirReplacement(t *testing.T) {
	t.Parallel()

	for word, replacement := range map[string]string{
		"list":   "list(dyn)",
		"struct": "map(string, dyn)",
		"float":  "double",
	} {
		_, _, err := flowfile.Parse([]byte(typedInputFile(word, "", `${"ok"}`)))
		require.ErrorContains(t, err, "write "+replacement, word)
	}
}

func TestADeclarationRefusesATypeWithNoLegacyProjection(t *testing.T) {
	t.Parallel()

	for _, typeText := range []string{"timestamp", "bytes", "duration", "dyn", "foo", "map(int, string)", "list(1)"} {
		_, _, err := flowfile.Parse([]byte(typedInputFile(typeText, "", `${"ok"}`)))
		require.Error(t, err, typeText)
	}
}

func TestATypedOutputCompiles(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(`edition: v2026.4
name: o
steps:
  - id: s
    value: [a]
outputs:
  names:
    type: list(string)
    value: ${steps.s.result}
`))
	require.NoError(t, err)
	out := wf.GetDeclaredOutputs()[0]
	require.Equal(t, v1.InputDeclaration_TYPE_LIST, v1.InputDeclaration_Type(out.GetType()))
	require.NotNil(t, out.GetValueType())
}

func TestATypedDeclarationSurvivesMarshal(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(typedInputFile("map(string, list(int))", "", `${"ok"}`)))
	require.NoError(t, err)
	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	require.Contains(t, string(out), "type: map(string, list(int))")

	again, _, err := flowfile.Parse(out)
	require.NoError(t, err)
	require.True(t, proto.Equal(wf, again))
}

func TestADeclaredTypeNestedTooDeepIsRefusedAtCompile(t *testing.T) {
	t.Parallel()

	deep := "string"
	for range v1.MaxStructureDepth + 2 {
		deep = "list(" + deep + ")"
	}
	_, _, err := flowfile.Parse([]byte(typedInputFile(deep, "", `${"ok"}`)))
	require.Error(t, err)
}
