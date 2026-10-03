package flowfile_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

func TestParseTypeAcceptsTheSpelling(t *testing.T) {
	t.Parallel()

	for src, want := range map[string]string{
		"string":                 "string",
		"timestamp":              "timestamp",
		"duration":               "duration",
		"bytes":                  "bytes",
		"double":                 "double",
		"dyn":                    "dyn",
		"list(dyn)":              "list(dyn)",
		"list(list(bytes))":      "list(list(bytes))",
		"map(string, list(int))": "map(string, list(int))",
		"map(string,int)":        "map(string, int)",
	} {
		got, err := flowfile.ParseType(src)
		require.NoError(t, err, src)
		printed, err := flowfile.FormatType(got)
		require.NoError(t, err, src)
		require.Equal(t, want, printed, src)
	}
}

func TestParseTypeRefusesWithAColumn(t *testing.T) {
	t.Parallel()

	for src, want := range map[string]string{
		"list(1)":           "1:5",
		"foo":               "foo",
		"map(int, string)":  "keys are strings",
		"list":              "list",
		"float":             "float",
		"":                  "empty",
		"list(string) == 1": "",
	} {
		_, err := flowfile.ParseType(src)
		require.Error(t, err, src)
		require.Contains(t, err.Error(), want, src)
	}
}

// TestATypeValueNeverEntersARunEnvironment holds the rule that keeps the
// spelling honest: `list(string)` is declared in the type environment and
// nowhere else, so a run expression using it is refused rather than false.
func TestATypeValueNeverEntersARunEnvironment(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: t
inputs:
  tags:
    type: list
steps:
  - id: s
    value: ${type(inputs.tags) == list(string)}
`
	diags, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, diags)
	require.Contains(t, fmt.Sprint(diags), "list")
}

func randomType(r *rand.Rand, depth int) *v1.Type {
	if depth > 0 {
		switch r.IntN(4) {
		case 0:
			return &v1.Type{Kind: &v1.Type_List{List: randomType(r, depth-1)}}
		case 1:
			return &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: randomType(r, depth-1)}}}
		}
	}
	if r.IntN(8) == 0 {
		return &v1.Type{Kind: &v1.Type_Dyn{Dyn: true}}
	}
	return &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_Scalar(1 + r.IntN(8))}}
}

func TestFormatThenParseIsTheIdentity(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(1640, 1))
	for range 500 {
		want := randomType(r, 4)
		src, err := flowfile.FormatType(want)
		require.NoError(t, err)
		got, err := flowfile.ParseType(src)
		require.NoError(t, err, src)
		require.True(t, proto.Equal(want, got), "%s", src)
	}
}

func TestFormatTypeRefusesWhatHasNoSpelling(t *testing.T) {
	t.Parallel()

	for _, bad := range []*v1.Type{
		{Kind: &v1.Type_Enum{Enum: true}},
		{Kind: &v1.Type_Message{Message: "a.B"}},
		{},
	} {
		_, err := flowfile.FormatType(bad)
		require.Error(t, err)
	}
}

// TestSpellingMatchesCELsOwnPrinter records, per kind, where the house spelling
// equals cel-go's Type.String() and the two deliberate translations.
func TestSpellingMatchesCELsOwnPrinter(t *testing.T) {
	t.Parallel()

	translated := map[string]string{
		"google.protobuf.Timestamp": "timestamp",
		"google.protobuf.Duration":  "duration",
	}
	for src, celType := range map[string]*types.Type{
		"string":                 types.StringType,
		"int":                    types.IntType,
		"double":                 types.DoubleType,
		"bool":                   types.BoolType,
		"bytes":                  types.BytesType,
		"null_type":              types.NullType,
		"timestamp":              types.TimestampType,
		"duration":               types.DurationType,
		"list(string)":           types.NewListType(types.StringType),
		"map(string, list(int))": types.NewMapType(types.StringType, types.NewListType(types.IntType)),
	} {
		got, err := flowfile.ParseType(src)
		require.NoError(t, err, src)
		printed, err := flowfile.FormatType(got)
		require.NoError(t, err)
		celPrinted := celType.String()
		if house, ok := translated[celPrinted]; ok {
			celPrinted = house
		}
		require.Equal(t, celPrinted, printed, fmt.Sprint(src))
	}
}
