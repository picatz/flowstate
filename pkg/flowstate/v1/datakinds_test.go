package flowstatev1_test

import (
	"encoding/base64"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// takesData is a workflow declaring one input of the given data kind.
func takesData(t v1.InputDeclaration_Type, must string) *v1.Workflow {
	declaration := &v1.InputDeclaration{Name: "x", Type: t, Required: true}
	if must != "" {
		declaration.Must = &must
	}

	return &v1.Workflow{
		Name:           "takes-data",
		Profile:        v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{declaration},
		Steps: []*v1.Node{{
			Id:   "a",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hello")}}},
		}},
	}
}

// TestADataKindIsBoundAsTheValueCELReads is #1436's binder claim: text goes in,
// and what comes out is the timestamp, duration or bytes an expression sees.
func TestADataKindIsBoundAsTheValueCELReads(t *testing.T) {
	t.Parallel()

	bound, err := v1.BindRunInputs(takesData(v1.InputDeclaration_TYPE_TIMESTAMP, ""),
		map[string]*v1.Value{"x": v1.NewLiteral("2026-01-01T00:00:00Z")})
	require.NoError(t, err)

	var stamp timestamppb.Timestamp
	require.NoError(t, bound["x"].GetLiteral().GetObjectValue().UnmarshalTo(&stamp))
	assert.Equal(t, int64(1767225600), stamp.GetSeconds())

	bound, err = v1.BindRunInputs(takesData(v1.InputDeclaration_TYPE_DURATION, ""),
		map[string]*v1.Value{"x": v1.NewLiteral("1h30m")})
	require.NoError(t, err)

	var span durationpb.Duration
	require.NoError(t, bound["x"].GetLiteral().GetObjectValue().UnmarshalTo(&span))
	assert.Equal(t, int64(5400), span.GetSeconds())

	bound, err = v1.BindRunInputs(takesData(v1.InputDeclaration_TYPE_BYTES, ""),
		map[string]*v1.Value{"x": v1.NewLiteral("aGk=")})
	require.NoError(t, err)
	assert.Equal(t, []byte("hi"), bound["x"].GetLiteral().GetBytesValue())

	// Binding what was already bound changes nothing: a call boundary and a
	// Temporal worker both hand the binder a value that is already normalized.
	again, err := v1.BindRunInputs(takesData(v1.InputDeclaration_TYPE_BYTES, ""), bound)
	require.NoError(t, err)
	assert.Equal(t, []byte("hi"), again["x"].GetLiteral().GetBytesValue())
}

// TestADataKindRefusesTextThatIsNotOne is the negative direction, with the
// boundaries each parser has: the wrong kind of value, an out-of-range year, an
// unpadded base64 text, and a bytes input past its bound. None of the refusals
// repeats what was sent, because an input may be `sensitive:`.
func TestADataKindRefusesTextThatIsNotOne(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		kind  v1.InputDeclaration_Type
		value *v1.Value
		says  string
	}{
		"words, not a timestamp":      {v1.InputDeclaration_TYPE_TIMESTAMP, v1.NewLiteral("next tuesday"), "RFC 3339"},
		"a date with no time":         {v1.InputDeclaration_TYPE_TIMESTAMP, v1.NewLiteral("2026-01-01"), "RFC 3339"},
		"a number, not a timestamp":   {v1.InputDeclaration_TYPE_TIMESTAMP, v1.NewLiteral(int64(5)), "RFC 3339"},
		"a bare number of seconds":    {v1.InputDeclaration_TYPE_DURATION, v1.NewLiteral("90"), "a duration such as"},
		"words, not a duration":       {v1.InputDeclaration_TYPE_DURATION, v1.NewLiteral("a while"), "a duration such as"},
		"not base64":                  {v1.InputDeclaration_TYPE_BYTES, v1.NewLiteral("not base64!"), "base64"},
		"base64 without its padding":  {v1.InputDeclaration_TYPE_BYTES, v1.NewLiteral("aGk"), "base64"},
		"bytes past the stated bound": {v1.InputDeclaration_TYPE_BYTES, v1.NewLiteral(strings.Repeat("QUFB", 400_000)), "decodes to more than"},
	} {
		_, err := v1.BindRunInputs(takesData(test.kind, ""), map[string]*v1.Value{"x": test.value})
		require.ErrorContains(t, err, test.says, name)
		require.NotContains(t, err.Error(), "tuesday", name)
	}
}

// TestADataKindIsHeldToItsMust shows `must:` reads `this` as the CEL type and
// that the refusal quotes the value in its own spelling.
func TestADataKindIsHeldToItsMust(t *testing.T) {
	t.Parallel()

	wf := takesData(v1.InputDeclaration_TYPE_TIMESTAMP, `this > timestamp("2025-01-01T00:00:00Z")`)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"x": v1.NewLiteral("2026-01-01T00:00:00Z")})
	require.NoError(t, err)

	_, err = v1.BindRunInputs(wf, map[string]*v1.Value{"x": v1.NewLiteral("2024-01-01T00:00:00Z")})
	require.ErrorContains(t, err, "got 2024-01-01T00:00:00Z")

	wf = takesData(v1.InputDeclaration_TYPE_BYTES, `size(this) == 2`)
	_, err = v1.BindRunInputs(wf, map[string]*v1.Value{"x": v1.NewLiteral(base64.StdEncoding.EncodeToString([]byte("hi")))})
	require.NoError(t, err)
}

// TestADataKindRefusesConstraintsThatAreNotItsOwn pins that a string constraint
// on a timestamp is a declaration refusal rather than a no-op.
func TestADataKindRefusesConstraintsThatAreNotItsOwn(t *testing.T) {
	t.Parallel()

	wf := takesData(v1.InputDeclaration_TYPE_TIMESTAMP, "")
	wf.DeclaredInputs[0].MinLen = new(uint64)

	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"x": v1.NewLiteral("2026-01-01T00:00:00Z")})
	require.ErrorContains(t, err, "those apply only to a string input")
}

// TestADataKindTypeTextIsItsOwnName pins what a hover or diagnostic shows.
func TestADataKindTypeTextIsItsOwnName(t *testing.T) {
	t.Parallel()

	for kind, want := range map[v1.InputDeclaration_Type]string{
		v1.InputDeclaration_TYPE_TIMESTAMP: "timestamp",
		v1.InputDeclaration_TYPE_DURATION:  "duration",
		v1.InputDeclaration_TYPE_BYTES:     "bytes",
	} {
		assert.Equal(t, want, (&v1.InputDeclaration{Type: kind}).TypeText())
		assert.True(t, v1.IsDataKind(kind))
	}
	assert.False(t, v1.IsDataKind(v1.InputDeclaration_TYPE_STRING))
}

// TestASensitiveDataKindIsNotQuotedInAMustRefusal pins that the normalized
// spelling of a sensitive input never reaches a diagnostic.
func TestASensitiveDataKindIsNotQuotedInAMustRefusal(t *testing.T) {
	t.Parallel()

	decl := &v1.InputDeclaration{
		Name:      "seal",
		Type:      v1.InputDeclaration_TYPE_BYTES,
		Must:      proto.String("size(this) > 100"),
		Sensitive: true,
	}
	err := v1.CheckInputConstraints(v1.CurrentProfile, "seal", decl, v1.NewLiteral("aGk="))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "aGk=")
}
