package flowstatev1_test

import (
	"encoding/base64"
	"google.golang.org/protobuf/proto"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/anypb"
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

// TestAnOutputMayDeclareADataKind pins that a workflow built in code is admitted
// with a timestamp, duration or bytes output: the run document has a plain-JSON
// form for each (see [v1.LiteralToGo]), which is what kept them off an output.
func TestAnOutputMayDeclareADataKind(t *testing.T) {
	t.Parallel()

	for _, typ := range []v1.InputDeclaration_Type{
		v1.InputDeclaration_TYPE_TIMESTAMP,
		v1.InputDeclaration_TYPE_DURATION,
		v1.InputDeclaration_TYPE_BYTES,
	} {
		wf := &v1.Workflow{
			Name:            "out",
			Profile:         v1.CurrentProfile,
			DeclaredOutputs: []*v1.OutputDeclaration{{Name: "at", Type: typ}},
		}
		_, err := v1.BindRunInputs(wf, nil)
		require.NoError(t, err, typ.String())
	}
}

// TestASubmittedPackedValueIsHeldToTheSameRangeAsText pins that a value that
// arrives already packed is validated, not trusted for its type URL.
func TestASubmittedPackedValueIsHeldToTheSameRangeAsText(t *testing.T) {
	t.Parallel()

	bad := func(url string, payload []byte) *expr.Value {
		return &expr.Value{Kind: &expr.Value_ObjectValue{ObjectValue: &anypb.Any{TypeUrl: url, Value: payload}}}
	}
	huge, err := proto.Marshal(&timestamppb.Timestamp{Seconds: math.MaxInt64})
	require.NoError(t, err)
	hugeSpan, err := proto.Marshal(&durationpb.Duration{Seconds: math.MaxInt64})
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		typ v1.InputDeclaration_Type
		val *expr.Value
	}{
		"garbage timestamp":     {v1.InputDeclaration_TYPE_TIMESTAMP, bad("type.googleapis.com/google.protobuf.Timestamp", []byte{0xff, 0xff, 0xff})},
		"out of range stamp":    {v1.InputDeclaration_TYPE_TIMESTAMP, bad("type.googleapis.com/google.protobuf.Timestamp", huge)},
		"garbage duration":      {v1.InputDeclaration_TYPE_DURATION, bad("type.googleapis.com/google.protobuf.Duration", []byte{0xff, 0xff, 0xff})},
		"out of range duration": {v1.InputDeclaration_TYPE_DURATION, bad("type.googleapis.com/google.protobuf.Duration", hugeSpan)},
	} {
		_, err := v1.NormalizeDataKind(tc.typ, tc.val)
		require.Error(t, err, name)
	}
}

func timestampsOf(t *testing.T, lit *expr.Value) []string {
	t.Helper()

	var out []string
	for _, element := range lit.GetListValue().GetValues() {
		text, err := v1.LiteralToGo(element)
		require.NoError(t, err)
		out = append(out, text.(string))
	}

	return out
}

// TestDataKindsInsideAListAndAMapAreBoundAsTheValueCELReads holds the walk below the
// top of a value: a list(timestamp) and a map(string, duration) hold the CEL kinds,
// not the text they arrived as, and a bad element is refused with its path and
// without its text.
func TestDataKindsInsideAListAndAMapAreBoundAsTheValueCELReads(t *testing.T) {
	t.Parallel()

	stamps := &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_TIMESTAMP}}}}
	spans := &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_DURATION}}}}}

	list := v1.NewLiteralList("2026-01-01T00:00:00Z", "2026-06-01T12:00:00+02:00").GetLiteral()
	got := v1.NormalizeWireValue(nil, stamps, list)
	require.NotSame(t, list, got, "text must be replaced")
	assert.Equal(t, []string{"2026-01-01T00:00:00Z", "2026-06-01T10:00:00Z"}, timestampsOf(t, got))

	// Idempotent, and the identity when there is nothing to turn.
	assert.Same(t, got, v1.NormalizeWireValue(nil, stamps, got))
	plain := v1.NewLiteralList("a", "b").GetLiteral()
	assert.Same(t, plain, v1.NormalizeWireValue(nil, &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}}}, plain))

	m := v1.NewLiteralMap(map[string]any{"short": "30s", "long": "90m"}).GetLiteral()
	normalized := v1.NormalizeWireValue(nil, spans, m)
	texts := map[string]string{}
	for _, entry := range normalized.GetMapValue().GetEntries() {
		text, err := v1.LiteralToGo(entry.GetValue())
		require.NoError(t, err)
		texts[entry.GetKey().GetStringValue()] = text.(string)
	}
	assert.Equal(t, map[string]string{"short": "30s", "long": "1h30m0s"}, texts)

	declaration := &v1.InputDeclaration{Name: "at", Type: v1.InputDeclaration_TYPE_LIST, ValueType: stamps}
	err := v1.CheckInputValue("at", declaration, v1.NewLiteralList("2026-01-01T00:00:00Z", "tomorrow"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not an RFC 3339 timestamp")
	assert.Contains(t, err.Error(), "[1]")
	assert.NotContains(t, err.Error(), "tomorrow")
}

// TestATimestampAndADurationAreAPlainValueAtTheEdge is the run-document half of
// #1436: LiteralToGo answers the strings a caller would submit, so `-o json`, an
// embedder and an http body carry them and not a tagged Any.
func TestATimestampAndADurationAreAPlainValueAtTheEdge(t *testing.T) {
	t.Parallel()

	stamp, err := v1.NormalizeDataKind(v1.InputDeclaration_TYPE_TIMESTAMP, v1.NewLiteral("2026-01-01T00:00:00Z").GetLiteral())
	require.NoError(t, err)
	got, err := v1.LiteralToGo(stamp)
	require.NoError(t, err)
	assert.Equal(t, "2026-01-01T00:00:00Z", got)

	span, err := v1.NormalizeDataKind(v1.InputDeclaration_TYPE_DURATION, v1.NewLiteral("5400s").GetLiteral())
	require.NoError(t, err)
	got, err = v1.LiteralToGo(span)
	require.NoError(t, err)
	assert.Equal(t, "1h30m0s", got)

	// Any other packed message still has no plain form.
	other, err := anypb.New(durationpb.New(0))
	require.NoError(t, err)
	other.TypeUrl = "type.googleapis.com/example.Other"
	_, err = v1.LiteralToGo(&expr.Value{Kind: &expr.Value_ObjectValue{ObjectValue: other}})
	require.Error(t, err)
}

// TestAnEmbedderBuildsATimestampAndADurationFromGoValues is the Go half of #1436:
// NewValue answers a time.Time and a time.Duration with the values a run holds, so
// a plugin's output or an embedder's input is the CEL kind and not an error value.
func TestAnEmbedderBuildsATimestampAndADurationFromGoValues(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := v1.LiteralToGo(v1.NewValue(at).GetLiteral())
	require.NoError(t, err)
	assert.Equal(t, "2026-01-01T00:00:00Z", got)

	got, err = v1.LiteralToGo(v1.NewValue(90 * time.Minute).GetLiteral())
	require.NoError(t, err)
	assert.Equal(t, "1h30m0s", got)

	// And they are the kinds a declaration accepts, so a built input binds.
	_, err = v1.NormalizeDataKind(v1.InputDeclaration_TYPE_TIMESTAMP, v1.NewValue(at).GetLiteral())
	require.NoError(t, err)
	_, err = v1.NormalizeDataKind(v1.InputDeclaration_TYPE_DURATION, v1.NewValue(at).GetLiteral())
	require.Error(t, err, "a timestamp is not a duration")
}
