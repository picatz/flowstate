package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// TestLiteralToGoStringKeyedMap is the direction that always worked: a map
// whose keys are strings, including the empty string, round-trips to a
// map[string]any with every entry present.
func TestLiteralToGoStringKeyedMap(t *testing.T) {
	m := &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
		Entries: []*expr.MapValue_Entry{
			{Key: str(""), Value: str("empty")},
			{Key: str("a"), Value: str("one")},
			{Key: str("b"), Value: str("two")},
		},
	}}}

	got, err := v1.LiteralToGo(m)
	if err != nil {
		t.Fatalf("LiteralToGo returned error: %v", err)
	}
	object, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("LiteralToGo returned %T, want map[string]any", got)
	}
	if object[""] != "empty" || object["a"] != "one" || object["b"] != "two" || len(object) != 3 {
		t.Fatalf("LiteralToGo returned %#v, want {\"\":empty, a:one, b:two}", object)
	}
}

// TestLiteralToGoNonStringKeyFailsClosed is the negative direction, and the
// reason this function returns an error at all. A Go map[string]any cannot
// hold an integer, unsigned, or boolean CEL key; the old code read every one
// of them through GetStringValue, which returns "" for a non-string arm, so
// two int-keyed entries collapsed into a single object[""] and the function
// reported success. That silent corruption reached StepOutput as ok=true on a
// wrong value. Assert the function now refuses the conversion instead — and
// assert it by count, because a bug that collapses two entries into one is
// invisible to a test that checks a single entry.
func TestLiteralToGoNonStringKeyFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  *expr.Value
	}{
		{"int64", &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 1}}},
		{"uint64", &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: 1}}},
		{"bool", &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Two distinct non-string keys: the collapse only shows when a
			// second entry has somewhere to overwrite the first.
			second := &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 2}}
			if tc.name == "uint64" {
				second = &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: 2}}
			}
			if tc.name == "bool" {
				second = &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: false}}
			}
			m := &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
				Entries: []*expr.MapValue_Entry{
					{Key: tc.key, Value: str("one")},
					{Key: second, Value: str("two")},
				},
			}}}

			got, err := v1.LiteralToGo(m)
			if err == nil {
				t.Fatalf("LiteralToGo(%s-keyed map) = %#v, nil error; want an error refusing the non-string key", tc.name, got)
			}
			if got != nil {
				t.Fatalf("LiteralToGo(%s-keyed map) returned a non-nil value %#v alongside its error; a refused conversion must yield nil", tc.name, got)
			}
		})
	}
}

// TestNewValueLiteralToGoRoundTrip asserts that NewValue(LiteralToGo(v)) == v
// for every kind LiteralToGo can return, proving the two functions are inverses.
// The []byte case is the regression from #1442: before the fix, NewValue([]byte)
// fell through to reflection and produced a list of int64 values.
func TestNewValueLiteralToGoRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value *expr.Value
	}{
		{"null", &expr.Value{Kind: &expr.Value_NullValue{}}},
		{"bool", &expr.Value{Kind: &expr.Value_BoolValue{BoolValue: true}}},
		{"string", &expr.Value{Kind: &expr.Value_StringValue{StringValue: "hello"}}},
		{"int64", &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 42}}},
		// uint64 is intentionally asymmetric: NewValue(uint64) converts to
		// int64 because CEL represents all integers as int64. The value is
		// preserved; only the wire kind differs.
		{"uint64", &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: 99}}},
		{"double", &expr.Value{Kind: &expr.Value_DoubleValue{DoubleValue: 3.14}}},
		{"bytes", &expr.Value{Kind: &expr.Value_BytesValue{BytesValue: []byte("abc")}}},
		{"list", &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{
			Values: []*expr.Value{
				{Kind: &expr.Value_Int64Value{Int64Value: 1}},
				{Kind: &expr.Value_StringValue{StringValue: "two"}},
			},
		}}}},
		{"map", &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
			Entries: []*expr.MapValue_Entry{
				{Key: str("k"), Value: &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 7}}},
			},
		}}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			native, err := v1.LiteralToGo(tc.value)
			if err != nil {
				t.Fatalf("LiteralToGo: %v", err)
			}

			got := v1.NewValue(native)
			gotLiteral := got.GetLiteral()
			if gotLiteral == nil {
				t.Fatalf("NewValue(%T) did not produce a literal value: %v", native, got)
			}

			if tc.name == "uint64" {
				// uint64 round-trips through int64 by design; verify the
				// numeric value is preserved, not the wire kind.
				if gotLiteral.GetInt64Value() != int64(tc.value.GetUint64Value()) {
					t.Fatalf("uint64 value not preserved: got %d, want %d",
						gotLiteral.GetInt64Value(), tc.value.GetUint64Value())
				}
				return
			}
			if !proto.Equal(gotLiteral, tc.value) {
				t.Fatalf("round-trip mismatch for %s:\n  LiteralToGo → %T(%v)\n  NewValue    → %v\n  want          %v",
					tc.name, native, native, gotLiteral, tc.value)
			}
		})
	}
}

func str(s string) *expr.Value {
	return &expr.Value{Kind: &expr.Value_StringValue{StringValue: s}}
}
