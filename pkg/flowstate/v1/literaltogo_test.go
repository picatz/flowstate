package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// TestLiteralToGoStringKeyedMap is the direction that always worked: a map
// whose keys are strings round-trips to a map[string]any with every entry
// present.
func TestLiteralToGoStringKeyedMap(t *testing.T) {
	m := &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
		Entries: []*expr.MapValue_Entry{
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
	if object["a"] != "one" || object["b"] != "two" || len(object) != 2 {
		t.Fatalf("LiteralToGo returned %#v, want {a:one, b:two}", object)
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

func str(s string) *expr.Value {
	return &expr.Value{Kind: &expr.Value_StringValue{StringValue: s}}
}
