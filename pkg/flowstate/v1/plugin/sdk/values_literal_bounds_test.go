package sdk

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// TestLiteralErrorValueCarriesNoErrorText holds that an error handed to Literal
// is reported by its Go type alone: its text, a secret or invalid UTF-8
// included, never reaches the output that is written to workflow history.
func TestLiteralErrorValueCarriesNoErrorText(t *testing.T) {
	const secret = "hunter2-token-AKIAIOSFODNN7EXAMPLE"
	failure := errors.New("dial https://user:" + secret + "@db.internal: \xff\xfe refused")

	for name, input := range map[string]any{
		"error":   failure,
		"nested":  map[string]any{"k": []any{failure}},
		"wrapped": fmt.Errorf("upstream: %w", failure),
	} {
		got := Literal(input)
		raw, err := proto.Marshal(got)
		if err != nil {
			t.Fatalf("%s: the error value does not marshal: %v", name, err)
		}
		for _, leaked := range []string{secret, "db.internal", "\xff", "refused"} {
			if strings.Contains(string(raw), leaked) {
				t.Errorf("%s: the output carries %q", name, leaked)
			}
		}
	}
	requireErrorLiteral(t, failure, Literal(failure))
}

// fuzzCyclicLiteral is a literal map that contains itself twice over.
func fuzzCyclicLiteral() *expr.Value {
	cycle := &expr.MapValue{}
	self := &expr.Value{Kind: &expr.Value_MapValue{MapValue: cycle}}
	key := &expr.Value{Kind: &expr.Value_StringValue{StringValue: "k"}}
	cycle.Entries = []*expr.MapValue_Entry{{Key: key, Value: self}, {Key: key, Value: self}}
	return self
}

// fuzzNilEntryLiteral is a literal holding a nil map entry and a nil element.
func fuzzNilEntryLiteral() *expr.Value {
	return &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{
		nil,
		{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{nil}}}},
	}}}}
}

// TestLiteralWalkIsBoundedAndNilSafe holds that a caller-supplied literal that is
// cyclic, or holds nil entries, is answered quickly and without a panic.
func TestLiteralWalkIsBoundedAndNilSafe(t *testing.T) {
	cycleList := &expr.ListValue{}
	cycleList.Values = []*expr.Value{{Kind: &expr.Value_ListValue{ListValue: cycleList}}}

	cycleMap := &expr.MapValue{}
	self := &expr.Value{Kind: &expr.Value_MapValue{MapValue: cycleMap}}
	key := &expr.Value{Kind: &expr.Value_StringValue{StringValue: "k"}}
	cycleMap.Entries = []*expr.MapValue_Entry{{Key: key, Value: self}, {Key: key, Value: self}}

	start := time.Now()
	for name, input := range map[string]any{
		"cyclic list":      &expr.Value{Kind: &expr.Value_ListValue{ListValue: cycleList}},
		"cyclic wide map":  self,
		"cycle in a map":   map[string]any{"x": self},
		"nil map entry":    &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{nil, {Key: key}}}}},
		"nil list element": &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{nil}}}},
		"nil list":         &expr.Value{Kind: &expr.Value_ListValue{}},
	} {
		if got := Literal(input); got == nil {
			t.Errorf("%s: Literal returned nil", name)
		}
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the walk took %s", elapsed)
	}
}
