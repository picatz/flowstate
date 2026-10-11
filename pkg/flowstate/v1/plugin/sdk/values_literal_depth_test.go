package sdk

import (
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// TestLiteralRefusesAChainPastTheDepthCap holds that an acyclic literal nested
// far past maxLiteralDepth is answered with an error value, not walked until the
// stack gives out, and that the answer marshals.
func TestLiteralRefusesAChainPastTheDepthCap(t *testing.T) {
	chain := &expr.Value{Kind: &expr.Value_NullValue{}}
	for range 100_000 {
		chain = &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{chain}}}}
	}

	got := Literal(chain)
	requireErrorLiteral(t, chain, got)
	if _, err := proto.Marshal(got); err != nil {
		t.Fatal(err)
	}

	// Just inside the cap still passes through as written.
	inside := &expr.Value{Kind: &expr.Value_NullValue{}}
	for range maxLiteralDepth - 1 {
		inside = &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{inside}}}}
	}
	if got := Literal(inside); got != inside {
		t.Error("a chain inside the cap was not returned as it was")
	}
}
