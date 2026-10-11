package sdk

import (
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// listOf wraps a literal in a one-element list.
func listOf(inner *expr.Value) *expr.Value {
	return &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: []*expr.Value{inner}}}}
}

// TestLiteralRefusesAChainPastTheDepthCap holds that an acyclic literal nested
// far past maxLiteralDepth is answered with an error value, not walked until the
// stack gives out, and that the answer marshals.
func TestLiteralRefusesAChainPastTheDepthCap(t *testing.T) {
	chain := &expr.Value{Kind: &expr.Value_NullValue{}}
	for range 100_000 {
		chain = listOf(chain)
	}

	got := Literal(chain)
	requireErrorLiteral(t, chain, got)
	if _, err := proto.Marshal(got); err != nil {
		t.Fatal(err)
	}

	// Just inside the cap still passes through as written, and survives the wire.
	inside := &expr.Value{Kind: &expr.Value_NullValue{}}
	for range maxLiteralDepth - 1 {
		inside = listOf(inside)
	}
	got = Literal(inside)
	if got != inside {
		t.Fatal("a chain inside the cap was not returned as it was")
	}
	raw, err := proto.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(raw, &expr.Value{}); err != nil {
		t.Fatalf("a chain inside the cap does not unmarshal: %v", err)
	}

	// One level more is refused.
	requireErrorLiteral(t, inside, Literal(listOf(listOf(inside))))
}

// TestLiteralRefusesDepthBuiltFromSharedNodes holds that the depth cap counts
// what a node shared by many parents adds: segments that each end in the
// previous segment's top, with a root listing every top, are linear to walk and
// would still marshal to a tree far deeper than the cap.
func TestLiteralRefusesDepthBuiltFromSharedNodes(t *testing.T) {
	const segments = 3
	top := &expr.Value{Kind: &expr.Value_NullValue{}}
	tops := make([]*expr.Value, 0, segments)
	for range segments {
		for range maxLiteralDepth - 2 {
			top = listOf(top)
		}
		tops = append(tops, top)
	}
	root := &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: tops}}}

	got := Literal(root)
	requireErrorLiteral(t, root, got)
	if _, err := proto.Marshal(got); err != nil {
		t.Fatal(err)
	}
}
