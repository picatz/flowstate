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

// mapOf wraps a literal in a one-entry map, as the value or as the key.
func mapOf(inner *expr.Value, asKey bool) *expr.Value {
	entry := &expr.MapValue_Entry{Key: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "k"}}, Value: inner}
	if asKey {
		entry = &expr.MapValue_Entry{Key: inner, Value: &expr.Value{Kind: &expr.Value_NullValue{}}}
	}
	return &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: []*expr.MapValue_Entry{entry}}}}
}

// TestLiteralMapChainAtTheCapSurvivesTheWire holds that a map level, which is
// three messages, still unmarshals at the cap, through values and through keys.
func TestLiteralMapChainAtTheCapSurvivesTheWire(t *testing.T) {
	for name, asKey := range map[string]bool{"values": false, "keys": true} {
		chain := &expr.Value{Kind: &expr.Value_NullValue{}}
		for range maxLiteralDepth - 1 {
			chain = mapOf(chain, asKey)
		}
		got := Literal(chain)
		if got != chain {
			t.Fatalf("%s: a map chain inside the cap was not returned as it was", name)
		}
		raw, err := proto.Marshal(got)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := proto.Unmarshal(raw, &expr.Value{}); err != nil {
			t.Errorf("%s: a map chain at the cap does not unmarshal: %v", name, err)
		}

		requireErrorLiteral(t, chain, Literal(mapOf(mapOf(chain, asKey), asKey)))
	}
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
