package sdk

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestLiteralPatchesEveryUnrepresentableElement holds that no element is left
// nil however many nodes the value holds: the pass has no budget to run out of.
func TestLiteralPatchesEveryUnrepresentableElement(t *testing.T) {
	const items = 100_000
	list := make([]any, items)
	for i := range list {
		list[i] = []any{make(chan int)}
	}

	got := Literal(list)
	elements := got.GetListValue().GetValues()
	if len(elements) != items {
		t.Fatalf("got %d elements, want %d", len(elements), items)
	}
	for i, element := range elements {
		inner := element.GetListValue().GetValues()
		if len(inner) != 1 || inner[0] == nil {
			t.Fatalf("element %d left an unrepresentable value nil: %v", i, element)
		}
		requireErrorLiteral(t, "chan", inner[0])
	}
	if _, err := proto.Marshal(got); err != nil {
		t.Fatal(err)
	}
}
