package flowstatev1

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestOccurrenceTrackingCostsNothingWithoutADebugger pins the disabled-mode
// cost: a run nobody is debugging records no segments and allocates nothing
// for them, and its failure hook is a nil check.
func TestOccurrenceTrackingCostsNothingWithoutADebugger(t *testing.T) {
	ctx := contextWithExecutingWorkflow(context.Background(), "root", SensitiveValues{})

	allocs := testing.AllocsPerRun(100, func() {
		if contextWithSegment(ctx, DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, "loop", 3) != ctx {
			t.Fatal("a segment was recorded with no debugger installed")
		}
		if err := debuggerStepFailed(ctx, nil, nil, nil, false); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("disabled occurrence tracking allocated %v times per boundary", allocs)
	}

	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)
	if len(position.segments) != 0 {
		t.Fatalf("segments recorded without a debugger: %v", position.segments)
	}
}

type recordingDebugger struct{}

func (recordingDebugger) BeforeStep(context.Context, *Node, *Scope) error { return nil }

func TestOccurrenceTrackingRecordsNestingUnderADebugger(t *testing.T) {
	ctx := NewContextWithDebugger(contextWithExecutingWorkflow(context.Background(), "root", SensitiveValues{}), recordingDebugger{})
	ctx = contextWithSegment(ctx, DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, "pages", 2)
	ctx = contextWithExecutingCall(ctx, "fetch", "call", "child", SensitiveValues{})
	ctx = contextWithSegment(ctx, DebugSegmentKind_DEBUG_SEGMENT_KIND_BRANCH, "fan", 1)

	occurrence := ExecutingOccurrenceFromContext(ctx, &Node{Id: "get"})
	if got, want := occurrence.GetAddress(), "pages[2]/fetch(child)/fan#1/get"; got != want {
		t.Fatalf("address %q, want %q", got, want)
	}
	if got := occurrence.GetSite().GetWorkflow(); got != "child" {
		t.Fatalf("site workflow %q, want the callee", got)
	}
	if got := occurrence.GetSite().GetPath(); len(got) != 2 || got[0] != "fan" || got[1] != "get" {
		t.Fatalf("site path %v, want the callee-relative [fan get]", got)
	}
}

// TestAnOccurrenceStaysInsideTheSchemasBounds nests as deep as an occurrence
// may, under ids as long as a step id may be, and reads the result against the
// schema's own limits: the segments, the site's path, and the address.
func TestAnOccurrenceStaysInsideTheSchemasBounds(t *testing.T) {
	id := strings.Repeat("s", 256)
	segments := make([]*DebugSegment, 0, MaxDebugSegments)
	for range MaxDebugSegments {
		segments = append(segments, &DebugSegment{Kind: DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, StepId: id, Index: 9})
	}
	occurrence := NewDebugOccurrence("root", segments, "leaf", "log")

	if got := len(occurrence.GetSegments()); got > MaxDebugSegments {
		t.Fatalf("segments = %d, over the schema's %d", got, MaxDebugSegments)
	}
	if got := len(occurrence.GetSite().GetPath()); got > MaxDebugSegments+1 {
		t.Fatalf("site path = %d ids, over the schema's %d", got, MaxDebugSegments+1)
	}
	address := occurrence.GetAddress()
	if got := utf8.RuneCountInString(address); got > MaxDebugAddressRunes {
		t.Fatalf("address = %d runes, over the schema's %d", got, MaxDebugAddressRunes)
	}
	if !strings.HasSuffix(address, "/leaf") {
		t.Fatalf("a shortened address lost the step it names: ...%s", address[len(address)-32:])
	}

	ctx := NewContextWithDebugger(contextWithExecutingWorkflow(context.Background(), "root", SensitiveValues{}), recordingDebugger{})
	for range MaxDebugSegments + 4 {
		ctx = contextWithExecutingCall(ctx, "caller", "call", "callee", SensitiveValues{})
	}
	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)
	if got := len(position.segments); got > MaxDebugSegments {
		t.Fatalf("call segments = %d, over the schema's %d", got, MaxDebugSegments)
	}
}
