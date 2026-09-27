package flowstatev1

import (
	"context"
	"testing"
)

// TestOccurrenceTrackingCostsNothingWithoutADebugger pins the disabled-mode
// cost: a run nobody is debugging records no segments and allocates nothing
// for them, and its failure hook is a nil check.
func TestOccurrenceTrackingCostsNothingWithoutADebugger(t *testing.T) {
	ctx := contextWithExecutingWorkflow(context.Background(), "root")

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
	ctx := NewContextWithDebugger(contextWithExecutingWorkflow(context.Background(), "root"), recordingDebugger{})
	ctx = contextWithSegment(ctx, DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, "pages", 2)
	ctx = contextWithExecutingCall(ctx, "fetch", "call", "child")
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
