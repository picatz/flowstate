package flowdebug

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAFailureIsTheSameOnlyWhereItsStepEnclosesTheLast pins what makes a
// failure's re-arrival the same failure: the step it now fails encloses the
// place it stopped. The same error failing a sibling, or a step elsewhere, is a
// failure of its own and stops again.
func TestAFailureIsTheSameOnlyWhereItsStepEnclosesTheLast(t *testing.T) {
	iteration := func(step string, index int) *v1.DebugSegment {
		return &v1.DebugSegment{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, StepId: step, Workflow: "root", Index: int32(index)}
	}
	call := &v1.DebugSegment{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL, StepId: "nested", Workflow: "root", Callee: "child"}

	boom := v1.NewDebugOccurrence("root", []*v1.DebugSegment{iteration("outer", 0)}, "boom", "value")
	inCallee := v1.NewDebugOccurrence("child", []*v1.DebugSegment{call}, "greet", "log")

	for _, tc := range []struct {
		name         string
		outer, inner *v1.DebugOccurrence
		want         bool
	}{
		{"the loop around it", v1.NewDebugOccurrence("root", nil, "outer", "for_each"), boom, true},
		{"the call around it", v1.NewDebugOccurrence("root", nil, "nested", "call"), inCallee, true},
		{"a sibling step", v1.NewDebugOccurrence("root", nil, "after", "value"), boom, false},
		{"another iteration's step", v1.NewDebugOccurrence("root", []*v1.DebugSegment{iteration("outer", 1)}, "boom", "value"), boom, false},
		{"itself", boom, boom, false},
		{"nothing before it", boom, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := encloses(tc.outer, tc.inner); got != tc.want {
				t.Fatalf("encloses(%s, %s) = %v, want %v", tc.outer.GetAddress(), tc.inner.GetAddress(), got, tc.want)
			}
		})
	}
}
