package embed

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// stepOutputsFixture builds a [v1.Workflow_StepOutputs] with one step,
// "welcome", carrying the named outputs given, the same shape [RunLocal]
// itself returns, without having to compile and run a workflow just to read
// it back.
func stepOutputsFixture(named map[string]any) *v1.Workflow_StepOutputs {
	return &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"welcome": {
				NamedValues: v1.NewNamedValues(named),
			},
		},
	}
}

func TestStepOutput_Present(t *testing.T) {
	outputs := stepOutputsFixture(map[string]any{"greeting": "hello, world"})

	value, ok := StepOutput(outputs, "welcome", "greeting")
	if !ok {
		t.Fatal("StepOutput: ok = false, want true for a present output")
	}
	if value != "hello, world" {
		t.Fatalf("StepOutput: value = %v, want %q", value, "hello, world")
	}
}

func TestStepOutput_AbsentStep(t *testing.T) {
	outputs := stepOutputsFixture(map[string]any{"greeting": "hello, world"})

	value, ok := StepOutput(outputs, "does-not-exist", "greeting")
	if ok {
		t.Fatalf("StepOutput: ok = true, want false for an absent step (value=%v)", value)
	}
	if value != nil {
		t.Fatalf("StepOutput: value = %v, want nil for ok=false", value)
	}
}

func TestStepOutput_AbsentName(t *testing.T) {
	outputs := stepOutputsFixture(map[string]any{"greeting": "hello, world"})

	value, ok := StepOutput(outputs, "welcome", "does-not-exist")
	if ok {
		t.Fatalf("StepOutput: ok = true, want false for an absent output name (value=%v)", value)
	}
	if value != nil {
		t.Fatalf("StepOutput: value = %v, want nil for ok=false", value)
	}
}

func TestStepOutput_NilOutputs(t *testing.T) {
	value, ok := StepOutput(nil, "welcome", "greeting")
	if ok {
		t.Fatalf("StepOutput: ok = true, want false for nil outputs (value=%v)", value)
	}
}

func TestStepOutputString_Present(t *testing.T) {
	outputs := stepOutputsFixture(map[string]any{"greeting": "hello, world"})

	str, ok := StepOutputString(outputs, "welcome", "greeting")
	if !ok {
		t.Fatal("StepOutputString: ok = false, want true for a present string output")
	}
	if str != "hello, world" {
		t.Fatalf("StepOutputString: value = %q, want %q", str, "hello, world")
	}
}

func TestStepOutputString_WrongType(t *testing.T) {
	// count is an int64 by way of v1.NewNamedValues, not a string.
	outputs := stepOutputsFixture(map[string]any{"count": int64(3)})

	str, ok := StepOutputString(outputs, "welcome", "count")
	if ok {
		t.Fatalf("StepOutputString: ok = true, want false for a non-string output (value=%q)", str)
	}
	if str != "" {
		t.Fatalf("StepOutputString: value = %q, want \"\" for ok=false", str)
	}
}

func TestStepOutputString_AbsentStep(t *testing.T) {
	outputs := stepOutputsFixture(map[string]any{"greeting": "hello, world"})

	str, ok := StepOutputString(outputs, "does-not-exist", "greeting")
	if ok {
		t.Fatalf("StepOutputString: ok = true, want false for an absent step (value=%q)", str)
	}
}

// TestStepOutput_OKFalsePathBites proves the ok=false cases above actually
// exercise something: an accessor that always reports ok=true, the mutation
// this test is written to catch, fails TestStepOutput_AbsentStep and
// TestStepOutput_AbsentName immediately because they assert ok is false.
// This test documents that property rather than performing the mutation
// itself; running the two tests above against a deliberately broken
// alwaysOK variant is how that guarantee was checked by hand.
func TestStepOutput_OKFalsePathBites(t *testing.T) {
	alwaysOK := func(outputs *v1.Workflow_StepOutputs, step, name string) (any, bool) {
		value, _ := StepOutput(outputs, step, name)
		return value, true
	}

	outputs := stepOutputsFixture(map[string]any{"greeting": "hello, world"})

	if _, ok := alwaysOK(outputs, "does-not-exist", "greeting"); !ok {
		t.Fatal("test harness bug: alwaysOK must always report ok=true")
	}

	// The real accessor disagrees with the mutation on exactly the cases
	// that matter: an absent step and an absent name must report ok=false.
	if _, ok := StepOutput(outputs, "does-not-exist", "greeting"); ok {
		t.Fatal("StepOutput: ok = true for an absent step, the mutation this test guards against")
	}
	if _, ok := StepOutput(outputs, "welcome", "does-not-exist"); ok {
		t.Fatal("StepOutput: ok = true for an absent name, the mutation this test guards against")
	}
}
