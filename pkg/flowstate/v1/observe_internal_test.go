package flowstatev1

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// panickingObserver is the embedder's mistake: an observer that fails on the
// fact it is handed.
type panickingObserver struct{ saw []string }

func (o *panickingObserver) StepFinished(id string, _ *Node_Outputs, _ error, _ bool) {
	o.saw = append(o.saw, id)
	panic("an observer that cannot handle what it was told")
}

func (o *panickingObserver) StepSkipped(id string) {
	o.saw = append(o.saw, id)
	panic("an observer that cannot handle a skip")
}

func (o *panickingObserver) WaitStarted(id string, _ string, _ time.Duration, _ bool) {
	o.saw = append(o.saw, id)
	panic("an observer that cannot handle a wait")
}

// TestAPanickingObserverDoesNotTakeTheRunWithIt is the rule [RunObserver]
// states, checked rather than asserted in prose: an account of the work must
// never be the reason the work does not happen.
//
// It matters because RunObserver is exported, so the implementation may be an
// embedder's. Without the recover, a run that was succeeding fails — and it
// fails inside recordStepOutcome, so the failure is reported against the step
// that was about to succeed, which is the most misleading place it could
// possibly surface.
//
// The observer is asked to observe on every callback and panics on each, so
// the assertion is that the run still reports success and that every
// observation point was actually reached rather than skipped.
func TestAPanickingObserverDoesNotTakeTheRunWithIt(t *testing.T) {
	t.Parallel()

	observer := &panickingObserver{}
	ctx := NewContextWithRunObserver(t.Context(), observer)

	require.NotPanics(t, func() {
		observeStepFinished(ctx, "build", nil, nil, false, SensitiveValues{})
		observeStepSkipped(ctx, &Node{Id: "prod_gate"})
		observeWaitStarted(ctx, "approval", "ship-approved", time.Hour, true)
	})

	require.Equal(t, []string{"build", "prod_gate", "approval"}, observer.saw,
		"a panic in one callback stopped a later observation point from being reached")
}

// onlyWithholding is a [WithholdingOnlyRunObserver] that counts what it is
// told.
type onlyWithholding struct{ told int }

func (*onlyWithholding) StepFinished(string, *Node_Outputs, error, bool) {}
func (*onlyWithholding) StepSkipped(string)                              {}
func (*onlyWithholding) WaitStarted(string, string, time.Duration, bool) {}
func (o *onlyWithholding) StepWithheld(string, SensitiveValues)          { o.told++ }

// TestAWithholdingOnlyObserverCostsNoCopy: an observer that reads only what a
// step withholds is told it, and the engine copies no outputs for it, where
// an observer reading the outputs gets its own copy of each (Codex, #2215).
func TestAWithholdingOnlyObserverCostsNoCopy(t *testing.T) {
	named := make(map[string]*Value, 200)
	for i := range 200 {
		named[string(rune('a'+i%26))+string(rune('a'+i/26))] = NewLiteral("value")
	}
	outputs := &Node_Outputs{NamedValues: named}

	only := &onlyWithholding{}
	onlyCtx := NewContextWithRunObserver(t.Context(), only)
	require.True(t, withholdingRead(onlyCtx), "the engine would compute no set for it")
	onlyAllocs := testing.AllocsPerRun(20, func() {
		observeStepFinished(onlyCtx, "step", outputs, nil, false, SensitiveValues{})
	})
	require.Positive(t, only.told, "the observer was not told")

	copyingCtx := NewContextWithRunObserver(t.Context(), &withholdingCounter{})
	copyingAllocs := testing.AllocsPerRun(20, func() {
		observeStepFinished(copyingCtx, "step", outputs, nil, false, SensitiveValues{})
	})

	require.Less(t, onlyAllocs, 5.0, "a withholding-only observer's step copied its outputs")
	require.Greater(t, copyingAllocs, 100.0, "the copying observer's step did not copy, so this proves nothing")
}

// withholdingCounter is a [WithholdingRunObserver] that discards what it is
// told.
type withholdingCounter struct{}

func (withholdingCounter) StepFinished(string, *Node_Outputs, error, bool) {}
func (withholdingCounter) StepSkipped(string)                              {}
func (withholdingCounter) WaitStarted(string, string, time.Duration, bool) {}
func (withholdingCounter) StepFinishedWithholding(string, *Node_Outputs, error, bool, SensitiveValues) {
}

// TestASkipQuotesAWholeConditionOrNone: the account quotes the whole `if:` it
// can render, leaving the bound to each driver after it withholds, and falls
// back to naming the skip for a condition it cannot render.
func TestASkipQuotesAWholeConditionOrNone(t *testing.T) {
	t.Parallel()

	long := NewExpr(`"` + strings.Repeat("é", 1000) + `" == ""`)
	text := SkippedText("gate", long, nil)
	require.True(t, strings.HasPrefix(text, "gate skipped: `if: \""), text)
	require.Equal(t, 1000, strings.Count(text, "é"), "the quote was cut before a driver could withhold it")

	require.Equal(t, "gate skipped (`if:` was false)", SkippedText("gate", NewLiteral("no"), nil),
		"a literal that is not a boolean has no condition to quote")
	require.Equal(t, "gate skipped (`if:` was false)", SkippedText("gate", nil, nil))
}

// TestASkipWithholdsAConstantByValue: a constant the predicate withholds is
// written as the marker before the condition is rendered, wherever it is, a
// macro's body and a map's key included, and whatever the renderer would have
// spelled it as: a bytes literal is written in octal escapes, which no match
// for its text finds (Codex, #2227). The run's own condition is left as it was.
func TestASkipWithholdsAConstantByValue(t *testing.T) {
	t.Parallel()

	secret := []byte("hunter2")
	condition := NewExpr(`inputs.token != b"hunter2" && ["a"].exists(x, x == "hunter2") && {"hunter2": 1}.size() == 2`)
	withheld := func(value any) bool {
		switch v := value.(type) {
		case []byte:
			return string(v) == string(secret)
		case string:
			return v == string(secret)
		}
		return false
	}

	text := SkippedText("gate", condition, withheld)
	require.NotContains(t, text, "hunter2")
	require.NotContains(t, text, `\150`, "the bytes literal was written in octal, so this proves nothing")
	require.Equal(t, 3, strings.Count(text, `"`+SensitiveMarker+`"`), text)
	require.Contains(t, SkippedText("gate", condition, nil), `b"\150`, "the renderer no longer writes bytes in octal, so this proves nothing")
	require.Contains(t, SkippedText("gate", condition, nil), `"hunter2"`, "the run's own condition was edited")

	// A value is asked about as every type it could be held as: an int in a
	// double literal, which renders in exponent form, and a string in a bytes
	// literal, which renders in octal.
	pin := func(value any) bool { return value == int64(918273645) }
	require.Contains(t, SkippedText("gate", NewExpr(`inputs.x != 918273645.0`), nil), "e+08", "the renderer no longer writes this double in exponent form, so this proves nothing")
	require.Equal(t, "gate skipped: `if: inputs.x != \"[redacted]\"` was false", SkippedText("gate", NewExpr(`inputs.x != 918273645.0`), pin))
	require.Equal(t, "gate skipped: `if: inputs.x != 9.182736455e+08` was false", SkippedText("gate", NewExpr(`inputs.x != 918273645.5`), pin),
		"a double that is no int was taken for one")
	word := func(value any) bool { return value == "hunter2" }
	require.Equal(t, "gate skipped: `if: inputs.x != \"[redacted]\"` was false", SkippedText("gate", NewExpr(`inputs.x != b"hunter2"`), word))

	// An unsigned value past the signed range, written as a double, and a
	// null leaf, which a sensitive structure's set holds as nil.
	top := func(value any) bool { return value == uint64(1<<63) }
	require.Equal(t, "gate skipped: `if: double(inputs.x) != \"[redacted]\"` was false",
		SkippedText("gate", NewExpr(`double(inputs.x) != 9223372036854775808.0`), top))
	null := func(value any) bool { return value == nil }
	require.Equal(t, "gate skipped: `if: inputs.x != \"[redacted]\"` was false", SkippedText("gate", NewExpr(`inputs.x != null`), null))
	require.Equal(t, "gate skipped: `if: inputs.x != null` was false", SkippedText("gate", NewExpr(`inputs.x != null`), nil),
		"the renderer no longer writes null, so this proves nothing")

	// A field name is no constant, but a sensitive structure's keys are what
	// its set withholds, whether selected or written in a message literal.
	key := func(value any) bool { return value == "string_value" }
	for _, source := range []string{`inputs.x.string_value != "a"`, `google.protobuf.Value{string_value: "a"} != inputs.x`} {
		rendered := SkippedText("gate", NewExpr(source), key)
		require.NotContains(t, rendered, "string_value", source)
		require.Contains(t, rendered, SensitiveMarker, source)
	}
}

// guardOnlyObserver is an embedder's observer that implements the guard
// interface and nothing else that withholds.
type guardOnlyObserver struct {
	mu       sync.Mutex
	withheld []SensitiveValues
}

func (*guardOnlyObserver) StepFinished(string, *Node_Outputs, error, bool)    {}
func (*guardOnlyObserver) StepSkipped(string)                                 {}
func (*guardOnlyObserver) WaitStarted(string, string, time.Duration, bool)    {}
func (*guardOnlyObserver) GuardFailed(string, string, error, SensitiveValues) {}
func (o *guardOnlyObserver) StepSkippedBy(_, _ string, _ *Value, withhold SensitiveValues) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.withheld = append(o.withheld, withhold)
}

// TestAGuardOnlyObserverIsToldWhatToWithhold: an observer that implements
// only [GuardRunObserver] is still one that renders, so the run gathers the
// declared-sensitive values for it and a skip inside a callee hands it the
// callee's (Codex, Copilot, #2227).
func TestAGuardOnlyObserverIsToldWhatToWithhold(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-guard-only"
	workflow := &Workflow{
		Name:    "guard-only",
		Profile: CurrentProfile,
		Steps: []*Node{{Id: "nested", Kind: &Node_Call{Call: &Call{
			Workflow: &Workflow{
				Name:           "child",
				Profile:        CurrentProfile,
				DeclaredInputs: []*InputDeclaration{{Name: "api_key", Type: InputDeclaration_TYPE_STRING, Sensitive: true}},
				Steps: []*Node{{
					Id:        "rotate",
					Condition: NewExpr(`inputs.api_key != "` + secret + `"`),
					Kind:      &Node_Task{Task: &Task{Name: "log", Inputs: map[string]*Value{"message": NewLiteral("never")}}},
				}},
			},
			Arguments: map[string]*Value{"api_key": NewLiteral(secret)},
		}}}},
	}

	observer := &guardOnlyObserver{}
	_, err := RunWithInputs(NewContextWithRunObserver(t.Context(), observer), workflow, nil)
	require.NoError(t, err)
	require.Len(t, observer.withheld, 1, "the skip was not reported, so this proves nothing")
	require.True(t, observer.withheld[0].IsSensitive(secret), "a guard-only observer was not told what the callee withholds")
}
