package flowtest

import (
	"context"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

type silentObserver struct{}

func (silentObserver) StepFinished(string, *v1.Node_Outputs, error, bool) {}
func (silentObserver) StepSkipped(string)                                 {}
func (silentObserver) WaitStarted(string, string, time.Duration, bool)    {}

type notingObserver struct {
	silentObserver
	notes []string
}

func (o *notingObserver) TaskNoted(step, text string) { o.notes = append(o.notes, step+": "+text) }

// TestATeeCarriesATasksNotesToTheDebugger: a debugged case tees the recorder
// with the session, and a task's note must still reach the session.
func TestATeeCarriesATasksNotesToTheDebugger(t *testing.T) {
	debugger := &notingObserver{}
	ctx := v1.NewContextWithRunObserver(context.Background(), teeObserver{first: silentObserver{}, second: debugger})

	v1.NoteTask(ctx, "fetched page 3")

	if len(debugger.notes) != 1 || debugger.notes[0] != ": fetched page 3" {
		t.Fatalf("notes = %q, want the one note", debugger.notes)
	}
}

type withholdingObserver struct {
	silentObserver
	plain, withheld int
	withhold        v1.SensitiveValues
}

func (o *withholdingObserver) StepFinished(string, *v1.Node_Outputs, error, bool) { o.plain++ }

func (o *withholdingObserver) StepFinishedWithholding(_ string, _ *v1.Node_Outputs, _ error, _ bool, withhold v1.SensitiveValues) {
	o.withheld++
	o.withhold = withhold
}

type countingObserver struct {
	silentObserver
	plain int
}

func (o *countingObserver) StepFinished(string, *v1.Node_Outputs, error, bool) { o.plain++ }

// TestATeeTellsTheDebuggerWhatToWithhold: a debugged case tees the recorder
// with the session, and the session must still be told what a step's
// account withholds (#2210), while the recorder, the case's record, is told
// the plain outcome.
func TestATeeTellsTheDebuggerWhatToWithhold(t *testing.T) {
	recorder, debugger := &countingObserver{}, &withholdingObserver{}
	withhold := v1.SensitiveInputValues(map[string]*v1.Value{"api_key": v1.NewLiteral("hunter2")}, map[string]bool{"api_key": true})

	teeObserver{first: recorder, second: debugger}.StepFinishedWithholding("boom", nil, nil, false, withhold)

	if recorder.plain != 1 {
		t.Fatalf("the recorder heard %d plain outcomes, want 1", recorder.plain)
	}
	if debugger.withheld != 1 || debugger.plain != 0 {
		t.Fatalf("the debugger heard %d withholding and %d plain outcomes, want 1 and 0", debugger.withheld, debugger.plain)
	}
	if !debugger.withhold.IsSensitive("hunter2") {
		t.Fatal("the debugger was not told what to withhold")
	}
}

// TestATeeTellsAGathererWhatToWithhold: a gatherer teed with a debugger hears
// each step's set, as it does alone, though it reads nothing else.
func TestATeeTellsAGathererWhatToWithhold(t *testing.T) {
	gatherer, debugger := &sensitiveGatherer{}, &withholdingObserver{}
	withhold := v1.SensitiveInputValues(map[string]*v1.Value{"api_key": v1.NewLiteral("hunter2")}, map[string]bool{"api_key": true})

	teeObserver{first: gatherer, second: debugger}.StepFinishedWithholding("boom", nil, nil, false, withhold)

	if !gatherer.withheld().IsSensitive("hunter2") {
		t.Fatal("the gatherer was not told what to withhold")
	}
	if debugger.withheld != 1 {
		t.Fatalf("the debugger heard %d withholding outcomes, want 1", debugger.withheld)
	}
}
