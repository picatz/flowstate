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
