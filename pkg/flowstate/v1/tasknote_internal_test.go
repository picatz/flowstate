package flowstatev1

import (
	"context"
	"strings"
	"testing"
	"time"
)

// notingObserver records the notes it is given.
type notingObserver struct{ notes []string }

func (*notingObserver) StepFinished(string, *Node_Outputs, error, bool) {}
func (*notingObserver) StepSkipped(string)                              {}
func (*notingObserver) WaitStarted(string, string, time.Duration, bool) {}
func (o *notingObserver) TaskNoted(step, text string)                   { o.notes = append(o.notes, step+"|"+text) }

// TestANoteReachesItsWatcherAndStaysInsideItsBound: the step travels with the
// note, a long note is cut inside [MaxTaskNoteBytes] elision included, and
// without a watcher the context is left as it was.
func TestANoteReachesItsWatcherAndStaysInsideItsBound(t *testing.T) {
	plain := context.Background()
	if got := contextWithTaskStep(plain, "sync"); got != plain {
		t.Fatal("nobody is listening for notes, and the context changed anyway")
	}
	NoteTask(plain, "unheard") // must not panic

	observer := &notingObserver{}
	ctx := contextWithTaskStep(NewContextWithRunObserver(plain, observer), "sync")
	NoteTask(ctx, "page 2 of 5")
	NoteTask(ctx, strings.Repeat("é", MaxTaskNoteBytes))

	if len(observer.notes) != 2 {
		t.Fatalf("notes = %q, want two", observer.notes)
	}
	if observer.notes[0] != "sync|page 2 of 5" {
		t.Fatalf("note = %q, want it attributed to its step", observer.notes[0])
	}
	cut := strings.TrimPrefix(observer.notes[1], "sync|")
	if len(cut) > MaxTaskNoteBytes || !strings.HasSuffix(cut, "…") {
		t.Fatalf("a long note is %d bytes (bound %d), elided: %v", len(cut), MaxTaskNoteBytes, strings.HasSuffix(cut, "…"))
	}
}
