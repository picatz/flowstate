package exploretui

import (
	"bytes"
	"testing"
	"time"

	"github.com/charmbracelet/x/exp/teatest/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
)

// The real event loop, once: a program that reads, draws, takes keys and exits.
// The tests above fold messages by hand, which proves the model and says nothing
// about whether a program built from it ever paints or ends; the waits here are
// on the program's output, not on a sleep.
func TestTheProgramReadsPaintsAndLeaves(t *testing.T) {
	l := &loader{graph: fleet()}
	tm := teatest.NewTestModel(t, modelFor(t, l), teatest.WithInitialTermSize(120, 30))

	teatest.WaitFor(t, tm.Output(), func(out []byte) bool {
		return bytes.Contains(out, []byte("flow explore")) && bytes.Contains(out, []byte("checkout"))
	}, teatest.WithDuration(20*time.Second))

	tm.Send(tuitest.Key("q"))
	tm.WaitFinished(t, teatest.WithFinalTimeout(20*time.Second))
	final, ok := tm.FinalModel(t).(Model)
	require.True(t, ok)
	assert.True(t, final.Done())
	assert.Equal(t, 1, l.calls)
}
