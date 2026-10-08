package debugtui

import (
	"bytes"
	"testing"
	"time"

	"github.com/charmbracelet/x/exp/teatest/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
)

// The real event loop, once: a program that draws, takes keys and exits. The
// tests above fold messages by hand, which proves the model and says nothing
// about whether a program built from it ever paints or ends; the waits here are
// on the program's output, not on a sleep.

func TestTheProgramPaintsAndDetaches(t *testing.T) {
	fake := newFake()
	m := modelFor(t, fake, func(c *Config) { c.Watch = true })
	tm := teatest.NewTestModel(t, m, teatest.WithInitialTermSize(120, 36))

	// The first paint is whole; what follows it is a diff of cells, so a name
	// that changes is not found in the output as a word. The title is in the
	// first paint, and the first read of the run is seen on the target.
	teatest.WaitFor(t, tm.Output(), func(out []byte) bool { return bytes.Contains(out, []byte("flow debug")) },
		teatest.WithDuration(20*time.Second))
	require.Eventually(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()

		return len(fake.inspects) > 0
	}, 20*time.Second, time.Millisecond, "the first frame was never read")

	// The run moving by itself, with no key pressed, does not disturb the screen.
	fake.advance()

	// q detaches the run; a press that lands while a read is being answered is
	// pressed again, since the screen takes one command at a time.
	require.Eventually(t, func() bool {
		tm.Send(tuitest.Key("q"))
		fake.mu.Lock()
		defer fake.mu.Unlock()

		return fake.detached
	}, 20*time.Second, 5*time.Millisecond)

	tm.WaitFinished(t, teatest.WithFinalTimeout(20*time.Second))
	final, ok := tm.FinalModel(t).(Model)
	require.True(t, ok)
	assert.True(t, final.Done())
	// Detach once the run took it; Ended if a read of the run, already detached by
	// that very command, got there first. The caller releases the run the same way.
	assert.Contains(t, []Outcome{OutcomeDetach, OutcomeEnded}, final.Outcome())
}

func TestTheProgramEndsOnCtrlCWithoutTouchingTheRun(t *testing.T) {
	fake := newFake()
	tm := teatest.NewTestModel(t, modelFor(t, fake), teatest.WithInitialTermSize(100, 30))
	teatest.WaitFor(t, tm.Output(), func(out []byte) bool { return bytes.Contains(out, []byte("flow debug")) },
		teatest.WithDuration(20*time.Second))

	tm.Send(tuitest.Key("ctrl+c"))
	tm.WaitFinished(t, teatest.WithFinalTimeout(20*time.Second))

	final := tm.FinalModel(t).(Model)
	assert.Equal(t, OutcomeInterrupt, final.Outcome())
	assert.Empty(t, fake.resumes, "an interrupt resumed or detached the run itself")
}
