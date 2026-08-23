package flowtest

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTranscriptBoundsRetainedOutputBytes pins the round-six P1 on #1052: the
// event count alone reads as a bound while each event may retain megabytes of
// cloned outputs — the resource an untrusted submission actually controls. A
// recorder past the byte budget drops the event and marks the account
// truncated, exactly as past the event count.
func TestTranscriptBoundsRetainedOutputBytes(t *testing.T) {
	t.Parallel()

	r := newRunRecorder(v1.NewVirtualClock(epoch))

	big := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"blob": v1.NewLiteral(strings.Repeat("x", 1<<20)),
	}}
	for i := 0; i < 32; i++ {
		r.StepFinished("bulky", big, nil, false)
	}

	require.True(t, r.bytesFull, "past the byte budget the account must say it is incomplete")
	require.Less(t, len(r.events), 32, "events past the budget are dropped, not retained")
	require.LessOrEqual(t, r.outputBytes, maxTranscriptOutputBytes)

	lines := r.render()
	require.NotEmpty(t, lines)
	last := lines[len(lines)-1].Text
	assert.Contains(t, last, "truncated")
	assert.Contains(t, last, "step-output bound",
		"the truncation line names the bound that stopped the account, not the one that did not")
	assert.NotContains(t, last, "event bound")
}

// TestFailureTextIsWithheldWhenTheSetCouldNotBeBuilt pins round seven's P1
// on #1052: withholdAll means no text is provably safe, and a failure
// message can embed the very value the walk could not enumerate — so the
// failure line withholds rather than applying an empty substring list.
func TestFailureTextIsWithheldWhenTheSetCouldNotBeBuilt(t *testing.T) {
	t.Parallel()

	text, tone := stepOutcomeText(
		transcriptEvent{kind: eventStepFinished, step: "boom", failure: "expression refused secret-material-here"},
		sensitiveInputs{withholdAll: true},
		map[string]switchFact{},
	)
	assert.Equal(t, ToneDanger, tone)
	assert.NotContains(t, text, "secret-material-here")
	assert.Contains(t, text, "[failure withheld")
}

// TestShortDurationKeepsSubMillisecondTime pins the round-six timing finding:
// `sleep: 500us` is legal, and a timing account that rendered it as 0s would
// report that no virtual time passed.
func TestShortDurationKeepsSubMillisecondTime(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "0s", shortDuration(0))
	assert.Equal(t, "500µs", shortDuration(500*time.Microsecond))
	assert.Equal(t, "30s", shortDuration(30*time.Second))
	assert.Equal(t, "5m", shortDuration(5*time.Minute))
	assert.Equal(t, "1h30m", shortDuration(90*time.Minute))
	assert.Equal(t, "1h", shortDuration(time.Hour))
}
