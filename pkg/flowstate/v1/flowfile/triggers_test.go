package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The spelling this feature ships under, written once so a change to it fails a
// test rather than quietly becoming a second grammar.
const triggeredSource = `edition: v2026.2
name: nightly-report
triggers:
  schedule:
    cron: "0 7 * * MON-FRI"
    time_zone: Europe/Dublin
    jitter: 5m
    overlap: skip
steps:
  - id: report
    log:
      message: reporting
`

// TestParsingATriggersBlock pins what the block compiles to.
func TestParsingATriggersBlock(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(triggeredSource))
	require.NoError(t, err)

	schedule := workflow.GetTriggers().GetSchedule()
	require.NotNil(t, schedule)

	assert.Equal(t, []string{"0 7 * * MON-FRI"}, schedule.GetCron())
	assert.Equal(t, "Europe/Dublin", schedule.GetTimeZone())
	assert.Equal(t, 5*60.0, schedule.GetJitter().AsDuration().Seconds())
	assert.Equal(t, v1.ScheduleTrigger_OVERLAP_SKIP, schedule.GetOverlap())
	assert.Nil(t, schedule.GetEvery())

	// Positions, because a diagnostic without one is a diagnostic the author has to
	// go looking for. Every key a mistake can be written under has to be locatable.
	for _, path := range []string{
		"triggers", "triggers.schedule", "triggers.schedule.cron",
		"triggers.schedule.time_zone", "triggers.schedule.jitter", "triggers.schedule.overlap",
	} {
		_, ok := positions.At(path)
		assert.True(t, ok, "no recorded position for %q", path)
	}
}

// TestATriggersBlockDoesNotStopAWorkflowRunning is the property the examples
// harness depends on and the design intends: declaring a cadence is not starting
// one, so a scheduled file is still an ordinary file both drivers execute.
func TestATriggersBlockDoesNotStopAWorkflowRunning(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(triggeredSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "a declared schedule is not a validation problem")

	workflow, err := flowfile.Unmarshal([]byte(triggeredSource))
	require.NoError(t, err)
	require.Len(t, workflow.GetSteps(), 1, "the steps are what a driver runs, and they are untouched")
}

// TestCronMayBeOneOrMany covers both spellings, and that writing them back out
// keeps the one somebody wrote.
func TestCronMayBeOneOrMany(t *testing.T) {
	t.Parallel()

	many := strings.Replace(triggeredSource,
		`    cron: "0 7 * * MON-FRI"`,
		"    cron:\n      - \"0 7 * * MON-FRI\"\n      - \"0 12 * * SUN\"", 1)

	workflow, err := flowfile.Unmarshal([]byte(many))
	require.NoError(t, err)
	assert.Equal(t, []string{"0 7 * * MON-FRI", "0 12 * * SUN"}, workflow.GetTriggers().GetSchedule().GetCron())
}

// TestMarshalIsTheInverseForTriggers is the guard against a formatter that
// silently deletes a schedule.
//
// `flow fmt` writes Marshal(Parse(x)), so a key the parser knows and the writer
// does not is an author's block disappearing from a file they asked to be tidied.
// Compared by re-parsing rather than by bytes, because the block is rewritten in
// the canonical key order and comparing text would be testing the layout.
func TestMarshalIsTheInverseForTriggers(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(triggeredSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)

	assert.Equal(t, workflow.GetTriggers().GetSchedule().GetCron(), again.GetTriggers().GetSchedule().GetCron())
	assert.Equal(t, workflow.GetTriggers().GetSchedule().GetTimeZone(), again.GetTriggers().GetSchedule().GetTimeZone())
	assert.Equal(t, workflow.GetTriggers().GetSchedule().GetOverlap(), again.GetTriggers().GetSchedule().GetOverlap())
	assert.Equal(t,
		workflow.GetTriggers().GetSchedule().GetJitter().AsDuration(),
		again.GetTriggers().GetSchedule().GetJitter().AsDuration())

	// One expression stays a scalar rather than becoming a list of one, so a
	// formatter does not rewrite a line for no reason a reader can see.
	assert.Contains(t, string(written), "cron: 0 7 * * MON-FRI")
	assert.NotContains(t, string(written), "cron:\n")
}

// TestTriggerDiagnosticsCarryAPosition is the point of validating a cadence in the
// compiler at all.
//
// A malformed cron expression is a property of the file, so it is reported the way
// every other property of the file is: with a line and a column, and a sentence
// saying what to write instead. Without the position it is a message about a
// protobuf field, which sends the author to look at the wrong thing.
func TestTriggerDiagnosticsCarryAPosition(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		source  string
		line    int
		message string
	}{
		{
			name:    "a cron expression with too few fields",
			source:  strings.Replace(triggeredSource, `"0 7 * * MON-FRI"`, `"0 7 * *"`, 1),
			line:    5,
			message: "has 4 fields",
		},
		{
			name:    "an hour that is not an hour",
			source:  strings.Replace(triggeredSource, `"0 7 * * MON-FRI"`, `"0 25 * * *"`, 1),
			line:    5,
			message: "hours is 25",
		},
		{
			name: "a schedule with no cadence at all",
			source: `edition: v2026.2
name: nightly-report
triggers:
  schedule:
    time_zone: Europe/Dublin
steps:
  - id: report
    log:
      message: reporting
`,
			line:    5,
			message: "a schedule needs a cadence",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := flowfile.ValidateSource([]byte(tt.source))
			require.NoError(t, err)
			require.NotEmpty(t, diagnostics, "the mistake was not reported at all")

			found := false
			for _, d := range diagnostics {
				if !strings.Contains(d.Message, tt.message) {
					continue
				}
				found = true
				assert.Equal(t, tt.line, d.Line, "reported at the wrong line: %s", d.Error())
				assert.Positive(t, d.Column, "reported without a column: %s", d.Error())
			}

			assert.True(t, found, "wanted a diagnostic saying %q, got: %s", tt.message, diagnostics.Error())
		})
	}
}

// TestAnEmptyTriggersBlockIsRefused, with a position, because a block that reads as
// if this workflow starts on its own and compiles to nothing is exactly the silent
// success this repository ranks worst.
//
// An empty flow mapping has no span of its own, so the diagnostic falls back to the
// key — which is what makes the line here 3 rather than nothing.
func TestAnEmptyTriggersBlockIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: nightly-report
triggers: {}
steps:
  - id: report
    log:
      message: reporting
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "3:1: triggers: declares no trigger")
}

// TestAMisspelledScheduleKeyIsReported holds the rule that a key doing nothing
// silently is the worst outcome: the author has no reason to doubt the file.
func TestAMisspelledScheduleKeyIsReported(t *testing.T) {
	t.Parallel()

	source := strings.Replace(triggeredSource, "    time_zone: Europe/Dublin", "    timezone: Europe/Dublin", 1)

	// A key nobody knows is a compile error rather than a validation diagnostic —
	// ValidateSource answers with an error for a file that did not parse — which is
	// what makes the suggestion possible: an unknown key is compared against the keys
	// that exist here, and there is a very close one.
	_, err := flowfile.ValidateSource([]byte(source))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown key "timezone"; did you mean "time_zone"?`)
}

// TestAnUnknownOverlapPolicyIsReported, with the alternatives offered — the same
// treatment a misspelled input type gets, because they are the same mistake.
func TestAnUnknownOverlapPolicyIsReported(t *testing.T) {
	t.Parallel()

	source := strings.Replace(triggeredSource, "    overlap: skip", "    overlap: queue", 1)

	// A compile error rather than a validation diagnostic, the same standing an
	// unknown input type has: the value names something the grammar does not have,
	// so there is nothing to compile it to, and the refusal offers the alternatives.
	_, err := flowfile.ValidateSource([]byte(source))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffer_one")
}

// TestAZoneBesideAnIntervalIsReported covers the line that reads as if it does
// something and does not: an interval is measured from a fixed instant, so there is
// no local clock for a zone to shift.
func TestAZoneBesideAnIntervalIsReported(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.2
name: nightly-report
triggers:
  schedule:
    every: 15m
    time_zone: Europe/Dublin
steps:
  - id: report
    log:
      message: reporting
`

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)
	assert.Contains(t, diagnostics.Error(), "has no effect beside `every:`")
}
