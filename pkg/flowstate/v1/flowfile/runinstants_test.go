package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `run.started_at` and `trigger.scheduled_at`, from the validator's side: where
// they may be read, what type the checker gives them, and what stays refused.

func instantsFile(use, slot string) string {
	extra := ""
	if slot == "if" {
		extra = "\n    value: 1"
	}

	return `edition: v2026.4
name: t
steps:
  - id: a
    ` + slot + `: ` + use + extra + `
`
}

func TestRunStartedAtIsReadableWhereverAnExpressionIs(t *testing.T) {
	t.Parallel()

	for _, use := range []string{
		"${run.started_at}",
		`${run.started_at - duration("24h")}`,
		"${run.started_at.getFullYear()}",
		"${trigger.scheduled_at}",
		`${trigger.scheduled_at - duration("24h")}`,
		"'${trigger.kind == \"schedule\" ? trigger.scheduled_at : run.started_at}'",
	} {
		wf, _, err := flowfile.Parse([]byte(instantsFile(use, "value")))
		require.NoError(t, err, use)
		assert.Empty(t, flowfile.Validate(wf), use)
	}

	wf, _, err := flowfile.Parse([]byte(`edition: v2026.4
name: t
steps:
  - id: a
    value: 1
    if: ${run.started_at.getFullYear() >= 2026}
outputs:
  at:
    value: ${run.started_at}
`))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(wf))
}

func TestTheInstantsAreTypedAsTimestamps(t *testing.T) {
	t.Parallel()

	for use, want := range map[string]string{
		"${run.started_at + 1}":            "no matching overload",
		"${trigger.scheduled_at + 'x'}":    "no matching overload",
		"${run.started_at.getFullYear(1)}": "no matching overload",
	} {
		wf, _, err := flowfile.Parse([]byte(instantsFile(use, "value")))
		require.NoError(t, err, use)
		err = flowfile.Validate(wf)
		require.Error(t, err, use)
		assert.Contains(t, err.Error(), want, use)
	}

	// A timestamp where a condition is read is refused with the position's own
	// sentence, as the typed leaf makes it knowable.
	wf, _, err := flowfile.Parse([]byte(instantsFile("${run.started_at}", "if")))
	require.NoError(t, err)
	err = flowfile.Validate(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "condition")
}

// Rule 1 stands: a start is fixed for the life of the run, and `now` is not, so
// the second is still bound only where a clock exists.
func TestNowIsStillRefusedOutsideAWait(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(instantsFile("${now}", "value")))
	if err == nil {
		err = flowfile.Validate(wf)
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

func TestUnknownFieldsUnderRunAndTriggerAreStillReported(t *testing.T) {
	t.Parallel()

	for use, want := range map[string]string{
		"${run.start_time}":          `unknown field "start_time" of ` + "`run`",
		"${run.attempt}":             `unknown field "attempt" of ` + "`run`",
		"${trigger.scheduled_time}":  `unknown field "scheduled_time" of ` + "`trigger`",
		"${trigger.payload}":         `unknown field "payload" of ` + "`trigger`",
		"${run.started_at.nope}":     "",
		"${trigger.scheduled_at.at}": "",
	} {
		wf, _, err := flowfile.Parse([]byte(instantsFile(use, "value")))
		require.NoError(t, err, use)
		err = flowfile.Validate(wf)
		require.Error(t, err, use)
		assert.Contains(t, err.Error(), want, use)
	}
}
