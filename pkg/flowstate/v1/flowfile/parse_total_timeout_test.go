package flowfile_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The step's other clock, as an author writes it (#920).
//
// `total_timeout:` maps onto a bound both drivers have enforced since the
// defaults were reconciled — [v1.StepTimeouts.ScheduleToClose], defaulting to
// ten minutes — and what landed with the key is the ability to say what the
// budget is. So these tests are about the *grammar*: that the key compiles to
// the field, that it survives a round trip through the rewriter, and that the
// one arrangement of it nobody can have meant is refused with a position.

// TestATotalTimeoutCompilesToThePolicyField is the capability's first bar: a
// Flowfile can express it.
func TestATotalTimeoutCompilesToThePolicyField(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(`edition: v2026.3
name: w
steps:
  - id: poll
    timeout: 30s
    total_timeout: 5m
    retry:
      attempts: 20
      interval: 5s
    log:
      message: polling
`))
	require.NoError(t, err)

	policy := wf.GetSteps()[0].GetPolicy()
	require.Equal(t, 30*time.Second, policy.GetTimeout().AsDuration())
	require.Equal(t, 5*time.Minute, policy.GetTotalTimeout().AsDuration(),
		"the key must reach StepPolicy.total_timeout, which is what both drivers read through v1.StepTimeoutsFor")
}

// TestATotalTimeoutSurvivesTheRewriter is the other half of that bar. `flow fix`
// marshals a parsed workflow back out, and a key the writer does not know about
// is a key the rewriter silently deletes — which is the worst thing this package
// can do (CLAUDE.md, "A rewriter has to know what the grammar binds").
func TestATotalTimeoutSurvivesTheRewriter(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: w
steps:
  - id: poll
    timeout: 30s
    total_timeout: 5m
    log:
      message: polling
`)

	wf, _, err := flowfile.Parse(src)
	require.NoError(t, err)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	require.Contains(t, string(out), "total_timeout: 5m")

	again, _, err := flowfile.Parse(out)
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, again.GetSteps()[0].GetPolicy().GetTotalTimeout().AsDuration())
}

// TestATotalTimeoutShorterThanOneAttemptIsRefused is the diagnostic. A budget
// that expires inside the first attempt allows no attempt at all, and the two
// values sit in the same step where the compiler can compare them — a property
// of the file rather than of a deployment, which is the test validate.go's own
// standard sets for whether a diagnostic belongs here.
func TestATotalTimeoutShorterThanOneAttemptIsRefused(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(`edition: v2026.3
name: w
steps:
  - id: poll
    timeout: 5m
    total_timeout: 30s
    log:
      message: polling
`))
	require.Error(t, err)

	require.Contains(t, err.Error(), "total_timeout: 30s is shorter than timeout: 5m0s",
		"the diagnostic has to name both values, since which one is wrong is the author's to decide")
	require.Regexp(t, `\d+:\d+:`, err.Error(),
		"a position is what makes a diagnostic actionable in an editor rather than merely true")
}

// TestAMisspelledTotalTimeoutIsSuggested keeps the key inside the step grammar's
// own vocabulary rather than beside it: an unknown key is reported with what the
// author probably meant, and a key the parser knows nothing about would instead
// be reported as unknown with no suggestion at all.
func TestAMisspelledTotalTimeoutIsSuggested(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(`edition: v2026.3
name: w
steps:
  - id: poll
    total_timout: 5m
    log:
      message: polling
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), `did you mean "total_timeout"?`)
}

// TestTotalTimeoutIsReservedAgainstTaskNames is the ambiguity stepkeys.go exists
// to prevent, asked of the new word: a plugin registering a task called
// `total_timeout` would make `total_timeout: 5m` mean two incompatible things in
// one position, and no parser could recover which.
func TestTotalTimeoutIsReservedAgainstTaskNames(t *testing.T) {
	t.Parallel()

	require.True(t, v1.IsReservedStepKey("total_timeout"))
	require.False(t, v1.IsFutureStepKey("total_timeout"),
		"it is grammar now, not a word held for later")
}
