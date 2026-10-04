package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A report names the window it covers, and the window is arithmetic over two
// instants a case can state: when the firing was meant for, and when the run began.

const theWindowedReport = `edition: v2026.4
name: windowed-report
steps:
  - id: window
    value: ${trigger.scheduled_at - duration("24h")}
  - id: stamp
    log:
      message: ${"window " + string(steps.window.value)}
outputs:
  from:
    value: ${string(steps.window.value)}
  to:
    value: ${string(trigger.scheduled_at)}
  started:
    value: ${string(run.started_at)}
  start_year:
    value: ${run.started_at.getFullYear()}
`

func TestACaseStatesTheInstantsAWindowIsComputedFrom(t *testing.T) {
	t.Parallel()

	report := flowtest.RunSource("window", []byte(theWindowedReport), []byte(`edition: v2026.4
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: yesterday's window is the slot minus a day, whenever the run happens
    started_at: 2026-08-03T09:00:00Z
    trigger:
      kind: schedule
      name: nightly
      scheduled_at: 2026-08-02T07:00:00Z
    expect:
      outputs:
        from: "2026-08-01T07:00:00Z"
        to: "2026-08-02T07:00:00Z"
        started: "2026-08-03T09:00:00Z"
        start_year: 2026

  - name: a case that states neither starts where every case always has
    trigger: { kind: manual }
    expect:
      outputs:
        started: "2020-01-01T00:00:00Z"
        start_year: 2020
        to: "1970-01-01T00:00:00Z"
        from: "1969-12-31T00:00:00Z"
`))

	require.Empty(t, report.GetRefused(), "the file was refused: %s", report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		assert.Truef(t, c.GetPassed(), "case %q failed: %v", c.GetName(), c.GetFailures())
	}
}

// An instant that is not one, and a slot where there is no schedule, would each be
// a green case certifying a value nothing produces.
func TestAnInstantIsRefusedWhereItCannotMeanWhatItSays(t *testing.T) {
	t.Parallel()

	for _, test := range []struct{ name, tests, want string }{
		{
			name: "a start that is not an instant",
			tests: `  - name: bad
    started_at: yesterday
    trigger: { kind: manual }
`,
			want: "is not an RFC 3339 instant",
		},
		{
			name: "a slot that is not an instant",
			tests: `  - name: bad
    trigger: { kind: schedule, scheduled_at: noon }
`,
			want: "is not an RFC 3339 instant",
		},
		{
			name: "a slot on a manual start",
			tests: `  - name: bad
    trigger: { kind: manual, scheduled_at: 2026-08-02T07:00:00Z }
`,
			want: "belongs to `kind: schedule`",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			report := flowtest.RunSource("refused", []byte(theWindowedReport),
				[]byte("edition: v2026.4\ntests:\n"+test.tests))
			assert.Contains(t, report.GetRefused(), test.want)
		})
	}
}
