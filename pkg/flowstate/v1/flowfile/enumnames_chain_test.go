package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestEnumComparisonsJudgeOnlyAChainRootedAtATask pins the conservative edge of
// the diagnostic: an operand is judged only when it is written as a chain from
// `steps.<id>`. A name an author bound (a comprehension variable, an iterator, a
// var) or a record they built is never judged, whatever its fields are called.
func TestEnumComparisonsJudgeOnlyAChainRootedAtATask(t *testing.T) {
	registerEnumProbe(t, 1)

	const header = `edition: v2026.4
name: chains
steps:
  - id: probe
    ` + enumProbeTask + `:
      message: hi
`
	for name, source := range map[string]string{
		"a var shadowing a comprehension variable": header + `  - id: gate
    vars:
      c:
        calibration: low
    value: ${steps.probe.distribution.exists(c, true) && c.calibration == "high"}
`,
		"an iterator name reused by a sibling var": header + `  - id: each
    for_each:
      items: ${steps.probe.distribution.map(k, k)}
      as: row
      steps:
        - id: inner
          value: ${row}
  - id: gate
    vars:
      row:
        calibration: low
    value: ${row.calibration == "high"}
`,
		"a record the author built with map": header + `  - id: gate
    value: '${steps.probe.distribution.map(k, {"calibration": "x"})[0].calibration == "high"}'
`,
	} {
		t.Run(name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(source))
			require.NoError(t, err)
			assert.Empty(t, ds, ds.Error())
		})
	}

	for _, comparison := range []string{
		`steps.probe.distribution.filter(d, true)[0].calibration == "x"`,
		`steps.probe.calibration == "x"`,
	} {
		ds, err := flowfile.ValidateSource([]byte(enumSource(comparison)))
		require.NoError(t, err)
		assert.NotEmpty(t, ds, "%s is a chain from the task and is judged", comparison)
	}
}
