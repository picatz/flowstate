package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The machine half of issue #931: what a `--seeds` run found rides the
// [v1.TestReport] as the `schedules` schema field, attached in the one suite
// loop every door shares, so the CLI's JSON modes and the MCP tool carry the
// account CI reads rather than prose it would have to scrape.

// TestTheReportCarriesTheExploration pins the attach through the real door: an
// exploring run's report carries the same numbers the Go-side result does, and
// a default run's report carries nothing — the document it always was.
func TestTheReportCarriesTheExploration(t *testing.T) {
	t.Parallel()

	path := writeScheduleFixture(t, junctionWorkflow)

	report, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 4, Seed0: 1})
	require.NotNil(t, schedules)

	carried := report.GetSchedules()
	require.NotNil(t, carried, "the report must carry the exploration it performed")
	assert.Equal(t, int32(4), carried.GetSchedules())
	assert.Equal(t, int32(1), carried.GetCases())
	assert.Equal(t, int32(schedules.Decisions), carried.GetDecisions(),
		"one exploration, one account: the schema field and the Go result must agree")
	assert.Nil(t, carried.GetDivergence())

	plain, _, _ := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{})
	assert.Nil(t, plain.GetSchedules(),
		"a run that explored nothing must leave the field unset, not zero-valued")
}

// TestScheduleReportRendersTheDivergence covers the rendering a real run
// cannot cheaply produce: a divergence carries the case, the seed — the whole
// of what a replay needs — and both renderings, field for field. Hand-built
// because this is a pure rendering check; the engine's own divergence
// detection is dst's tested ground.
func TestScheduleReportRendersTheDivergence(t *testing.T) {
	t.Parallel()

	in := &flowtest.ScheduleReport{
		Schedules: 8,
		Cases:     2,
		Decisions: 5,
		Truncated: true,
		Divergence: &flowtest.ScheduleDivergence{
			Case:         "the racing case",
			Seed:         42,
			Decisions:    5,
			Truncated:    true,
			WrittenOrder: "a then b",
			Seeded:       "b then a",
		},
	}

	out := in.Report()
	assert.Equal(t, int32(8), out.GetSchedules())
	assert.Equal(t, int32(2), out.GetCases())
	assert.Equal(t, int32(5), out.GetDecisions())
	assert.True(t, out.GetTruncated())

	d := out.GetDivergence()
	require.NotNil(t, d)
	assert.Equal(t, "the racing case", d.GetCase())
	assert.Equal(t, uint64(42), d.GetSeed())
	assert.Equal(t, int32(5), d.GetDecisions())
	assert.True(t, d.GetTruncated())
	assert.Equal(t, "a then b", d.GetWrittenOrder())
	assert.Equal(t, "b then a", d.GetSeeded())
}
