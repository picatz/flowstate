package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// twoGates waits an hour for each of two approvals, so a lost delivery of
// either can only lapse its own gate.
const twoGates = `edition: v2026.4
name: two-gates
steps:
  - id: first
    wait_for_signal: {name: a, timeout: 1h, outputs: {timed_out: '${timed_out}'}}
  - id: second
    wait_for_signal: {name: b, timeout: 1h, outputs: {timed_out: '${timed_out}'}}
outputs:
  done:
    value: 'true'
`

// swarmCase claims that losing a's delivery implies b's was lost too. Both
// faults fire on every delivery they are on for, so the claim holds in every
// run that has both on and is broken only by a run with a on and b off.
const swarmCase = `edition: v2026.4
tests:
  - name: the gates
    workflow: ./workflow.yaml
    signals:
      - {name: a, at: 10m, payload: {}}
      - {name: b, at: 20m, payload: {}}
    faults:
      - {signal: a, drop: true, rate: 1}
      - {signal: b, drop: true, rate: 1}
    invariants:
      - that: "!('a' in run.signals.dropped) || 'b' in run.signals.dropped"
        because: a lost a-approval is always accompanied by a lost b-approval
    expect: {failed: false}
`

// With every fault on in every seed, the world the asymmetry needs never
// comes up: the case looks resilient.
func TestWithoutSwarmEveryFaultIsOnInEverySeed(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, twoGates, swarmCase)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 16, Seed0: 1})

	require.NotNil(t, schedules)
	assert.Nil(t, schedules.Divergence, "both faults fire in every seed, so the claim holds in all of them")
}

// Under swarm some seed leaves b's fault off and keeps a's, which is the run
// that breaks the claim; the finding says the seed needs --swarm to replay, and
// replaying it without does not reproduce it.
func TestSwarmFindsTheFailureThatNeedsOneFaultAlone(t *testing.T) {
	t.Parallel()

	path := writeFaultFixture(t, twoGates, swarmCase)
	_, _, schedules := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Schedules: 16, Seed0: 1, Swarm: true})

	require.NotNil(t, schedules)
	require.NotNil(t, schedules.Divergence, "some seed keeps a's fault on and b's off")
	assert.True(t, schedules.Divergence.Invariant)
	assert.True(t, schedules.Divergence.Swarm)
	assert.Contains(t, schedules.Divergence.Seeded, "a lost a-approval is always accompanied")

	seed := schedules.Divergence.Seed
	_, _, same := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Pinned: &seed, Swarm: true})
	require.NotNil(t, same.Divergence)
	assert.Equal(t, schedules.Divergence.Seeded, same.Divergence.Seeded, "the seed replays under the same flag")

	_, _, plain := flowtest.RunFileUnderSchedules(t.Context(), path, dst.Budget{Pinned: &seed})
	require.NotNil(t, plain)
	assert.Nil(t, plain.Divergence, "without --swarm the same seed turns every fault on")
}
