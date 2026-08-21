package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// What this file is for.
//
// telemetry_metrics_test.go covers the two instruments a *working* plugin
// moves: the calls counter and the operation-duration histogram, on the
// execute and health paths. The other four are the ones that only move when
// something goes wrong — a launch that fails, a handshake that is not one, a
// process that has to be brought back — and they are exactly the instruments
// an operator reads during an incident, so an absent or mislabelled one is
// discovered at the worst possible moment.
//
// The same manual reader is used here for the same reason it is used there: a
// PeriodicReader would make every assertion a race against a wall clock, and
// these tests already have real processes to wait for.

// requireNoMetric asserts that an instrument recorded nothing.
//
// The SDK exports an instrument only once it has a data point, so "never
// incremented" and "absent from the batch" are the same observation. This is
// the negative direction the counters need: a failure counter that moves on a
// healthy deployment is worse than one that does not move at all, because it
// is a page nobody can act on.
func requireNoMetric(t *testing.T, collected []metricdata.Metrics, name string) {
	t.Helper()

	for _, m := range collected {
		require.NotEqual(t, name, m.Name, "instrument %q recorded on a path that must not move it", name)
	}
}

// sumOf returns the single data point of an Int64 counter, insisting there is
// exactly one: two data points mean the counter split into two series, which
// for these instruments would mean a label nobody intended.
func sumOf(t *testing.T, m metricdata.Metrics) metricdata.DataPoint[int64] {
	t.Helper()

	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "instrument %q is a %T, want a monotonic Int64 sum", m.Name, m.Data)
	require.True(t, sum.IsMonotonic, "instrument %q is a counter and must be monotonic", m.Name)
	require.Len(t, sum.DataPoints, 1, "instrument %q split into %d series", m.Name, len(sum.DataPoints))

	return sum.DataPoints[0]
}

// TestRestartsAreCountedPerPluginUpToTheBudgetAndNoFurther drives a plugin that
// dies shortly after every launch, and asserts the restart counter reaches the
// restart budget exactly.
//
// Reaching the bound is the assertion, not merely staying under it: a counter
// that gave up after the first relaunch also satisfies "no more than the
// budget", and it would hide precisely the flapping the metric exists to show
// (CLAUDE.md, "assert the bound was reached as well as not exceeded"). The
// arithmetic is deterministic — [Plugin.restart] increments its attempt count,
// refuses the attempt once it passes MaxRestarts, and records only after the
// budget said yes — so with a budget of two the counter must read two, while
// the plugin's own attempt count reads three.
//
// The plugin name on the data point is the other half. This counter carried no
// attributes at all until the commit before this one, which made it a restart
// rate summed over every plugin a deployment runs: a number that says something
// is flapping and refuses to say what.
func TestRestartsAreCountedPerPluginUpToTheBudgetAndNoFurther(t *testing.T) {
	// Deliberately not parallel. This is the only test in the package that
	// holds a plugin in a relaunch loop, so it spends several seconds
	// repeatedly forking, handshaking and reaping processes — and two of this
	// package's conformance tests assert that a progress flood does not starve
	// a terminal response, which is a claim about deadlines and therefore a
	// claim about how loaded the machine is. Run alongside them under -race it
	// made those two fail about one run in three. A test that measures its own
	// subject correctly and makes its neighbours wrong is still a broken test.
	provider, collect := collectMetrics(t)

	cfg := testConfig(t, pluginDir(t, "short-lived"))
	cfg.MeterProvider = provider
	cfg.MaxRestarts = 2
	cfg.RestartBackoff = 10 * time.Millisecond
	cfg.MaxRestartBackoff = 20 * time.Millisecond

	host := openHost(t, cfg)

	p, ok := host.Lookup("short-lived")
	require.True(t, ok, "the plugin was not launched")

	require.True(t, waitFor(t, 15*time.Second, func() bool { return p.State() == StateFailed }),
		"state = %v after waiting, want failed", p.State())

	restarts := findMetric(t, collect(), "flowstate.plugin.restarts")

	point := sumOf(t, restarts)
	require.Equal(t, int64(cfg.MaxRestarts), point.Value,
		"the restart budget is %d, the plugin was given up on, and the counter reads %d",
		cfg.MaxRestarts, point.Value)
	require.Greater(t, p.Restarts(), cfg.MaxRestarts,
		"the attempt that spent the budget must be counted as an attempt and not as a relaunch")

	require.Equal(t, map[string]string{metricschema.PluginName: "short-lived"},
		attributeSets(t, restarts)[0],
		"a restart counter without the plugin's name cannot say which plugin is flapping")
}

// TestAHandshakeFailureIsCountedAsBothALaunchFailureAndAProtocolError covers
// the two refusal counters together, because they are recorded from one defer
// and a test of either alone cannot see the condition that separates them.
//
// Both are labelled with the plugin's name for the same reason the restart
// counter is. And both are asserted to be *one* series: a refusal counted twice
// under two spellings of the same plugin would read as twice the trouble.
func TestAHandshakeFailureIsCountedAsBothALaunchFailureAndAProtocolError(t *testing.T) {
	t.Parallel()

	provider, collect := collectMetrics(t)

	cfg := testConfig(t, pluginDir(t, "garbage"))
	cfg.MeterProvider = provider
	cfg.HandshakeTimeout = time.Minute
	cfg.DescribeTimeout = time.Minute

	host, err := NewHost(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close(context.Background()) })

	require.Error(t, host.Open(t.Context()), "a plugin printing garbage was accepted")

	collected := collect()

	named := map[string]string{metricschema.PluginName: "garbage"}

	failures := findMetric(t, collected, "flowstate.plugin.launch.failures")
	require.Equal(t, int64(1), sumOf(t, failures).Value)
	require.Equal(t, named, attributeSets(t, failures)[0],
		"a launch failure with no plugin name says a launch failed and refuses to say whose")

	protocolErrors := findMetric(t, collected, "flowstate.plugin.protocol.errors")
	require.Equal(t, int64(1), sumOf(t, protocolErrors).Value)
	require.Equal(t, named, attributeSets(t, protocolErrors)[0])
}

// TestAPluginRefusedAfterItsHandshakeIsNoProtocolError is the negative
// direction, and the one that keeps the protocol-error counter meaning
// something.
//
// "describe-fails" hands shake correctly and then fails the call that asks it
// to describe itself. The host refuses it — but nothing about the protocol went
// wrong, and the launch itself succeeded, so neither refusal counter may move.
// A protocol-error counter that also counts application failures is a counter
// an operator reads as "the wire is broken" while the wire is fine.
//
// What *does* record is the calls counter, under the operation that failed: the
// launch succeeded, the start did not. Asserting both halves is the point —
// a version of this test that only checked the two absences would be equally
// happy with telemetry that had stopped recording altogether.
func TestAPluginRefusedAfterItsHandshakeIsNoProtocolError(t *testing.T) {
	t.Parallel()

	provider, collect := collectMetrics(t)

	cfg := testConfig(t, pluginDir(t, "describe-fails"))
	cfg.MeterProvider = provider
	cfg.HandshakeTimeout = time.Minute
	cfg.DescribeTimeout = time.Minute

	host, err := NewHost(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close(context.Background()) })

	require.Error(t, host.Open(t.Context()), "a plugin that cannot describe itself was accepted")

	collected := collect()

	requireNoMetric(t, collected, "flowstate.plugin.launch.failures")
	requireNoMetric(t, collected, "flowstate.plugin.protocol.errors")

	calls := attributeSets(t, findMetric(t, collected, "flowstate.plugin.calls"))
	require.Contains(t, calls, map[string]string{
		metricschema.PluginName:      "describe-fails",
		metricschema.PluginOperation: "launch",
		metricschema.PluginOutcome:   "success",
	}, "the launch itself worked and must be counted as a success")
	require.Contains(t, calls, map[string]string{
		metricschema.PluginName:      "describe-fails",
		metricschema.PluginOperation: "start",
		metricschema.PluginOutcome:   "error",
	}, "the failure has to be visible as a rate somewhere, or it is only in a trace someone already has open")
}

// TestEveryPluginOperationIsRecordedUnderItsOwnName asserts the set of values
// behind flowstate.plugin.operation, which [metricschema] classifies as bounded
// by construction — a fixed enumeration written in this repository.
//
// "Bounded by construction" is a claim about the source, and the only way to
// check a claim about a fixed enumeration is to drive every member of it and
// see the set that comes back. The set, not each member: a test that asserted
// only that "execute" appears would pass just as happily on a build where every
// operation recorded as "execute", which is one series pretending to be four.
//
// The healthy path's three absences are asserted here too, for the reason given
// where they appear.
func TestEveryPluginOperationIsRecordedUnderItsOwnName(t *testing.T) {
	t.Parallel()

	provider, collect := collectMetrics(t)

	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.MeterProvider = provider

	host := openHost(t, cfg)

	// launch and start happen during Open; execute and health are asked for
	// here, so all four verbs have been driven by the collection below.
	_, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
	require.NoError(t, err)

	p, ok := host.Lookup("ok")
	require.True(t, ok)
	require.Equal(t, HealthServing, p.CheckHealth(t.Context()).Status)

	collected := collect()

	// No failure counter moved, which is the assertion the three failure
	// instruments need most and the easiest one to leave out: a counter that
	// ticks on a healthy path is a false alarm arriving as a dashboard panel,
	// and nobody investigates the fourth false page. It rides here rather than
	// in a test of its own because it needs exactly this setup — a plugin that
	// launched, answered and stayed up — and a second host launching a second
	// process to assert three absences is load this package's deadline-sensitive
	// conformance tests have to share a machine with.
	requireNoMetric(t, collected, "flowstate.plugin.launch.failures")
	requireNoMetric(t, collected, "flowstate.plugin.protocol.errors")
	requireNoMetric(t, collected, "flowstate.plugin.restarts")

	operations := map[string]struct{}{}
	for _, set := range attributeSets(t, findMetric(t, collected, "flowstate.plugin.calls")) {
		operations[set[metricschema.PluginOperation]] = struct{}{}
	}

	require.Equal(t, map[string]struct{}{
		"launch":  {},
		"start":   {},
		"health":  {},
		"execute": {},
	}, operations, "the operations recorded are not the enumeration metricschema documents")
}
