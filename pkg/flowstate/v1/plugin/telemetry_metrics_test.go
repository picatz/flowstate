package plugin

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// What this file is for.
//
// #401 recorded that the telemetry tests cover logs over the wire and traces
// in a recorder, and metrics not at all: no manual reader, no metricdata
// assertion, no test import of the metric SDK anywhere. Every instrument in
// this repository is declared in this package, so this is where that gap is
// closed — an instrument could have been absent, double-counted, or carrying
// the wrong attributes, and nothing would have said so.
//
// The reader is [sdkmetric.ManualReader] on purpose. A PeriodicReader exports
// on a wall-clock interval, which would make every assertion here a race
// against a timer; the manual reader collects when the test asks, so these
// tests are hermetic and deterministic with no collector and no network.

// collectMetrics returns a meter provider writing into a manual reader, and a
// function collecting whatever has been recorded so far.
func collectMetrics(t *testing.T) (*sdkmetric.MeterProvider, func() []metricdata.Metrics) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	return provider, func() []metricdata.Metrics {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &rm))

		var out []metricdata.Metrics
		for _, scope := range rm.ScopeMetrics {
			out = append(out, scope.Metrics...)
		}
		return out
	}
}

// findMetric returns the collected instrument with the given name.
func findMetric(t *testing.T, collected []metricdata.Metrics, name string) metricdata.Metrics {
	t.Helper()

	for _, m := range collected {
		if m.Name == name {
			return m
		}
	}

	names := make([]string, 0, len(collected))
	for _, m := range collected {
		names = append(names, m.Name)
	}
	t.Fatalf("instrument %q was never recorded; collected: %v", name, names)

	return metricdata.Metrics{}
}

// attributeSets returns every data point's attributes, as maps, for whichever
// aggregation the instrument carries.
func attributeSets(t *testing.T, m metricdata.Metrics) []map[string]string {
	t.Helper()

	var sets []map[string]string

	asMap := func(set attribute.Set) map[string]string {
		out := map[string]string{}
		for _, kv := range set.ToSlice() {
			out[string(kv.Key)] = kv.Value.String()
		}
		return out
	}

	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			sets = append(sets, asMap(dp.Attributes))
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			sets = append(sets, asMap(dp.Attributes))
		}
	default:
		t.Fatalf("instrument %q has an aggregation this helper does not read: %T", m.Name, m.Data)
	}

	return sets
}

// TestPluginCallMetricsMoveAndCarryTheSchemasAttributes drives a real plugin
// process through a real task call and a real health check, then asserts that
// each instrument moved and carries exactly the attributes the schema permits
// — exactly, because an extra attribute is a cardinality decision nobody made
// and a missing one is a dashboard that cannot group.
func TestPluginCallMetricsMoveAndCarryTheSchemasAttributes(t *testing.T) {
	t.Parallel()

	provider, collect := collectMetrics(t)

	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.MeterProvider = provider

	host := openHost(t, cfg)

	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	_, err := defs[0].Fn(t.Context(), nil, nil)
	require.NoError(t, err)

	p, ok := host.Lookup("ok")
	require.True(t, ok)
	require.Equal(t, HealthServing, p.CheckHealth(t.Context()).Status)

	collected := collect()

	// The counter of plugin RPCs, on the task-execute path: the task name is
	// present because a plugin operation that runs a task has one.
	calls := findMetric(t, collected, "flowstate.plugin.calls")
	require.Contains(t, attributeSets(t, calls), map[string]string{
		metricschema.PluginName:      "ok",
		metricschema.PluginOperation: "execute",
		metricschema.TaskName:        "ok.ok_task",
		metricschema.PluginOutcome:   "success",
	}, "the execute call must be counted with the schema's attributes and no others")

	// The same operation's latency, under the same attributes. A histogram
	// that moved but under different labels than its sibling counter is a
	// dashboard whose two panels disagree.
	duration := findMetric(t, collected, "flowstate.plugin.operation.duration")
	require.Equal(t, "s", duration.Unit)
	require.Contains(t, attributeSets(t, duration), map[string]string{
		metricschema.PluginName:      "ok",
		metricschema.PluginOperation: "execute",
		metricschema.TaskName:        "ok.ok_task",
		metricschema.PluginOutcome:   "success",
	})

	// The health path records two instruments, and neither carries a task
	// name, because a health check is not about a task.
	require.Contains(t, attributeSets(t, findMetric(t, collected, "flowstate.plugin.calls")), map[string]string{
		metricschema.PluginName:      "ok",
		metricschema.PluginOperation: "health",
		metricschema.PluginOutcome:   "success",
	})
	require.Contains(t, attributeSets(t, findMetric(t, collected, "flowstate.plugin.health.checks")), map[string]string{
		metricschema.PluginName:         "ok",
		metricschema.PluginHealthStatus: "serving",
	})

	// Every data point of every instrument, over the whole batch: nothing
	// carries a key the schema does not name. This is the shape that catches a
	// label added six months from now next to one of these, which a test
	// asserting only its own expected set would step straight past.
	for _, m := range collected {
		if !strings.HasPrefix(m.Name, "flowstate.") {
			continue
		}
		for _, set := range attributeSets(t, m) {
			for key := range set {
				_, permitted := metricschema.Classification(key)
				require.True(t, permitted,
					"instrument %q carries %q, which the metric schema does not permit", m.Name, key)
			}
		}
	}
}

// TestFailedPluginCallIsCountedAsAnErrorWithoutItsMessage is the outcome
// classification half, and the metric-side mirror of this package's span
// containment tests: a failure must be countable, and the failure's own words
// must not become a label. An error message is peer-written text of unbounded
// length and unbounded distinctness — the two properties a metric label must
// never have — quite apart from what it might quote.
func TestFailedPluginCallIsCountedAsAnErrorWithoutItsMessage(t *testing.T) {
	t.Parallel()

	provider, collect := collectMetrics(t)

	cfg := testConfig(t, pluginDir(t, "permanent"))
	cfg.MeterProvider = provider

	host := openHost(t, cfg)

	_, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
	require.Error(t, err)

	sets := attributeSets(t, findMetric(t, collect(), "flowstate.plugin.calls"))
	require.Contains(t, sets, map[string]string{
		metricschema.PluginName:      "permanent",
		metricschema.PluginOperation: "execute",
		metricschema.TaskName:        "permanent.permanent_task",
		metricschema.PluginOutcome:   "error",
	})

	for _, set := range sets {
		for _, value := range set {
			require.NotContains(t, value, "permanent trouble",
				"the plugin's own error text reached a metric label")
		}
	}
}

// TestAnOverlongAttributeValueCollapsesOnTheRealRecordingPath proves the value
// bound where it actually has to hold: on the path plugin telemetry records
// through, not on the schema in isolation.
//
// A plugin name is chosen by the deployment rather than by an attacker, so
// this is the backstop rather than the front line — but a bound nothing
// reaches is a bound nothing tests (CLAUDE.md), and the whole point of the
// backstop is the day a value turns out not to have been bounded by
// configuration after all.
func TestAnOverlongAttributeValueCollapsesOnTheRealRecordingPath(t *testing.T) {
	t.Parallel()

	provider, collect := collectMetrics(t)

	tel := newTelemetry(Config{MeterProvider: provider})

	overlong := strings.Repeat("x", metricschema.MaxValueLength+1)

	_, _, finish := tel.start(t.Context(), "call", overlong, "task")
	finish(nil)

	calls := findMetric(t, collect(), "flowstate.plugin.calls")

	sets := attributeSets(t, calls)
	require.Len(t, sets, 1)
	require.Equal(t, metricschema.OverflowValue, sets[0][metricschema.PluginName],
		"an overlong value must collapse to the sentinel, so the series stops splitting")

	// The measurement itself survives the collapse. Dropping it would let a
	// bad value erase an operator's signal, which is the denial of service the
	// bound exists to prevent, arrived at from the other direction.
	sum, ok := calls.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(1), sum.DataPoints[0].Value)
}
