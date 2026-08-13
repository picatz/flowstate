package metricschema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// What this file is for.
//
// The positive direction — a counter moved, carrying the attributes it claims
// — lives next to the instruments, in plugin/telemetry_metrics_test.go. What
// lives here is the direction that actually guards something: that a value
// somebody else chose *cannot* reach a metric label. CLAUDE.md's "test that A
// cannot reach B" section is about exactly this: a test asserting each party
// reaches its own resource is a functionality test wearing a security test's
// clothes, and the assertion worth writing is the negative one.
//
// The end-to-end tests here run a real SDK meter over a manual reader, so what
// they assert is what a collector would have received, not what a filter
// function returned.

// theDeliveryID stands in for the worst case #526 names: an identifier minted
// by an external webhook sender, one per delivery, already carried on a span.
// A stranger with an HTTP client chooses it, which makes it the one value in
// this system that most directly buys an operator's metrics backend a new time
// series per request.
const theDeliveryID = "01J8ZK4Q1WQZ9V4ZC0PEER-CHOSEN-DELIVERY-ID"

// recordThrough records one measurement on a counter through the schema and
// returns the collected data points' attribute sets.
func recordThrough(t *testing.T, attrs ...attribute.KeyValue) []map[string]string {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	counter, err := provider.Meter("metricschema_test").Int64Counter("flowstate.test.counter")
	require.NoError(t, err)

	counter.Add(t.Context(), 1, metricschema.WithAttributes(attrs...))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var sets []map[string]string
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				set := map[string]string{}
				for _, kv := range dp.Attributes.ToSlice() {
					set[string(kv.Key)] = kv.Value.Emit()
				}
				sets = append(sets, set)
			}
		}
	}

	return sets
}

// TestAPeerChosenIdentifierCannotReachAMetricLabel is the point of the
// exercise. It records through the schema with a delivery id attached, the way
// a well-meaning change six months from now would attach one, and asserts that
// what the reader collects carries no trace of it.
//
// Delete the allowlist check from Limiter.Attributes and this fails: the key
// arrives at the instrument and the collected data point carries it.
func TestAPeerChosenIdentifierCannotReachAMetricLabel(t *testing.T) {
	t.Parallel()

	sets := recordThrough(t,
		attribute.String(metricschema.PluginName, "example"),
		attribute.String("flowstate.delivery.id", theDeliveryID),
	)

	require.Len(t, sets, 1)
	require.Equal(t, map[string]string{metricschema.PluginName: "example"}, sets[0],
		"only the allowlisted key may reach the instrument")

	for _, set := range sets {
		for key, value := range set {
			require.NotContains(t, value, theDeliveryID,
				"a peer-chosen delivery id reached a metric label under %q", key)
		}
	}
}

// TestNoDeniedKeyReachesAnInstrument runs the same probe over every key the
// schema names as never permitted, and over a couple of shapes nobody has
// thought of yet — a wire-format spelling and a plain unknown key — because
// the allowlist has to refuse what is not on it, not merely what is on the
// denied list.
func TestNoDeniedKeyReachesAnInstrument(t *testing.T) {
	t.Parallel()

	probes := metricschema.NeverKeys()
	probes = append(probes, "delivery_id", "run_id", "workflow_id", "http.url", "flowstate.secret.name", "")

	for _, key := range probes {
		sets := recordThrough(t,
			attribute.String(metricschema.PluginName, "example"),
			attribute.String(key, theDeliveryID),
		)

		require.Len(t, sets, 1)
		require.Equal(t, map[string]string{metricschema.PluginName: "example"}, sets[0],
			"key %q reached an instrument", key)
	}
}

// TestOneUnpermittedKeyDoesNotCostTheMeasurement states the exceed behaviour
// as an assertion rather than only in prose: the offending attribute is
// dropped and the measurement is still recorded. Dropping the data point would
// hand anyone who can influence one label the power to erase an operator's
// signal — the same denial of service, reached by trying to prevent it.
func TestOneUnpermittedKeyDoesNotCostTheMeasurement(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	counter, err := provider.Meter("metricschema_test").Int64Counter("flowstate.test.counter")
	require.NoError(t, err)

	for range 3 {
		counter.Add(t.Context(), 1, metricschema.WithAttributes(
			attribute.String(metricschema.PluginOutcome, "success"),
			attribute.String("flowstate.delivery.id", theDeliveryID),
		))
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)

	want := metricdata.Metrics{
		Name: "flowstate.test.counter",
		Data: metricdata.Sum[int64]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints: []metricdata.DataPoint[int64]{{
				Attributes: attribute.NewSet(attribute.String(metricschema.PluginOutcome, "success")),
				Value:      3,
			}},
		},
	}

	metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0].Metrics[0], metricdatatest.IgnoreTimestamp())
}

// TestTheOverflowSentinelReplacesValuesBeyondTheBound covers the other exceed
// behaviour: an allowlisted key whose value set turns out to be larger than
// the bound keeps counting, under the sentinel, instead of splitting the
// series without limit.
//
// It uses its own limiter rather than the package default, so what it asserts
// does not depend on what any other test in this binary recorded first.
func TestTheOverflowSentinelReplacesValuesBeyondTheBound(t *testing.T) {
	t.Parallel()

	limiter := metricschema.NewLimiter(3)

	seen := map[string]int{}
	for i := range 10 {
		bounded := limiter.Attributes(attribute.String(metricschema.TaskName, "task-"+string(rune('a'+i))))
		require.Len(t, bounded, 1)
		seen[bounded[0].Value.AsString()]++
	}

	require.Len(t, seen, 4, "three distinct values plus the sentinel")
	require.Equal(t, 7, seen[metricschema.OverflowValue], "everything past the bound collapses, and still counts")

	// A value already admitted keeps its own series after the bound is
	// reached; the bound is on how many distinct values are admitted, not on
	// how many measurements they carry.
	bounded := limiter.Attributes(attribute.String(metricschema.TaskName, "task-a"))
	require.Equal(t, "task-a", bounded[0].Value.AsString())

	// And the bound is per key: exhausting one key's budget does not spend
	// another's.
	bounded = limiter.Attributes(attribute.String(metricschema.PluginName, "some-plugin"))
	require.Equal(t, "some-plugin", bounded[0].Value.AsString())
}

// TestValuesThatCannotBeBoundedAreDropped covers the shapes the bounds are not
// expressed over: a non-string value has no length to measure, and an empty
// value labels nothing.
func TestValuesThatCannotBeBoundedAreDropped(t *testing.T) {
	t.Parallel()

	require.Empty(t, metricschema.Attributes(
		attribute.Int(metricschema.TaskName, 7),
		attribute.Bool(metricschema.PluginOutcome, true),
		attribute.String(metricschema.PluginName, ""),
	))
}

// TestOverlongValuesCollapseWithoutBeingAdmitted checks the length bound, and
// the property that makes it worth having: an overlong value must not consume
// the key's distinct-value budget on its way to the sentinel, or a peer able
// to send long values could exhaust the budget for the legitimate ones.
func TestOverlongValuesCollapseWithoutBeingAdmitted(t *testing.T) {
	t.Parallel()

	limiter := metricschema.NewLimiter(2)

	for range 50 {
		bounded := limiter.Attributes(attribute.String(metricschema.TaskName,
			strings.Repeat("x", metricschema.MaxValueLength+1)))
		require.Equal(t, metricschema.OverflowValue, bounded[0].Value.AsString())
	}

	for _, name := range []string{"first", "second"} {
		bounded := limiter.Attributes(attribute.String(metricschema.TaskName, name))
		require.Equal(t, name, bounded[0].Value.AsString(),
			"overlong values must not have spent the budget the real ones need")
	}
}

// TestTheSchemaClassifiesEveryKeyItPermits keeps the schema and its stated
// reasoning together: a key is in the allowlist because its value set is
// bounded by construction or by configuration, and a key with no
// classification is a label nobody justified.
func TestTheSchemaClassifiesEveryKeyItPermits(t *testing.T) {
	t.Parallel()

	keys := metricschema.Keys()
	require.NotEmpty(t, keys)

	for _, key := range keys {
		class, ok := metricschema.Classification(key)
		require.True(t, ok)
		require.Contains(t, []metricschema.Class{
			metricschema.ClassConstruction,
			metricschema.ClassConfiguration,
		}, class, "key %q has no classification", key)

		// One attribute schema (#522, invariant 1): a metric key is spelled
		// the way the span attribute for the same concept is spelled — dotted
		// and prefixed — not the way the wire and CEL surfaces spell theirs
		// (run_id, workflow_id, delivery_id).
		require.True(t, strings.HasPrefix(key, "flowstate."),
			"metric attribute %q must use the telemetry-side spelling", key)
		require.NotContains(t, key, "_", "metric attribute %q must not use the wire-format spelling", key)
	}

	// One table, every key in it, each carrying its own classification and the
	// name of whoever chooses its values. This is the shape a generator could
	// emit verbatim once telemetry attributes are declared on the schema, and
	// the reason the classification is a field rather than an implication of
	// which list a key sits in.
	require.Len(t, metricschema.Table, len(keys)+len(metricschema.NeverKeys()),
		"the table is the only place a key is declared")

	for _, attr := range metricschema.Table {
		require.NotEmpty(t, attr.Key)
		require.NotEmpty(t, attr.Chooser, "%q must say who chooses its values", attr.Key)
		require.NotEqual(t, "unknown", attr.Class.String(), "%q has no classification", attr.Key)
		require.True(t, strings.HasPrefix(attr.Key, "flowstate."),
			"%q must be namespaced, dotted, and prefixed the way OpenTelemetry spells an attribute", attr.Key)
		require.Equal(t, strings.ToLower(attr.Key), attr.Key,
			"%q must be lowercase, per OpenTelemetry naming", attr.Key)

		permitted := attr.Class != metricschema.ClassPeerControlled
		_, allowlisted := metricschema.Classification(attr.Key)
		require.Equal(t, permitted, allowlisted,
			"the allowlist must be derived from the table's classification, not maintained beside it")
	}

	for _, key := range metricschema.NeverKeys() {
		_, ok := metricschema.Classification(key)
		require.False(t, ok, "%q is named as never permitted and must not also be allowlisted", key)
	}
}

// TestEveryMetricRecordingSiteGoesThroughTheSchema is the guard that makes the
// allowlist a bound rather than a convention.
//
// A filter nothing is obliged to call protects only the call sites that
// remembered it, and the failure mode #526 names is precisely somebody adding
// a label in six months without reading the comment. So this walks the
// repository and requires that metric.WithAttributes — the way to attach
// attributes to a measurement while bypassing the schema entirely — appears
// nowhere outside the schema package itself.
func TestEveryMetricRecordingSiteGoesThroughTheSchema(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)

	bypasses := []string{"metric.WithAttributes(", "metric.WithAttributeSet("}

	var offenders []string

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		// The schema package is where the bypass is spelled once, deliberately,
		// underneath the filter.
		if strings.Contains(filepath.ToSlash(path), "/pkg/flowstate/v1/metricschema/") {
			return nil
		}

		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, bypass := range bypasses {
			if strings.Contains(string(contents), bypass) {
				rel, _ := filepath.Rel(root, path)
				offenders = append(offenders, rel+": "+bypass)
			}
		}
		return nil
	}))

	require.Empty(t, offenders,
		"these record metric attributes without passing the schema; use metricschema.WithAttributes so the "+
			"allowlist and the cardinality bound apply")
}

// repoRoot walks up from the test's directory to the go.mod root.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "walked to the filesystem root without finding go.mod")
		dir = parent
	}

	t.Fatal("go.mod not found within ten directories of the test")

	return ""
}
