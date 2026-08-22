package conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// Shared cases for #526's first slice: the `flowstate.*` metrics a run records,
// run against both execution drivers — `flowstatev1_test.TestRunWorkflowTaskMetrics`
// locally and `engine.TestRunWorkflowTaskMetrics` durably.
//
// # Why this is a shared case and not two
//
// The same argument [AssertTaskSpans] makes about traces, one signal over. A
// dashboard is built once and read against whatever ran; if a local run named
// its instrument one thing and a durable run another, an author rehearsing a
// change would be looking at an empty panel and concluding the change was fine.
// So the claim below is that both drivers record the *same instruments* with the
// *same attribute keys*, and differ in exactly one value — [metricschema.Driver],
// which exists to be the difference.
//
// # And the negative direction, which is the half that guards something
//
// Every collected attribute is held to the declaration in [metricschema.Instruments]:
// a key nobody declared fails here, which is what stops the next well-meaning
// change from labelling by run id, by workflow name, or by a URL. CLAUDE.md's
// "test that A cannot reach B" applies to a metric label as squarely as to a
// tenant boundary — asserting that the label you added shows up is a
// functionality test, and the assertion worth writing is that nothing else does.

// RecordMetrics installs a manual-reader meter provider for the duration of a
// test and returns the reader.
//
// The global provider, because that is where both drivers' instruments look —
// [v1.ObserveTask] reads `otel.GetMeterProvider()` per execution, for the reason
// its doc gives — and restored afterwards, since these tests share a binary with
// every other test in their package.
//
// A [sdkmetric.ManualReader] rather than a periodic one, deliberately: it
// collects when asked, so nothing here depends on an export interval and there
// is no window in which a test can be flaky by being early. #401 names this as
// the mechanism, and this is its first use outside the metricschema package.
func RecordMetrics(tb testing.TB) *sdkmetric.ManualReader {
	tb.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)

	tb.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return reader
}

// TaskMetricWorkflow returns the workflow both drivers run for the metric
// comparison.
//
// The span case's workflow, on purpose: three task executions from two steps,
// so a driver that recorded one measurement per *step* rather than per execution
// disagrees with the other about the count — the same distinction
// [ExpectedTaskSpans] pins for the trace, now pinned for the number an operator
// divides an error rate out of. Reusing it also means the secret carried in a
// task input is carried here too, so the containment assertion below is over the
// same material the span case hides.
func TaskMetricWorkflow() *v1.Workflow {
	return TaskSpanWorkflow()
}

// ExpectedTaskExecutions is how many task executions a [TaskMetricWorkflow] run
// performs, and therefore what both instruments must have counted.
const ExpectedTaskExecutions = 3

// Point is one collected measurement reduced to what both drivers must say the
// same way: its attributes, and how many events it aggregated.
//
// Exported because the assertions a single driver makes about its own
// instruments — a denial counter, say — read the same collection, and a second
// reader of a metricdata.ResourceMetrics is a second thing to get wrong.
type Point struct {
	Attributes map[string]string
	Count      uint64
}

// AssertTaskMetrics is the shared assertion both drivers make.
//
// driver is the value that side is expected to record for
// [metricschema.Driver] — the one thing the two are allowed to disagree about,
// asserted rather than ignored so that a driver recording the *other* one's
// label is a failure and not a silent mislabelling of production as a rehearsal.
func AssertTaskMetrics(tb testing.TB, reader *sdkmetric.ManualReader, driver string, outputs *v1.Workflow_StepOutputs, err error) {
	tb.Helper()

	if err != nil {
		tb.Fatalf("the run failed: %v", err)
	}
	if want := TaskSpanExpectedOutputs(); !proto.Equal(want, outputs) {
		tb.Fatalf("the run produced %v, want %v", outputs, want)
	}

	collected := collectFlowstateMetrics(tb, reader)

	// Both instruments, or the operator has half the pair: a rate with no
	// latency, or a latency with no denominator.
	for _, name := range []string{
		metricschema.InstrumentTaskExecutions,
		metricschema.InstrumentTaskDuration,
	} {
		points, ok := collected[name]
		if !ok {
			tb.Fatalf("the run recorded nothing on %s — every instrument the run touched: %v",
				name, sortedKeys(collected))
		}

		if len(points) != 1 {
			tb.Fatalf("%s recorded %d attribute sets, want one — %v", name, len(points), points)
		}

		point := points[0]
		if point.Count != ExpectedTaskExecutions {
			tb.Fatalf("%s counted %d task executions, want %d — a driver measuring per step rather than per execution counts %d",
				name, point.Count, ExpectedTaskExecutions, 2)
		}

		want := map[string]string{
			metricschema.TaskName:    "log",
			metricschema.TaskOutcome: metricschema.OutcomeSuccess,
			metricschema.Driver:      driver,
		}
		if !sameAttributes(point.Attributes, want) {
			tb.Fatalf("%s carries %v, want %v", name, point.Attributes, want)
		}
	}

	assertDeclaredAttributesOnly(tb, collected)
}

// AssertNoMetrics requires that this repository's own instruments recorded
// nothing — the zero-config claim, asserted by installing the reader *after* the
// run so that whatever the run recorded went to the no-op provider.
//
// Scoped to the `flowstate.` namespace for the durable driver's sake: a run
// through Temporal's test environment can bring the SDK's own instruments along,
// and those are not this claim's subject.
func AssertNoMetrics(tb testing.TB, reader *sdkmetric.ManualReader) {
	tb.Helper()

	if collected := collectFlowstateMetrics(tb, reader); len(collected) != 0 {
		tb.Fatalf("a run with no meter provider configured recorded %v", sortedKeys(collected))
	}
}

// assertDeclaredAttributesOnly is the negative direction.
//
// Every attribute key on every collected point has to be one the instrument
// *declares* in [metricschema.Instruments], and no key may be one the schema
// refuses outright ([metricschema.NeverKeys]) — a run id, an execution id, a
// webhook delivery id. The first check is the one that fires when somebody adds
// a label; the second is the one that says why they must not.
func assertDeclaredAttributesOnly(tb testing.TB, collected map[string][]Point) {
	tb.Helper()

	refused := map[string]struct{}{}
	for _, key := range metricschema.NeverKeys() {
		refused[key] = struct{}{}
	}

	for name, points := range collected {
		declared, ok := metricschema.InstrumentByName(name)
		if !ok {
			tb.Fatalf("the run recorded %s, which is not declared in metricschema.Instruments — declare it there so one file lists what this system emits",
				name)
		}

		allowed := map[string]struct{}{}
		for _, key := range declared.Keys {
			allowed[key] = struct{}{}
		}

		for _, point := range points {
			for key, value := range point.Attributes {
				if _, no := refused[key]; no {
					tb.Fatalf("%s carries %q, which metricschema refuses on any instrument: it is minted per event, so one value is one time series",
						name, key)
				}
				if _, ok := allowed[key]; !ok {
					tb.Fatalf("%s carries %q, which it does not declare — add it to metricschema.Instruments, or stop recording it",
						name, key)
				}
				if strings.Contains(value, TaskSpanSecret) {
					tb.Fatalf("an input value reached a metric label on %s, which is exported to a collector", name)
				}
			}
		}
	}
}

// CollectFlowstateMetrics reads the reader and returns this repository's own
// instruments, keyed by name — the collection every metric assertion in this
// repository reads, shared so nobody writes a second walk of a
// metricdata.ResourceMetrics.
func CollectFlowstateMetrics(tb testing.TB, reader *sdkmetric.ManualReader) map[string][]Point {
	tb.Helper()

	return collectFlowstateMetrics(tb, reader)
}

// collectFlowstateMetrics reads the reader and reduces what it holds to this
// repository's own instruments, keyed by name.
//
// Instruments outside the `flowstate.` namespace are ignored, exactly as the
// span case ignores spans outside it: a durable run also carries Temporal's own
// SDK metrics, and those are somebody else's vocabulary.
func collectFlowstateMetrics(tb testing.TB, reader *sdkmetric.ManualReader) map[string][]Point {
	tb.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		tb.Fatalf("collecting metrics failed: %v", err)
	}

	out := map[string][]Point{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if !strings.HasPrefix(m.Name, "flowstate.") {
				continue
			}

			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], Point{
						Attributes: attributeMap(dp.Attributes.ToSlice()),
						Count:      uint64(dp.Value),
					})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], Point{
						Attributes: attributeMap(dp.Attributes.ToSlice()),
						Count:      dp.Count,
					})
				}
			default:
				tb.Fatalf("%s collected as %T, which this case does not know how to read", m.Name, m.Data)
			}
		}
	}

	return out
}

// attributeMap flattens one point's attributes for comparison.
func attributeMap(attrs []attribute.KeyValue) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		// Value.String rather than AsString, so that a non-string value — which
		// the schema drops, and which therefore should never appear — would
		// show up in a failure message as itself rather than as "".
		out[string(attr.Key)] = attr.Value.String()
	}

	return out
}

// sameAttributes compares two flattened attribute sets.
func sameAttributes(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}

	return true
}

// sortedKeys names what was collected, for a failure message that says what the
// run *did* record rather than only what it did not.
func sortedKeys(collected map[string][]Point) []string {
	names := make([]string, 0, len(collected))
	for name := range collected {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// String renders one point for a failure message.
func (p Point) String() string {
	return fmt.Sprintf("%v x%d", p.Attributes, p.Count)
}
