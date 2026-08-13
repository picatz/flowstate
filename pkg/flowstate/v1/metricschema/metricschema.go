// Package metricschema decides which attribute keys may reach a metric
// instrument, and bounds what their values may be.
//
// # Why this is a package and not a comment
//
// A metric label set is a resource an operator pays for: most backends mint
// one time series per distinct combination of label values, so an unbounded
// label is a denial of service against the operator's own metrics store,
// executed by whoever chooses the value. CLAUDE.md's rule for every other
// bound in this repository applies unchanged here — ask which resource the
// far side controls, then bound that resource — and the resource a peer
// controls in a metric is the set of label values.
//
// The values that reach telemetry in this system fall into three classes,
// and only the first two are safe as labels:
//
//   - Bounded by construction: a fixed enumeration written in this repository.
//     An outcome is "success" or "error"; a health status is one of the
//     [flowstate.plugin.health.status] values. Nobody outside this module can
//     add one. See [ClassConstruction].
//   - Bounded by configuration: chosen by the deployment — the plugins it
//     installs, the tasks it registers, the tenants it provisions. The set
//     grows only when an operator or an author changes the deployment, never
//     when a request arrives. See [ClassConfiguration].
//   - Unbounded and peer-controlled: minted per event by somebody else. A
//     webhook delivery id is chosen by the external sender, one per delivery;
//     a run id is minted per execution. These belong on a span and a log line,
//     which is where a per-event identifier is useful anyway, and never on a
//     metric. See [NeverKeys].
//
// The third class is not merely discouraged here. [Attributes] filters to a
// named allowlist, so a key that is not in [Keys] cannot reach an instrument
// through this package at all, and TestEveryMetricRecordingSiteGoesThroughTheSchema
// asserts that no recording site in the repository bypasses it. That is the
// difference between a bound and a request to be careful: adding a label in
// six months requires editing this file, where the classification is written
// down, rather than remembering a comment somewhere else.
//
// # What happens when a bound is exceeded
//
//   - A key outside the allowlist: the attribute is dropped and the
//     measurement is still recorded, unlabeled by that key. Dropping the
//     measurement instead would let anyone who can influence one label erase
//     an operator's signal — a different denial of service, reached by trying
//     to prevent the first one.
//   - A value longer than [MaxValueLength], or a distinct value beyond
//     [MaxValuesPerKey] for its key: the value is replaced by the
//     [OverflowValue] sentinel and counting continues under it. The series
//     stops splitting; the measurement is not lost. An operator reading
//     "other" learns that a bound was reached, which is a fact worth seeing.
//   - A non-string or empty value: dropped, as above. The bounds here are
//     expressed over strings, and a bound that does not know how to measure
//     its input is not a bound.
//
// Author-chosen and attacker-chosen are different risk classes, and this
// package treats them differently on purpose. A task name is written by
// whoever writes the deployment's workflows; getting a bad one requires
// already being able to change what the deployment runs. A delivery id
// arrives in an HTTP request from a stranger. Both are bounded here, because
// [MaxValuesPerKey] applies to every allowlisted key, but only the second
// class is refused outright — an author who wants a per-task metric should
// have one.
package metricschema

import (
	"sort"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Class is how the value set behind an attribute key is bounded.
type Class int

const (
	// ClassConstruction is a fixed enumeration defined in this repository.
	// Its cardinality is a property of the source code.
	ClassConstruction Class = iota + 1

	// ClassConfiguration is chosen by the deployment: an installed plugin, a
	// registered task, a provisioned tenant. Its cardinality grows only when
	// somebody changes the deployment, and never in response to a request.
	ClassConfiguration
)

// String names the class for diagnostics and generated documentation.
func (c Class) String() string {
	switch c {
	case ClassConstruction:
		return "bounded by construction"
	case ClassConfiguration:
		return "bounded by configuration"
	default:
		return "unknown"
	}
}

// The attribute keys permitted on a metric instrument.
//
// These spellings are deliberately identical to the span attribute keys for
// the same concepts, per #522's first invariant: one concept, one spelling,
// across spans, logs and metrics. The recording sites use these constants for
// both the span and the metric so the two cannot drift apart — one constant
// cannot disagree with itself.
const (
	// PluginName is the name of a plugin as the deployment installed it.
	PluginName = "flowstate.plugin.name"

	// PluginOperation is the host-side plugin operation: a fixed set of
	// verbs written in this package's caller ("call", "health", "start").
	PluginOperation = "flowstate.plugin.operation"

	// PluginOutcome is "success" or "error".
	PluginOutcome = "flowstate.plugin.outcome"

	// PluginHealthStatus is one of the plugin health statuses.
	PluginHealthStatus = "flowstate.plugin.health.status"

	// TaskName is the name of a task as the deployment registered it.
	TaskName = "flowstate.task.name"
)

// allowed is the schema: every key a metric instrument may carry, with the
// reason its value set is bounded. A key absent from this map is dropped by
// [Attributes], so adding a label means adding it here, next to the
// classification that justifies it.
var allowed = map[string]Class{
	PluginName:         ClassConfiguration,
	PluginOperation:    ClassConstruction,
	PluginOutcome:      ClassConstruction,
	PluginHealthStatus: ClassConstruction,
	TaskName:           ClassConfiguration,
}

// NeverKeys are identifiers that exist elsewhere in this system's telemetry
// and must never become metric attributes. They are listed rather than merely
// omitted so the refusal is a statement someone can read and test, not an
// accident of what nobody has added yet.
//
// Each is minted per event by somebody other than the deployment:
// flowstate.delivery.id is chosen by an external webhook sender, one per
// delivery, so a stranger with an HTTP client chooses this system's metric
// cardinality; the run and execution identifiers are minted per execution, so
// a busy deployment mints thousands an hour. All of them are useful on a span
// and a log line, where a per-event identifier is the point.
var NeverKeys = []string{
	"flowstate.delivery.id",
	"flowstate.run.id",
	"flowstate.execution.id",
	"flowstate.workflow.run_id",
}

// Keys returns the allowlisted attribute keys, sorted. It exists for
// documentation and for tests that assert the schema's shape.
func Keys() []string {
	keys := make([]string, 0, len(allowed))
	for key := range allowed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Classification reports how the value set behind key is bounded, and whether
// the key is permitted on a metric at all.
func Classification(key string) (Class, bool) {
	class, ok := allowed[key]
	return class, ok
}

const (
	// MaxValueLength is the longest attribute value that reaches an
	// instrument unchanged. Longer values become [OverflowValue].
	MaxValueLength = 128

	// MaxValuesPerKey is how many distinct values one key may contribute
	// before the rest collapse into [OverflowValue]. It is generous relative
	// to the number of plugins or tasks any deployment installs, because the
	// bound is a backstop against a value that turned out not to be bounded
	// by configuration after all, not a limit anyone should reach.
	MaxValuesPerKey = 128

	// OverflowValue is the sentinel a value collapses to when it exceeds a
	// bound. Counting continues under it, so the measurement survives and the
	// series stops splitting.
	OverflowValue = "other"
)

// Limiter enforces the schema and remembers which values it has already let
// through, so that [MaxValuesPerKey] can be counted per key.
//
// The remembered set only grows, which is correct for the classes of value
// this schema permits: a deployment's plugins and tasks are fixed for the
// life of the process. It is deliberately a type rather than only a package
// global so a test can hold its own and assert the overflow behaviour without
// depending on what any other test recorded.
type Limiter struct {
	mu        sync.Mutex
	maxValues int
	seen      map[string]map[string]struct{}
}

// NewLimiter returns a limiter admitting maxValuesPerKey distinct values for
// each allowlisted key before collapsing the rest to [OverflowValue].
func NewLimiter(maxValuesPerKey int) *Limiter {
	return &Limiter{maxValues: maxValuesPerKey, seen: map[string]map[string]struct{}{}}
}

// defaultLimiter is what the package-level [Attributes] and [WithAttributes]
// use, so every recording site in the process shares one cardinality budget.
var defaultLimiter = NewLimiter(MaxValuesPerKey)

// Attributes filters attrs to the schema and bounds their values, per the
// rules in the package documentation.
func Attributes(attrs ...attribute.KeyValue) []attribute.KeyValue {
	return defaultLimiter.Attributes(attrs...)
}

// WithAttributes is [Attributes] as a measurement option, which is how a
// recording site should spell it: every instrument in this repository records
// through this function rather than through metric.WithAttributes, so no path
// reaches an instrument without passing the schema.
func WithAttributes(attrs ...attribute.KeyValue) metric.MeasurementOption {
	return defaultLimiter.WithAttributes(attrs...)
}

// Attributes filters attrs to the schema and bounds their values.
func (l *Limiter) Attributes(attrs ...attribute.KeyValue) []attribute.KeyValue {
	bounded := make([]attribute.KeyValue, 0, len(attrs))

	for _, attr := range attrs {
		key := string(attr.Key)
		if _, ok := allowed[key]; !ok {
			continue
		}
		if attr.Value.Type() != attribute.STRING {
			continue
		}
		value := attr.Value.AsString()
		if value == "" {
			continue
		}
		bounded = append(bounded, attribute.String(key, l.boundValue(key, value)))
	}

	return bounded
}

// WithAttributes is [Limiter.Attributes] as a measurement option.
func (l *Limiter) WithAttributes(attrs ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributes(l.Attributes(attrs...)...)
}

// boundValue returns the value to record for key: the value itself while it
// is short enough and the key's distinct-value budget holds, and
// [OverflowValue] once either bound is reached.
func (l *Limiter) boundValue(key, value string) string {
	if len(value) > MaxValueLength {
		return OverflowValue
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	values, ok := l.seen[key]
	if !ok {
		values = map[string]struct{}{}
		l.seen[key] = values
	}
	if _, ok := values[value]; ok {
		return value
	}
	if len(values) >= l.maxValues {
		return OverflowValue
	}
	values[value] = struct{}{}

	return value
}
