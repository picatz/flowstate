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
//     [OverflowValue] learns that a bound was reached, which is a fact worth
//     seeing.
//   - A non-string or empty value: dropped, as above. The bounds here are
//     expressed over strings, and a bound that does not know how to measure
//     its input is not a bound.
//
// # Naming
//
// Keys follow OpenTelemetry's convention: lowercase, dot-separated namespaces,
// most general segment first, no redundant prefix repeated inside a namespace.
// Every key in [Table] satisfies it, with two observations recorded here rather
// than acted on, because renaming a key breaks every dashboard already built on
// it and that migration should be planned once rather than smuggled in:
//
//   - flowstate.workflow.run_id and flowstate.run.id are two spellings of one
//     concept. Both are refused as metric labels either way, so nothing here
//     depends on the answer, but whichever survives should be the only one.
//   - flowstate.plugin.outcome describes the outcome of a plugin *operation*,
//     so flowstate.plugin.operation.outcome would nest it the way
//     flowstate.plugin.health.status nests under its own subject. It reads as
//     a plugin-level property today and is not one.
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

	// ClassPeerControlled is minted per event by somebody other than the
	// deployment — a webhook sender's delivery id, a generated run id. It is
	// unbounded by definition and may never reach an instrument; it belongs on
	// a span and a log line, where a per-event identifier is the point.
	ClassPeerControlled
)

// String names the class for diagnostics and generated documentation.
func (c Class) String() string {
	switch c {
	case ClassConstruction:
		return "bounded by construction"
	case ClassConfiguration:
		return "bounded by configuration"
	case ClassPeerControlled:
		return "unbounded and peer-controlled"
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

	// PluginOperation is the host-side plugin operation: a fixed set of verbs
	// written in this package's caller — "launch", "start", "health" and
	// "execute". TestEveryPluginOperationIsRecordedUnderItsOwnName in the
	// plugin package drives all four and asserts the set, so this list and the
	// recording sites cannot drift apart silently.
	PluginOperation = "flowstate.plugin.operation"

	// PluginOutcome is "success" or "error".
	PluginOutcome = "flowstate.plugin.outcome"

	// PluginHealthStatus is one of the plugin health statuses.
	PluginHealthStatus = "flowstate.plugin.health.status"

	// TaskName is the name of a task as the deployment registered it.
	TaskName = "flowstate.task.name"
)

// Attribute is one row of the schema: a key, how its value set is bounded, and
// who chooses the value.
//
// Everything else here is derived from these rows — the allowlist, the refusal
// list, the documentation, and whatever a dashboard registry eventually wants.
// The classification is a *field* rather than an implication of which list a
// key sits in, precisely so this table can be emitted rather than typed: the
// owner has raised declaring telemetry attributes on the schema with custom
// options, the way protovalidate declares validation rules, and if that lands
// [Table] becomes generated output with nothing else in this package changing.
// A generator can produce a table; it cannot produce a habit of remembering.
type Attribute struct {
	// Key is the attribute key as it is spelled on a span, a log field, and a
	// metric label alike (#522, invariant 1).
	Key string

	// Class is what bounds the set of values behind Key. Only
	// [ClassConstruction] and [ClassConfiguration] may reach an instrument.
	Class Class

	// Chooser names who decides the value, in the terms an operator reading a
	// cardinality question would use.
	Chooser string
}

// Table is the schema: every attribute key this system's telemetry knows
// about, including the ones a metric must never carry.
//
// The unbounded rows are listed rather than merely omitted, so the refusal is
// a statement someone can read and test rather than an accident of what nobody
// has added yet. Each is minted per event by somebody other than the
// deployment: flowstate.delivery.id is chosen by an external webhook sender,
// one per delivery, so a stranger with an HTTP client would be choosing this
// system's metric cardinality; the run and execution identifiers are minted
// per execution, so a busy deployment mints thousands an hour. All of them are
// useful on a span and a log line, where a per-event identifier is the point.
var Table = []Attribute{
	{Key: PluginName, Class: ClassConfiguration, Chooser: "the deployment, by which plugins it installs"},
	{Key: PluginOperation, Class: ClassConstruction, Chooser: "this repository: launch, start, health, execute"},
	{Key: PluginOutcome, Class: ClassConstruction, Chooser: "this repository: success, error"},
	{Key: PluginHealthStatus, Class: ClassConstruction, Chooser: "this repository's plugin health enumeration"},
	{Key: TaskName, Class: ClassConfiguration, Chooser: "the deployment, by which tasks it registers"},

	{Key: "flowstate.delivery.id", Class: ClassPeerControlled, Chooser: "the external sender, one per webhook delivery"},
	{Key: "flowstate.run.id", Class: ClassPeerControlled, Chooser: "generated, one per execution"},
	{Key: "flowstate.execution.id", Class: ClassPeerControlled, Chooser: "generated, one per execution"},
	{Key: "flowstate.workflow.run_id", Class: ClassPeerControlled, Chooser: "generated, one per execution"},
}

// allowed is [Table] indexed by key, holding only the rows a metric may carry.
// It is derived rather than written down twice, because a second copy of a
// list is a thing that disagrees with the first one.
var allowed = func() map[string]Class {
	out := map[string]Class{}
	for _, attr := range Table {
		if attr.Class == ClassPeerControlled {
			continue
		}
		out[attr.Key] = attr.Class
	}
	return out
}()

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

// NeverKeys returns the keys a metric instrument must never carry, sorted.
func NeverKeys() []string {
	var keys []string
	for _, attr := range Table {
		if attr.Class == ClassPeerControlled {
			keys = append(keys, attr.Key)
		}
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
	//
	// Wrapped in tildes rather than spelled as a bare word like "other",
	// because a bare word is a legal value under every grammar this schema
	// bounds and would silently merge with a real one — a plugin actually
	// named "other" would already share this series with every overflowed
	// key before a single value had overflowed. Same shape as
	// auth/namespace.go's "_default": pick a character the value's own
	// grammar refuses, so the collision is impossible rather than unlikely.
	// A tilde is outside every grammar an allowlisted key's value must
	// satisfy: pluginName (discover.go) admits only [a-z0-9-], the
	// TaskDescription.name field pattern and its sharper per-segment
	// registry rules admit only [A-Za-z0-9_-] plus a single separating dot,
	// and the fixed-enumeration values recorded for
	// PluginOperation/PluginOutcome/PluginHealthStatus are plain lowercase
	// words (with "not serving" the only one containing a space) written in
	// this repository. None of them can ever produce a tilde, so this
	// sentinel cannot collide with a legitimate value under any key the
	// schema allows.
	OverflowValue = "~other~"
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
