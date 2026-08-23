package flowstatev1

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// The first-party task span, and why it lives in this package rather than in
// either driver.
//
// It used to live in `engine`, which meant it existed for durable execution and
// did not exist at all for local execution: a `flow run local` with an OTLP
// endpoint configured produced no `flowstate.*` span for a run that would have
// produced one per task in production (#523's gap 3). That is invariant 5 —
// "both execution drivers must agree" — failing in its loudest possible form,
// since local runs exist to tell an author what production will do and a trace
// is one of the things they are being told.
//
// The fix is the one CLAUDE.md prescribes for every disagreement of this shape:
// the answer lives in `pkg/flowstate/v1`, which both drivers already import, and
// each driver reads it. Two spellings of "what a task span is called and what it
// may say" would be a fifth thing written down twice, and the four that came
// before it all eventually disagreed.
//
// # Two rules decide the whole shape, and they are not this package's own
//
// **Nothing here may be reached from workflow code.** Invariant 4: a span minted
// during replay is minted again on every replay, and the only code allowed to
// know when that is happening is Temporal's own interceptor. Under the durable
// driver every caller of [StartTaskSpan] is an activity function; under the
// local driver there is no replay to worry about, and the caller is the retry
// loop that actually runs the task.
//
// **No value ever becomes an attribute.** A span is exported to a collector,
// indexed, and read by people and systems with no relationship to the run — so
// the *only* things written here are names and classifications the schema
// already treats as public: the task's name, the step's id, and the scheme and
// name of a secret *reference*. Never an input, never an output, never a
// response body, and — the one that is easy to get wrong — never an error
// message, because a task's error can quote what it was given. The failure
// status therefore carries the error's *classification* and nothing else, and
// the error is not recorded as a span event, since RecordError writes the
// message into one.

// taskTracerName is the instrumentation scope the task span is attributed to,
// spelled the way `netpolicy` and `plugin` spell theirs: the import path of the
// package the instrumentation lives in.
//
// It moved here with the code. A scope naming `…/engine` for a span opened by a
// local run — which never loads the engine at all — would be the trace telling
// an operator the run went through the durable driver.
const taskTracerName = "github.com/picatz/flowstate/pkg/flowstate/v1"

// The attribute vocabulary, named once so a driver cannot invent a second
// spelling of a concept the other one already has, and so a test can assert the
// set is closed rather than assert the one key somebody remembered.
const (
	// SpanAttributeTaskName is the registered name of the task being run.
	SpanAttributeTaskName = "flowstate.task.name"

	// SpanAttributeStepID is the id of the step the task is running for, where
	// the caller knows it.
	SpanAttributeStepID = "flowstate.step.id"

	// SpanAttributeAttempt is the substrate's attempt number.
	//
	// Durable-only, deliberately: see [StartTaskSpan]'s doc.
	SpanAttributeAttempt = "flowstate.attempt"

	// SpanAttributeSecretRefs names the secrets a task will resolve, by scheme
	// and name. Never a value; a [SecretRef] contains no material by
	// construction.
	SpanAttributeSecretRefs = "flowstate.secret.refs"

	// SpanAttributeSecretRefCount is how many of them there are, so a query can
	// find the steps that read any secret at all without matching on a list.
	SpanAttributeSecretRefCount = "flowstate.secret.ref.count"
)

// MaxTaskNameLen is the longest task name the schema permits, in characters.
//
// The Go-side twin of `Task.name`'s protovalidate rule
// (`proto/flowstate/v1/task.proto:377-385`, `max_len: 128`), the same
// relationship [MaxEntityKeyLen] has to `entity_key`'s, and it exists for the
// same reason: a caller embedding this package builds a [Task] in Go and never
// parses a protovalidate error, so a bound that lives only in the schema is not
// a bound that path has.
const MaxTaskNameLen = 128

// boundedSpanName is what telemetry may say a thing is called.
//
// # Why a bound is needed at all, when the schema already has one
//
// The schema's rule is enforced when a specification is *validated*, and the
// paths that reach a span do not all validate first. [RunWithInputs] checks that
// a workflow is non-empty and then opens the run span; [CheckSubmissionSize]
// runs after it, inside the observed call. So a caller embedding this package —
// which builds a [Workflow] in Go rather than compiling a Flowfile — can hand a
// name of any length whatever to telemetry, and the span carrying it is
// exported to a collector before the submission is refused. That is
// peer-controlled-input reasoning applied to our own API surface: the bound
// belongs where the value is *used*, not only where it is checked.
//
// The server's own paths do validate — `server.WebhookReceiver.register` calls
// [Validate] before a workflow is ever served (`server/webhook.go:396`), which
// is why the webhook delivery span's names need no bounding of their own — but
// "some callers validate" is not a bound, it is a convention that holds until
// someone adds a caller.
//
// # Truncation with a marker, not a fixed fallback
//
// An over-long name keeps its first max characters and gains "…". A fixed
// fallback — exporting `<invalid>` for every over-long name — would be equally
// safe and strictly less useful: it collapses every malformed name to one value,
// so an operator looking at a span cannot tell which run produced it, which is
// the one question a span exists to answer. The prefix survives, and the marker
// says plainly that something was cut.
//
// The marker cannot be mistaken for part of a legal name: every name this bounds
// is matched by a protovalidate pattern over `[A-Za-z0-9-_]` (plus `.` for a
// plugin-qualified task), so a "…" in a span name means exactly one thing.
//
// Counted in runes and cut on a rune boundary, because the value being bounded
// is by definition one that broke the schema's pattern and may therefore contain
// anything at all — cutting mid-rune would export a mojibake byte sequence. The
// length pre-check is on bytes, which bound runes from above, so a legal name
// returns without the string ever being walked.
func boundedSpanName(name string, max int) string {
	if len(name) <= max {
		return name
	}

	count := 0
	for offset := range name {
		if count == max {
			return name[:offset] + "…"
		}
		count++
	}

	return name
}

// TaskSpanName is the name of the span covering one execution of a named task.
//
// Both drivers ask for it here rather than concatenating it themselves, which is
// what makes "the same workflow produces the same span names under either
// driver" a property of one function instead of a coincidence between two.
//
// Bounded by [MaxTaskNameLen], here rather than at the two call sites, so that
// what a test computes as the expected name and what a driver exports are the
// same string by construction.
func TaskSpanName(taskName string) string {
	return "flowstate.task/" + boundedSpanName(taskName, MaxTaskNameLen)
}

// StartTaskSpan opens the span covering one task execution.
//
// The provider is read per call rather than captured in a package variable, for
// the reason cmd/flow keeps rediscovering: an instrument built before telemetry
// is configured holds the no-op provider forever, and a worker's registration —
// or a `flow run local` invocation — happens at whatever moment the process
// assembles itself. With nothing configured this is the no-op tracer, the span
// is not recording, and the attribute walk below never happens, which is how
// zero-config stays literally silent.
//
// stepID is empty on the entry points that do not carry one — the durable
// driver's two pre-scope activities take the task alone — so the attribute is
// omitted rather than written blank. An empty attribute is worse than a missing
// one: it reads as a step whose id is the empty string.
//
// # What is deliberately not here: the attempt
//
// [SpanAttributeAttempt] is written by the durable driver alone, from
// `activity.GetInfo`, and the local driver leaves it absent even though its own
// retry loop is counting. The number means "which attempt at this activity is
// this", a fact the substrate owns and preserves across a worker crash; the
// local loop's counter is an in-process integer that a crash discards along with
// the run. Writing the same key for the second thing would make a trace claim
// substrate knowledge nobody has, and absence beats fabrication — a query
// filtering on `flowstate.attempt > 1` gets durable retries and no local
// impostors.
func StartTaskSpan(ctx context.Context, task *Task, stepID string) (context.Context, trace.Span) {
	ctx, span := otel.GetTracerProvider().Tracer(taskTracerName).Start(ctx,
		TaskSpanName(task.GetName()), trace.WithSpanKind(trace.SpanKindInternal))

	if !span.IsRecording() {
		// Nothing configured a provider, so the cheapest possible path: no
		// attribute built, no task walked. This is the zero-config case, which
		// is every first run.
		return ctx, span
	}

	// Bounded like the span name above it: an attribute reaches the same
	// collector, so bounding one alone would relocate the unbounded value rather
	// than remove it.
	attrs := []attribute.KeyValue{
		attribute.String(SpanAttributeTaskName, boundedSpanName(task.GetName(), MaxTaskNameLen)),
		attribute.String(SpanAttributeOperation, TraceOperationAttempt),
	}

	if stepID != "" {
		attrs = append(attrs, attribute.String(SpanAttributeStepID, stepID))
	}

	attrs = append(attrs, SecretReferenceAttributes(task)...)
	span.SetAttributes(attrs...)

	return ctx, span
}

// SecretReferenceAttributes names the secrets a task will resolve, without
// resolving anything.
//
// This is the observability that secret resolution can honestly have from here.
// A reference is what the worker is handed; the value is produced deep inside
// the task's own evaluation and is held in a closure precisely so nothing can
// reach it by reflection. Naming the reference answers the question a trace is
// actually asked — *which* secret did this step read, and did the one that was
// denied get asked for at all — and answering it costs nothing that can leak,
// because a [SecretRef] is a scheme and a name and contains no material by
// construction.
//
// Sorted, because the inputs are a map and a set of attributes that reorders
// between two runs of the same step is a diff for anyone comparing traces.
func SecretReferenceAttributes(task *Task) []attribute.KeyValue {
	// SecretRefsIn walks structures too — since Value.Structure landed, a
	// reference may sit nested inside a header map or json body, and a
	// top-level look would name some of a step's secrets and not others. The
	// walk visits references and structure entries only, never a literal's
	// contents, which is the walk that would leak.
	refs := SecretRefsIn(task)

	if len(refs) == 0 {
		return nil
	}

	return []attribute.KeyValue{
		attribute.StringSlice(SpanAttributeSecretRefs, refs),
		attribute.Int(SpanAttributeSecretRefCount, len(refs)),
	}
}

// RecordTaskOutcome marks a failed span with what kind of failure it was.
//
// The classification and not the message, and not [trace.Span.RecordError],
// which would write the message into an exception event. `${steps.<id>.error}`
// is rendered from whatever the task said, and a task can say a great deal —
// an http task's error names the URL it called, a plugin's names whatever the
// plugin wrote. That text belongs in the run's own history, which is read by
// somebody holding the run, and not in a span, which is read by a collector.
//
// The kind is the same one the durable driver hands Temporal (`activityError`)
// and the same one the local driver's retry loop asks whether to retry on, so a
// span's status cannot disagree with the retry decision about what happened.
func RecordTaskOutcome(span trace.Span, err error) {
	if err == nil || !span.IsRecording() {
		return
	}

	span.SetStatus(codes.Error, ClassifyError(err).String())
}

// TemporalSpanErrorDescription is what a Temporal SDK span's error status says
// instead of the failure's own text.
//
// A fixed string rather than a classification, unlike [RecordTaskOutcome]: this
// wraps spans the SDK opens for every workflow and activity it runs, including
// ones this repository knows nothing about, so there is no classification it is
// entitled to claim. What it can say honestly is that the operation failed.
//
// The considered alternative was to read the classification back off the error
// when it is a *temporal.ApplicationError, whose Type() this repository builds
// from [ClassifyError] at engine/activities.go and is therefore safe by
// construction. Two costs, and both are paid here rather than there. This
// package does not import Temporal at all — `go list -deps ./pkg/flowstate/v1`
// names nothing under go.temporal.io — and one description string is a poor
// price for that edge, given the engine already exports the classification on
// the first-party `flowstate.task` span an operator reads next. The
// import-free spelling, a structural check for `interface{ Type() string }`,
// fails open on every unrelated error type that happens to have that method,
// which is the direction this repository refuses to fail.
const TemporalSpanErrorDescription = "operation failed"

// SanitizedTemporalSpanStarter is [opentelemetry.TracerOptions.SpanStarter] for
// Temporal's tracing interceptor, and it exists because that interceptor is the
// one span-writing path in this repository that does not already obey
// [RecordTaskOutcome]'s rule.
//
// The interceptor's Finish calls RecordError with the activity's error and
// SetStatus with its message, and both write that text into an exported span.
// `${steps.<id>.error}` is rendered from whatever the task said, and a task can
// say a great deal — an http task's error names the URL it called, a plugin's
// names whatever the plugin wrote. The first-party flowstate.task span already
// refuses to export that; the SDK span Temporal wraps around it did not, so the
// same run leaked through the outer span what the inner one withheld.
//
// It lives here rather than in cmd/flow because `engine`'s trace-join tests build
// the same interceptor and say in writing that they match the binary's options.
// Left in cmd/flow, those tests would go on constructing the unsanitized version
// while their comment claimed otherwise — one meaning written down twice, and the
// copy that drifts is the one nothing executes in production.
//
// The signature is structural: it matches SpanStarter without this package
// importing anything from Temporal.
func SanitizedTemporalSpanStarter(
	ctx context.Context,
	tracer trace.Tracer,
	name string,
	options ...trace.SpanStartOption,
) trace.Span {
	_, span := tracer.Start(ctx, name, options...)

	return sanitizedTemporalSpan{Span: span}
}

// sanitizedTemporalSpan is a [trace.Span] whose failure reporting says a
// classification and never a message.
type sanitizedTemporalSpan struct {
	trace.Span
}

// RecordError writes nothing at all.
//
// Not a sanitized error — nothing. This repository's posture for a failed span
// is a status and no exception event, which [RecordTaskOutcome] states and
// plugin/telemetry.go repeats, because RecordError's whole job is to write an
// error's own message into an exported event. Recording a constant instead would
// be a third spelling of the same rule, and an event carrying no information
// anybody can act on.
func (sanitizedTemporalSpan) RecordError(error, ...trace.EventOption) {}

// SetStatus replaces the description on a failure, and leaves every other code's
// alone: an Ok or Unset description is set by the interceptor itself, not copied
// from something a task wrote.
func (s sanitizedTemporalSpan) SetStatus(code codes.Code, description string) {
	if code == codes.Error {
		description = TemporalSpanErrorDescription
	}

	s.Span.SetStatus(code, description)
}
