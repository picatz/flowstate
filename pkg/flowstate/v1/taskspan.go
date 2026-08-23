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

// TaskSpanName is the name of the span covering one execution of a named task.
//
// Both drivers ask for it here rather than concatenating it themselves, which is
// what makes "the same workflow produces the same span names under either
// driver" a property of one function instead of a coincidence between two.
func TaskSpanName(taskName string) string {
	return "flowstate.task/" + taskName
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

	attrs := []attribute.KeyValue{attribute.String(SpanAttributeTaskName, task.GetName())}

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
