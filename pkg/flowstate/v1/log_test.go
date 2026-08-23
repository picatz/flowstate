package flowstatev1_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What the shared driver cases deliberately cannot see.
//
// A `log:` step's observable result is that it produced nothing, which is what those
// assert. The message itself leaves through a logger, and everything worth checking
// about it — that it is emitted at all, at the right level, with its fields, through
// the *caller's* logger rather than a global one — is only visible from here.

// capture is a logger that keeps what it was told.
//
// A handler rather than parsing text off a writer, because these tests are about the
// record — level, message, attributes — and formatting is a different layer's problem.
// Asserting on rendered text would make every one of them fail the day a handler adds a
// space.
type capture struct {
	records []slog.Record
	attrs   []slog.Attr
	spans   []trace.SpanContext
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(ctx context.Context, record slog.Record) error {
	c.records = append(c.records, record)
	c.spans = append(c.spans, trace.SpanContextFromContext(ctx))

	return nil
}

func TestLogCarriesItsFlowstateStepSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	logs := &capture{}
	ctx, run := v1.StartRunSpan(v1.ContextWithLogger(t.Context(), slog.New(logs)))
	_, err := v1.Run(ctx, logStep(map[string]*v1.Value{"message": v1.NewLiteral("correlated")}))
	run.End()
	require.NoError(t, err)
	require.Len(t, logs.spans, 1)
	require.True(t, logs.spans[0].IsValid())

	var stepID trace.SpanID
	for _, span := range recorder.Ended() {
		if span.Name() == "flowstate.step" {
			stepID = span.SpanContext().SpanID()
		}
	}
	require.Equal(t, stepID, logs.spans[0].SpanID())
}

func (c *capture) WithAttrs(attrs []slog.Attr) slog.Handler {
	c.attrs = append(c.attrs, attrs...)

	return c
}

func (c *capture) WithGroup(string) slog.Handler { return c }

// fields returns one record's attributes as a map, for assertions that do not care
// about order.
func fields(record slog.Record) map[string]string {
	out := make(map[string]string, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		out[attr.Key] = attr.Value.String()

		return true
	})

	return out
}

// runWithCapture runs a workflow with a capturing logger installed and returns what it
// caught.
func runWithCapture(t *testing.T, workflow *v1.Workflow) *capture {
	t.Helper()

	logs := &capture{}
	_, err := v1.Run(v1.ContextWithLogger(t.Context(), slog.New(logs)), workflow)
	require.NoError(t, err)

	return logs
}

// logStep builds a one-step workflow around a `log` task.
func logStep(inputs map[string]*v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name:    "log-test",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{Id: "say", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: inputs}}},
		},
	}
}

// TestLogEmitsThroughTheContextLogger is the claim the whole design rests on.
//
// The task does not decide where its message goes, which is what lets one workflow log
// to a terminal locally and to a worker's aggregator in production. If it reached
// [slog.Default] instead, both of those would be the same place — and worse, two tests
// running in parallel would capture each other's lines, which is the shape that makes a
// suite mysteriously flaky rather than plainly broken.
func TestLogEmitsThroughTheContextLogger(t *testing.T) {
	t.Parallel()

	logs := runWithCapture(t, logStep(map[string]*v1.Value{
		"message": v1.NewLiteral("starting"),
	}))

	require.Len(t, logs.records, 1, "a `log:` step emitted nothing through the caller's logger")
	require.Equal(t, "starting", logs.records[0].Message)
	require.Equal(t, slog.LevelInfo, logs.records[0].Level,
		"a `log:` with no level should read as info, which is what the schema documents")
}

// TestLogLevelsMapOntoSlog checks each spelling reaches the severity it names.
//
// Table-driven over all three because the mapping is a switch, and a switch is where a
// copied case keeps the wrong branch: `warn` returning error is a line an operator
// pages on.
func TestLogLevelsMapOntoSlog(t *testing.T) {
	t.Parallel()

	for written, want := range map[string]slog.Level{
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,

		// The schema spelling, accepted because it is what a reader of the schema — or
		// of a protojson payload — has in front of them.
		"LEVEL_WARN": slog.LevelWarn,

		// Case carries no meaning here, so enforcing one would be a diagnostic that
		// teaches nothing.
		"Error": slog.LevelError,
	} {
		t.Run(written, func(t *testing.T) {
			t.Parallel()

			logs := runWithCapture(t, logStep(map[string]*v1.Value{
				"message": v1.NewLiteral("hi"),
				"level":   v1.NewLiteral(written),
			}))

			require.Len(t, logs.records, 1)
			require.Equal(t, want, logs.records[0].Level)
		})
	}
}

// TestLogRefusesALevelItDoesNotHave checks the engine's answer, not the validator's.
//
// `flow validate` reports this before a run, which is where an author meets it — but a
// specification can reach a worker without passing through that command, so the engine
// has to refuse rather than assume. The message names the choices, because a rejection
// that does not say what would have worked leaves the author guessing at a closed set
// of three.
func TestLogRefusesALevelItDoesNotHave(t *testing.T) {
	t.Parallel()

	_, err := v1.Run(t.Context(), logStep(map[string]*v1.Value{
		"message": v1.NewLiteral("hi"),
		"level":   v1.NewLiteral("critical"),
	}))

	require.Error(t, err, "a level outside the enum was accepted")
	require.Contains(t, err.Error(), "info, warn, error",
		"a refused level did not say which levels exist:\n%v", err)
}

// TestLogCarriesItsFields checks the structured half arrives.
func TestLogCarriesItsFields(t *testing.T) {
	t.Parallel()

	logs := runWithCapture(t, logStep(map[string]*v1.Value{
		"message": v1.NewLiteral("rolled out"),
		"fields":  v1.NewExpr(`{"region": "eu-west-1", "stage": "canary"}`),
	}))

	require.Len(t, logs.records, 1)
	require.Equal(t, map[string]string{"region": "eu-west-1", "stage": "canary"},
		fields(logs.records[0]))
}

// TestLogFieldsAreOrdered checks that two runs of one workflow say the same thing.
//
// A protobuf map has no order, so emitting fields in iteration order makes the same log
// line shuffle between runs — which defeats the reason anyone diffs two runs, and does
// it intermittently enough to be blamed on something else.
func TestLogFieldsAreOrdered(t *testing.T) {
	t.Parallel()

	workflow := logStep(map[string]*v1.Value{
		"message": v1.NewLiteral("hi"),
		"fields":  v1.NewExpr(`{"zebra": "1", "alpha": "2", "mango": "3"}`),
	})

	// Repeated because a single run agrees with itself by construction; what is being
	// checked is that the order does not depend on the map's.
	for range 8 {
		logs := runWithCapture(t, workflow)
		require.Len(t, logs.records, 1)

		var keys []string
		logs.records[0].Attrs(func(attr slog.Attr) bool {
			keys = append(keys, attr.Key)

			return true
		})
		require.Equal(t, []string{"alpha", "mango", "zebra"}, keys,
			"a log line's fields are not in a stable order, so two runs of one workflow differ")
	}
}

// TestLogEmitsOncePerIteration checks a loop body logs per item rather than per loop.
//
// The outputs cannot tell these apart — a log step contributes an empty entry either
// way — so this is the only place a loop that resolved its body's inputs once and
// reused them would show up.
func TestLogEmitsOncePerIteration(t *testing.T) {
	t.Parallel()

	logs := runWithCapture(t, &v1.Workflow{
		Name:    "log-loop",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "each",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:    v1.NewLiteralList("a", "b", "c"),
					Iterator: "name",
					Body: []*v1.Node{
						{
							Id: "note",
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name:   "log",
								Inputs: map[string]*v1.Value{"message": v1.NewExpr("name")},
							}},
						},
					},
				}},
			},
		},
	})

	messages := make([]string, 0, len(logs.records))
	for _, record := range logs.records {
		messages = append(messages, record.Message)
	}
	require.Equal(t, []string{"a", "b", "c"}, messages,
		"a log step in a loop body did not emit the current item once per iteration")
}

// TestLogHasNoOutputs is the refusal, asserted where it is decided.
//
// The design says a log line is an effect on a reader rather than a value for a later
// step, and the enforcement is simply that the outputs message is empty. Nothing else
// prevents `${steps.say.result}`; it fails because there is no such name. Asserting the
// descriptor is empty is asserting the rule, and it fails the moment someone adds a
// field "just for convenience" — which is exactly how this would erode.
func TestLogHasNoOutputs(t *testing.T) {
	t.Parallel()

	def, ok := v1.LookupTask("log")
	require.True(t, ok, "the log task is not registered")
	require.NotNil(t, def.Outputs, "the log task describes no outputs message at all")
	require.Zero(t, def.Outputs.Fields().Len(),
		"the log task grew an output; a log line is an effect on a reader, not a value")
}

// TestEnumInputSpellings covers the correspondence between what an author writes and
// what the schema stores.
//
// Derived from the descriptor rather than written out, so a fourth level — or a second
// enum-typed input on some other task — is covered the day it is added rather than the
// day someone remembers this file.
func TestEnumInputSpellings(t *testing.T) {
	t.Parallel()

	def, ok := v1.LookupTask("log")
	require.True(t, ok)

	field := def.Inputs.Fields().ByName("level")
	require.NotNil(t, field, "the log task has no `level` input")

	names := v1.EnumValueNames(field.Enum())
	require.Equal(t, []string{"info", "warn", "error"}, names,
		"the levels an author may write changed; if that is intended, the docs and the example say three")

	// The zero value is absence, not a choice. Offering it would invite
	// `level: unspecified` — a way of writing nothing that reads like writing
	// something — and it is excluded from both directions or from neither.
	require.NotContains(t, strings.Join(names, ","), "unspecified")
	_, resolvable := v1.EnumValueNumber(field.Enum(), "unspecified")
	require.False(t, resolvable, "the unspecified value is listed nowhere and resolvable anyway")

	// Every offered spelling resolves. A list an author is shown that contains a value
	// the resolver then refuses is worse than no list.
	for _, name := range names {
		_, ok := v1.EnumValueNumber(field.Enum(), name)
		require.True(t, ok, "%q is offered as a choice and does not resolve", name)
	}
}

// TestLogEnforcesItsDeclaredBounds checks the bounds are real rather than decorative.
//
// A `log:` step's fields are chosen by the workflow — how many, how long the keys, how
// long the values — and they are written to a worker's logs and into durable history.
// The schema declares limits on all three; nothing enforced them, which made them a
// comment. Bounding the resource an *outside party* controls is this repo's stated rule,
// and the party here is whoever wrote the workflow.
func TestLogEnforcesItsDeclaredBounds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		fields map[string]any
	}{
		{
			name:   "a value longer than the limit",
			fields: map[string]any{"k": strings.Repeat("x", 1025)},
		},
		{
			name:   "a key longer than the limit",
			fields: map[string]any{strings.Repeat("k", 65): "v"},
		},
		{
			name:   "more pairs than the limit",
			fields: manyFields(33),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := v1.Run(t.Context(), logStep(map[string]*v1.Value{
				"message": v1.NewLiteral("hi"),
				"fields":  v1.NewLiteralMap(test.fields),
			}))

			require.Error(t, err, "a `log:` step exceeded a declared bound and was emitted anyway")
		})
	}

	// And the shapes just inside each limit still run.
	//
	// Not decoration: without it the three cases above passed for the wrong reason. The
	// fixture built its map with the wrong constructor, every run failed on the
	// conversion rather than on the bound, and three green subtests said the bounds
	// worked. A bound is only demonstrated by a pair — the value it refuses and the
	// value it admits.
	_, err := v1.Run(t.Context(), logStep(map[string]*v1.Value{
		"message": v1.NewLiteral("hi"),
		"fields":  v1.NewLiteralMap(map[string]any{strings.Repeat("k", 64): strings.Repeat("x", 1024)}),
	}))
	require.NoError(t, err, "a `log:` step at the limit was refused")
}

// manyFields builds a fields map of n entries.
func manyFields(n int) map[string]any {
	out := make(map[string]any, n)
	for i := range n {
		out[fmt.Sprintf("k%02d", i)] = "v"
	}

	return out
}

// TestAMapInputRefusesAValueItCannotHold is the silent-corruption fix, tested where it
// is general rather than only through `log:`.
//
// Every string-valued map input shared one conversion path that called the protobuf
// getter for the field's kind directly — and a getter answers the *zero value* for a
// value of some other kind rather than failing. So `fields: {code: 500}` logged `code=`
// and `headers: {X-Count: 5}` sent an empty header: the wrong thing, silently, in a
// durable record and in a request to somebody else's server.
//
// Both are checked here because the fix is one path and a test through one task would
// leave the other free to regress on its own.
func TestAMapInputRefusesAValueItCannotHold(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		task *v1.Task
	}{
		{
			name: "a log field",
			task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewLiteral("hi"),
				"fields":  v1.NewExpr(`{"code": 500}`),
			}},
		},
		{
			name: "an http header",
			task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url":     v1.NewLiteral("https://example.com"),
				"headers": v1.NewExpr(`{"X-Count": 5}`),
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := v1.Run(t.Context(), &v1.Workflow{
				Name:    "map-input",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{{Id: "a", Kind: &v1.Node_Task{Task: test.task}}},
			})

			require.Error(t, err, "a non-string was written into a string map and silently became empty")

			// Naming both the key and what was wrong with it, because "expected a
			// string" against a mapping of eight entries is a search.
			require.Contains(t, err.Error(), "expected a string")
		})
	}
}
