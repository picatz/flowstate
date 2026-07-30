package flowstatev1_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(_ context.Context, record slog.Record) error {
	c.records = append(c.records, record)

	return nil
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
