package graph_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

func attempt(n uint32) *uint32 { return &n }

func TestRefSpellingsRoundTrip(t *testing.T) {
	step := v1.FormatDebugAddress([]*v1.DebugSegment{
		{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_ITERATION, StepId: "each", Index: 3},
		{Kind: v1.DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL, StepId: "deploy", Callee: "ship"},
	}, "push")

	for _, tc := range []struct {
		name      string
		ref       *v1.GraphRef
		level     graph.Level
		uri       string
		shorthand string
	}{
		{"fleet", &v1.GraphRef{}, graph.LevelFleet, "flowstate://fleet", ""},
		{"workflow", &v1.GraphRef{WorkflowName: "deploy"}, graph.LevelWorkflow, "flowstate://workflow/deploy", ""},
		{"latest run", &v1.GraphRef{WorkflowId: "orders-1"}, graph.LevelRun, "flowstate://run/orders-1", "orders-1"},
		{"run", &v1.GraphRef{WorkflowId: "orders-1", RunId: "r9"}, graph.LevelRun, "flowstate://run/orders-1/r9", "orders-1@r9"},
		{
			"step", &v1.GraphRef{WorkflowId: "w", RunId: "r", Step: "charge"}, graph.LevelStep,
			"flowstate://run/w/r/step/charge", "w@r:charge",
		},
		{
			"attempt", &v1.GraphRef{WorkflowId: "w", RunId: "r", Step: "charge", Attempt: attempt(2)}, graph.LevelAttempt,
			"flowstate://run/w/r/step/charge/attempt/2", "w@r:charge!2",
		},
		{
			"separators in every part", &v1.GraphRef{WorkflowId: "a/b@c:d!e%f", RunId: "x/y", Step: "s:t!u@v/w"}, graph.LevelStep,
			"flowstate://run/a%2Fb@c:d%21e%25f/x%2Fy/step/s:t%21u@v%2Fw", "a/b%40c%3Ad%21e%25f@x/y:s%3At%21u%40v/w",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			level, err := graph.RefLevel(tc.ref)
			require.NoError(t, err)
			assert.Equal(t, tc.level, level)

			uri, err := graph.FormatURI(tc.ref)
			require.NoError(t, err)
			assert.Equal(t, tc.uri, uri)
			back, err := graph.ParseURI(uri)
			require.NoError(t, err)
			assert.True(t, proto.Equal(tc.ref, back), "uri %s parsed to %v", uri, back)

			short, err := graph.FormatShorthand(tc.ref)
			if tc.level < graph.LevelRun {
				require.Error(t, err, "a %s has no shorthand", tc.level)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.shorthand, short)
			back, err = graph.ParseShorthand(short)
			require.NoError(t, err)
			assert.True(t, proto.Equal(tc.ref, back), "shorthand %s parsed to %v", short, back)
		})
	}

	t.Run("a debug address is carried unchanged", func(t *testing.T) {
		ref := &v1.GraphRef{WorkflowId: "w", RunId: "r", Step: step}
		uri, err := graph.FormatURI(ref)
		require.NoError(t, err)
		back, err := graph.ParseURI(uri)
		require.NoError(t, err)
		assert.Equal(t, step, back.GetStep(), "the explorer must hand the debugger the address it wrote")

		short, err := graph.FormatShorthand(ref)
		require.NoError(t, err)
		back, err = graph.ParseShorthand(short)
		require.NoError(t, err)
		assert.Equal(t, step, back.GetStep())
	})
}

func TestRefRefusesWhatItCannotMean(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  *v1.GraphRef
		want string
	}{
		{"name and id", &v1.GraphRef{WorkflowName: "a", WorkflowId: "b"}, "cannot be combined"},
		{"run without id", &v1.GraphRef{RunId: "r"}, "need a workflow_id"},
		{"step without run", &v1.GraphRef{WorkflowId: "w", Step: "s"}, "step needs a run_id"},
		{"attempt without step", &v1.GraphRef{WorkflowId: "w", RunId: "r", Attempt: attempt(1)}, "attempt needs a step"},
		{"attempt zero", &v1.GraphRef{WorkflowId: "w", RunId: "r", Step: "s", Attempt: attempt(0)}, "attempt counts from 1"},
		{"long id", &v1.GraphRef{WorkflowId: strings.Repeat("x", graph.MaxRefNameRunes+1)}, "workflow_id is 257 characters"},
		{"bad utf-8", &v1.GraphRef{WorkflowId: "\xff"}, "workflow_id is not valid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := graph.RefLevel(tc.ref)
			require.ErrorContains(t, err, tc.want)
			_, err = graph.FormatURI(tc.ref)
			require.Error(t, err)
		})
	}
}

// The schema and the Go helpers must agree on which references mean something,
// so a surface that only runs schema validation cannot admit one the helpers
// refuse.
func TestRefSchemaRulesMatchRefLevel(t *testing.T) {
	for _, ref := range []*v1.GraphRef{
		{},
		{WorkflowName: "a"},
		{WorkflowId: "w"},
		{WorkflowId: "w", RunId: "r", Step: "s", Attempt: attempt(1)},
		{WorkflowName: "a", WorkflowId: "b"},
		{RunId: "r"},
		{WorkflowId: "w", Step: "s"},
		{WorkflowId: "w", RunId: "r", Attempt: attempt(1)},
		{WorkflowId: "w", RunId: "r", Step: "s", Attempt: attempt(0)},
	} {
		_, levelErr := graph.RefLevel(ref)
		schemaErr := v1.Validate(ref)
		assert.Equal(t, levelErr == nil, schemaErr == nil, "%v: RefLevel said %v, the schema said %v", ref, levelErr, schemaErr)
	}
}

// The longest reference the schema accepts must survive both spellings: the
// parsers' size bound has to sit above what the formatters can write.
func TestRefLongestValidReferenceRoundTrips(t *testing.T) {
	name := strings.Repeat("𝒳", graph.MaxRefNameRunes)
	ref := &v1.GraphRef{
		WorkflowId: name, RunId: name, Step: strings.Repeat("𝒳", graph.MaxRefStepRunes), Attempt: attempt(4_000_000_000),
	}
	require.NoError(t, v1.Validate(ref))

	uri, err := graph.FormatURI(ref)
	require.NoError(t, err)
	require.LessOrEqual(t, len(uri), graph.MaxRefBytes)
	back, err := graph.ParseURI(uri)
	require.NoError(t, err)
	assert.True(t, proto.Equal(ref, back))

	short, err := graph.FormatShorthand(ref)
	require.NoError(t, err)
	require.LessOrEqual(t, len(short), graph.MaxRefBytes)
	back, err = graph.ParseShorthand(short)
	require.NoError(t, err)
	assert.True(t, proto.Equal(ref, back))
}

func TestParseRefRefusesMalformedText(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"scheme", "http://fleet", "does not start with flowstate://"},
		{"kind", "flowstate://galaxy", `unknown reference kind "galaxy"`},
		{"fleet trailing", "flowstate://fleet/x", "unexpected trailing"},
		{"workflow empty", "flowstate://workflow/", "missing the workflow name"},
		{"run missing id", "flowstate://run", "missing the workflow id"},
		{"bad escape", "flowstate://run/a%zz", "workflow id is not valid percent-encoding"},
		{"wrong word", "flowstate://run/w/r/steps/s", "expected step"},
		{"step empty", "flowstate://run/w/r/step/", "missing the step address"},
		{"attempt word", "flowstate://run/w/r/step/s/try/1", "expected attempt"},
		{"attempt nan", "flowstate://run/w/r/step/s/attempt/x", "not a number"},
		{"attempt negative", "flowstate://run/w/r/step/s/attempt/-1", "not a number"},
		{"after attempt", "flowstate://run/w/r/step/s/attempt/1/z", "unexpected trailing"},
		{"too long", "flowstate://run/" + strings.Repeat("a", graph.MaxRefBytes), "over the limit"},
	} {
		t.Run("uri "+tc.name, func(t *testing.T) {
			_, err := graph.ParseURI(tc.in)
			require.ErrorContains(t, err, tc.want)
		})
	}
	for _, tc := range []struct{ name, in, want string }{
		{"empty", "", "workflow id is empty"},
		{"empty run", "w@", "run id after @ is empty"},
		{"empty step", "w@r:", "step address after : is empty"},
		{"step without run", "w:s", "step needs a run_id"},
		{"stray at", "w@r@x", "literal @"},
		{"attempt nan", "w@r:s!x", "not a number"},
		{"double bang", "w@r:s!1!2", "single bang"},
		{"bad escape", "w%zz", "not valid percent-encoding"},
		{"too long", strings.Repeat("a", graph.MaxRefBytes+1), "over the limit"},
	} {
		t.Run("shorthand "+tc.name, func(t *testing.T) {
			_, err := graph.ParseShorthand(tc.in)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// FuzzRefRoundTrip is the property the design asks for: any reference the
// schema accepts survives both spellings, whatever its ids hold, and a parser
// never panics on text it was not given by a formatter.
func FuzzRefRoundTrip(f *testing.F) {
	f.Add("w", "r", "s", uint32(1), true)
	f.Add("a/b@c:d!e%f", "x y", "loop[2]/call(ship)/step", uint32(0), true)
	f.Add("%2F", "%", "…/s", uint32(7), false)
	f.Fuzz(func(t *testing.T, id, run, step string, n uint32, withAttempt bool) {
		ref := &v1.GraphRef{WorkflowId: id, RunId: run, Step: step}
		if withAttempt {
			ref.Attempt = &n
		}
		if _, err := graph.RefLevel(ref); err != nil {
			return
		}
		if !utf8.ValidString(id + run + step) {
			return
		}

		uri, err := graph.FormatURI(ref)
		require.NoError(t, err)
		back, err := graph.ParseURI(uri)
		require.NoError(t, err, uri)
		require.True(t, proto.Equal(ref, back), "%q -> %s -> %v", ref, uri, back)
		again, err := graph.FormatURI(back)
		require.NoError(t, err)
		require.Equal(t, uri, again)

		short, err := graph.FormatShorthand(ref)
		require.NoError(t, err)
		back, err = graph.ParseShorthand(short)
		require.NoError(t, err, short)
		require.True(t, proto.Equal(ref, back), "%q -> %s -> %v", ref, short, back)
	})
}

func FuzzParseRefNeverPanics(f *testing.F) {
	for _, s := range []string{"", "flowstate://", "flowstate://run//", "w@r:s!1", "%", "flowstate://run/%/%/step/%"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if ref, err := graph.ParseURI(s); err == nil {
			_, err := graph.RefLevel(ref)
			require.NoError(t, err)
		}
		if ref, err := graph.ParseShorthand(s); err == nil {
			_, err := graph.RefLevel(ref)
			require.NoError(t, err)
		}
	})
}
