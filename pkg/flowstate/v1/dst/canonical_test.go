package dst_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
)

// What a comparison is allowed to notice about a transcript, and what it is not.
//
// [dst.Explore] decides equality on a digest of the encoded transcript, and
// `Deterministic: true` sorts a *proto* map's keys — which a CEL map inside a
// transcript is not. It is expr.MapValue's repeated Entries, built by ranging a
// Go map, so its order is Go's randomized one and carries no claim at all.
// Comparing it made the examples corpus report six divergences under schedules
// that answered zero scheduling questions: identical content, identical byte
// count, entries listed in a different order (issue #800).
//
// The first three tests below are the halves of that fix, and the third is the
// one that keeps it honest: a map's entry order stops being a difference, and a
// list's order does not. The last two are a second bound this package needed for
// the same reason it needed the first — a caller driving it from `flow test`
// (#800) reaches paths a `go test` caller never did.

// mapEntries builds a CEL map literal with its entries in the order given, which
// is how a run whose Go map iterated differently would have recorded it.
func mapEntries(order ...string) *v1.Workflow_StepOutputs {
	entries := make([]*expr.MapValue_Entry, 0, len(order))
	for _, key := range order {
		entries = append(entries, &expr.MapValue_Entry{
			Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: key}},
			Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: key + "-value"}},
		})
	}

	return &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"step": {NamedValues: map[string]*v1.Value{
				"claims": {Kind: &v1.Value_Literal{Literal: &expr.Value{
					Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}},
				}}},
			}},
		},
	}
}

// listValues builds a CEL list literal holding the items given, in that order.
func listValues(items ...string) *v1.Workflow_StepOutputs {
	values := make([]*expr.Value, 0, len(items))
	for _, item := range items {
		values = append(values, &expr.Value{Kind: &expr.Value_StringValue{StringValue: item}})
	}

	return &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"step": {NamedValues: map[string]*v1.Value{
				"results": {Kind: &v1.Value_Literal{Literal: &expr.Value{
					Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{Values: values}},
				}}},
			}},
		},
	}
}

// alternating returns a RunFunc that hands back the first transcript on the
// written-order baseline and the second on every seeded schedule, so a single
// [dst.Explore] compares exactly those two renderings.
func alternating(first, second *v1.Workflow_StepOutputs) dst.RunFunc {
	return func(ctx context.Context) dst.Result {
		if v1.SchedulerFromContext(ctx) == v1.WrittenOrder {
			return dst.Result{Transcript: first}
		}

		return dst.Result{Transcript: second}
	}
}

// TestAMapsEntryOrderIsNotADivergence pins the fix: one CEL map recorded with
// its entries in two orders is one transcript, because a map has no order.
//
// Mutation-proven: dropping the canonicalTranscript call from render (encoding
// result.Transcript directly, as this package did before #800) makes this fail
// with a divergence at seed 1 — which is exactly the false report the examples
// corpus produced.
func TestAMapsEntryOrderIsNotADivergence(t *testing.T) {
	report := dst.Explore(t.Context(), dst.Budget{Schedules: 4, Seed0: 1},
		alternating(mapEntries("subject", "issuer", "deployment"), mapEntries("deployment", "subject", "issuer")))

	require.Nil(t, report.Divergence,
		"a CEL map's entry order is not a claim, so listing the same entries differently is not a divergence")
}

// TestAMapsContentsAreStillCompared is the other direction, and the reason
// sorting beats ignoring: sorting the entries leaves a changed key, a changed
// value, or a different number of them as differences the digest still sees.
func TestAMapsContentsAreStillCompared(t *testing.T) {
	t.Run("a different value", func(t *testing.T) {
		changed := mapEntries("subject")
		changed.GetStepValues()["step"].GetNamedValues()["claims"].GetLiteral().
			GetMapValue().GetEntries()[0].Value = &expr.Value{Kind: &expr.Value_StringValue{StringValue: "someone-else"}}

		report := dst.Explore(t.Context(), dst.Budget{Schedules: 4, Seed0: 1},
			alternating(mapEntries("subject"), changed))

		require.NotNil(t, report.Divergence, "a changed map value must still be a divergence")
	})

	t.Run("a missing entry", func(t *testing.T) {
		report := dst.Explore(t.Context(), dst.Budget{Schedules: 4, Seed0: 1},
			alternating(mapEntries("subject", "issuer"), mapEntries("subject")))

		require.NotNil(t, report.Divergence, "a dropped map entry must still be a divergence")
	})
}

// TestAListsOrderIsStillADivergence is the half that stops the fix from being a
// loosening. A CEL list is ordered — a loop's `results` travel in one, and undo
// order is read out of one — so reordering a list is precisely the class of
// defect this package exists to find, and canonicalization must not touch it.
func TestAListsOrderIsStillADivergence(t *testing.T) {
	report := dst.Explore(t.Context(), dst.Budget{Schedules: 4, Seed0: 1},
		alternating(listValues("alpha", "beta", "gamma"), listValues("gamma", "beta", "alpha")))

	require.NotNil(t, report.Divergence,
		"a list is ordered, so a schedule that reordered one has changed something an author can see")
}

// TestExploreStopsWhenTheContextIsDone is the third bound, and the one a caller
// cannot supply for itself: each of a caller's units is a whole search, so
// checking cancellation between them still leaves every remaining schedule of
// the current one to start — and starting one is not cheap. `flow test`'s run
// recompiles a case's stubs and reparses its workflow before execution ever
// consults a context, so an interrupted `--seeds 10000` would spend thousands of
// parses on its way to stopping. Reported by Codex on picatz/flowstate#814.
//
// Mutation-proven: removing the ctx check from Explore's seed loop takes this
// from 3 runs to 51.
func TestExploreStopsWhenTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	runs := 0
	report := dst.Explore(ctx, dst.Budget{Schedules: 50, Seed0: 1}, func(context.Context) dst.Result {
		runs++
		if runs == 3 {
			cancel()
		}

		return dst.Result{}
	})

	assert.Equal(t, 3, runs, "the baseline, two schedules, and then nothing after the cancellation")
	assert.Equal(t, 2, report.Schedules(),
		"a stopped search reports the schedules it managed, not the ones it was asked for")
	assert.Nil(t, report.Divergence,
		"schedules that never ran are not observations to disagree with the baseline")
}

// TestTheBaselineRunsEvenWhenTheContextIsAlreadyDone pins the deliberate
// exception. The baseline is the run a caller *reports* — `flow test` takes its
// verdict and its coverage from it — so gating it would hand back no case at
// all, which is a nil where a report expects a result rather than a bound doing
// its job. The caller's own pre-case check is what refuses to start a case at
// all; this one only stops the search around it.
func TestTheBaselineRunsEvenWhenTheContextIsAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	runs := 0
	report := dst.Explore(ctx, dst.Budget{Schedules: 4, Seed0: 1}, func(context.Context) dst.Result {
		runs++

		return dst.Result{}
	})

	assert.Equal(t, 1, runs, "the baseline, and no schedule after it")
	require.Len(t, report.Observations, 1)
	assert.True(t, report.Observations[0].Baseline)
	assert.Zero(t, report.Schedules())
}
