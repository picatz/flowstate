package dst_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
)

// words is a [dst.Withholding] that withholds the words it names.
type words []string

func (w words) Join(other dst.Withholding) dst.Withholding {
	return slices.Concat(w, other.(words))
}

// withholdingWords is a result whose error is text, shown with every word
// withheld names replaced.
func withholdingWords(text string, withheld words) dst.Result {
	return dst.Result{
		Err:      errors.New(text),
		Withheld: withheld,
		Show: func(under dst.Withholding) dst.Result {
			shown := text
			for _, word := range under.(words) {
				shown = strings.ReplaceAll(shown, word, "[withheld]")
			}

			return dst.Result{Err: errors.New(shown)}
		},
	}
}

// TestADivergenceShowsBothSidesUnderWhatEitherWithholds: each run withholds a
// word only the other shows, and a divergence shows both runs at once, so
// each is shown under both runs' withholdings. A run that withholds nothing
// is shown under what the other side withholds, while every other observation
// keeps its own rendering (#2214).
func TestADivergenceShowsBothSidesUnderWhatEitherWithholds(t *testing.T) {
	t.Parallel()

	report := dst.Explore(t.Context(), dst.Budget{Schedules: 1, Seed0: 1}, func(ctx context.Context) dst.Result {
		if v1.SchedulerFromContext(ctx) == v1.WrittenOrder {
			return withholdingWords("alpha beta first", nil)
		}

		return withholdingWords("alpha beta second", words{"alpha"})
	})

	require.NotNil(t, report.Divergence)
	for _, rendering := range []string{report.Divergence.Baseline.Rendering, report.Divergence.Diverged.Rendering} {
		assert.NotContains(t, rendering, "alpha", "one side showed what the other withholds")
		assert.Contains(t, rendering, "beta", "a word neither side withholds must still be shown")
		assert.True(t, strings.HasSuffix(rendering, "the comparison read them\n"))
	}
	assert.NotEqual(t, report.Divergence.Baseline.Digest, report.Divergence.Diverged.Digest)
	assert.Contains(t, report.Observations[0].Rendering, "alpha",
		"the baseline withholds nothing, so its own observation shows it as it is")
	assert.NotContains(t, report.Observations[1].Rendering, "alpha",
		"the seeded run's own observation is shown under what it withholds")
}

// TestADivergenceSideWithNothingToShowItByIsShownAsNothing: a side with no
// [dst.Result.Show] cannot be shown under what the other side withholds, so it
// is shown as the notice alone rather than as recorded.
func TestADivergenceSideWithNothingToShowItByIsShownAsNothing(t *testing.T) {
	t.Parallel()

	report := dst.Explore(t.Context(), dst.Budget{Schedules: 1, Seed0: 1}, func(ctx context.Context) dst.Result {
		if v1.SchedulerFromContext(ctx) == v1.WrittenOrder {
			return dst.Result{Err: errors.New("alpha first")}
		}

		return withholdingWords("alpha second", words{"alpha"})
	})

	require.NotNil(t, report.Divergence)
	assert.NotContains(t, report.Divergence.Baseline.Rendering, "alpha")
	assert.Equal(t, "withheld: values this run does not disclose; the comparison read them\n", report.Divergence.Baseline.Rendering)
	assert.NotContains(t, report.Divergence.Diverged.Rendering, "alpha")
}
