package flowstatev1_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAStatusLiteralIsReadByItsEnumNameWhateverTheSpelling is the fix for a filter
// typed the way a person says it: `status == "failed"`, or `"succeeded"` because
// that is the word `flow get` prints for COMPLETED. Each is one meaning with
// another spelling, so it is matched rather than refused, through the one
// evaluator — and in the `!=` and `in` forms, from either side of the operator.
func TestAStatusLiteralIsReadByItsEnumNameWhateverTheSpelling(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	closed := start.Add(time.Minute)

	runs := map[string]*v1.RunSummary{
		"FAILED":    run("a", v1.RunResponse_STATUS_FAILED, start, &closed),
		"COMPLETED": run("b", v1.RunResponse_STATUS_COMPLETED, start, &closed),
		"TIMED_OUT": run("c", v1.RunResponse_STATUS_TIMED_OUT, start, &closed),
		"CANCELED":  run("d", v1.RunResponse_STATUS_CANCELED, start, &closed),
		"RUNNING":   run("e", v1.RunResponse_STATUS_RUNNING, start, nil),
	}

	// keeps asserts that filter keeps exactly the runs whose status name satisfies want.
	keeps := func(t *testing.T, expression string, want func(name string) bool) {
		t.Helper()

		filter, err := v1.NewRunFilter(expression)
		require.NoError(t, err)

		for name, summary := range runs {
			matched, err := filter.Match(t.Context(), summary)
			require.NoError(t, err)
			require.Equal(t, want(name), matched, "%s against the %s run", expression, name)
		}
	}

	for _, test := range []struct {
		filter string
		want   string
	}{
		{`status == "failed"`, "FAILED"},
		{`status == "Failed"`, "FAILED"},
		{`"failed" == status`, "FAILED"},
		{`status == "succeeded"`, "COMPLETED"},
		{`status == "Succeeded"`, "COMPLETED"},
		{`status == "completed"`, "COMPLETED"},
		{`status == "timed out"`, "TIMED_OUT"},
		{`status == "timed_out"`, "TIMED_OUT"},
		{`status == "Timed-Out"`, "TIMED_OUT"},
		{`status == "cancelled"`, "CANCELED"},
		{`status == "canceled"`, "CANCELED"},
		{`status == "running"`, "RUNNING"},
	} {
		t.Run(test.filter, func(t *testing.T) {
			t.Parallel()

			keeps(t, test.filter, func(name string) bool { return name == test.want })
		})
	}

	t.Run("!=", func(t *testing.T) {
		t.Parallel()

		keeps(t, `status != "succeeded"`, func(name string) bool { return name != "COMPLETED" })
	})

	t.Run("in", func(t *testing.T) {
		t.Parallel()

		keeps(t, `status in ["failed", "Timed Out", "RUNNING"]`, func(name string) bool {
			return name == "FAILED" || name == "TIMED_OUT" || name == "RUNNING"
		})
	})

	// Only a comparison against `status` is read: a string that merely looks like
	// a status elsewhere is untouched.
	t.Run("only a status comparison is read", func(t *testing.T) {
		t.Parallel()

		filter, err := v1.NewRunFilter(`status == "failed" && name != "failed"`)
		require.NoError(t, err)

		named := run("a", v1.RunResponse_STATUS_FAILED, start, &closed)
		named.Name = "failed"
		matched, err := filter.Match(t.Context(), named)
		require.NoError(t, err)
		require.False(t, matched, "a name literal was folded into a status")

		named.Name = "other"
		matched, err = filter.Match(t.Context(), named)
		require.NoError(t, err)
		require.True(t, matched)
	})
}

// TestAStatusThatIsNotOneStaysRefused is the fail-closed direction of the same
// change: accepting more spellings must not turn an unknown literal into a match.
// Ambiguous display words are refused too, because `done` could be COMPLETED,
// CANCELED, or TERMINATED and picking one would answer a different question than
// the one asked.
func TestAStatusThatIsNotOneStaysRefused(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`status == "banana"`,
		`status == ""`,
		`status == "done"`,
		`status == "stopped"`,
		`status == "success"`,
		`status != "bananas"`,
		`status in ["failed", "banana"]`,
		`"banana" == status`,
		`status == "unspecified"`,
		`status == "STATUS_FAILED"`,
	} {
		_, err := v1.NewRunFilter(expression)
		require.Error(t, err, "accepted: %s", expression)
		require.Contains(t, err.Error(), "not a run status", expression)
	}
}

// TestANearMissSaysWhatItProbablyMeant checks the one-line diagnostic: it quotes
// the literal, offers the status when exactly one is close, lists every accepted
// word once, and says nothing twice.
func TestANearMissSaysWhatItProbablyMeant(t *testing.T) {
	t.Parallel()

	_, err := v1.NewRunFilter(`status == "faild"`)
	require.Error(t, err)

	message := err.Error()
	require.NotContains(t, message, "\n", "the diagnostic is one line")
	require.Contains(t, message, `"faild" (did you mean "FAILED"?)`)
	for _, name := range []string{"CANCELED", "COMPLETED", "FAILED", "RUNNING", "TERMINATED", "TIMED_OUT"} {
		require.Contains(t, message, name)
	}
	require.Equal(t, 1, strings.Count(message, "the statuses are"))
	require.Contains(t, message, `"succeeded" for COMPLETED`)
	require.Contains(t, message, `"cancelled" for CANCELED`)
	require.Contains(t, message, `"timed out" for TIMED_OUT`)

	// Far from every status, there is nothing to suggest.
	_, err = v1.NewRunFilter(`status == "banana"`)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "did you mean")

	// Each bad literal is named once however often it is written.
	_, err = v1.NewRunFilter(`status == "faild" || status != "faild"`)
	require.Error(t, err)
	require.Equal(t, 1, strings.Count(err.Error(), `"faild"`))

	// A literal far longer than any status is refused without the distance work.
	_, err = v1.NewRunFilter(`status == "` + strings.Repeat("x", 4096) + `"`)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "did you mean")
}
