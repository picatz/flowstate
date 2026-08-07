package flowstatev1_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// run builds a summary for a filter to be asked about.
func run(id string, status v1.RunResponse_Status, start time.Time, closed *time.Time) *v1.RunSummary {
	summary := &v1.RunSummary{
		WorkflowId: id,
		RunId:      id + "-run",
		Status:     status,
		StartTime:  timestamppb.New(start),
	}
	if closed != nil {
		summary.CloseTime = timestamppb.New(*closed)
	}

	return summary
}

// TestAFilterKeepsTheRunsItSaysYesAbout is the ordinary direction.
func TestAFilterKeepsTheRunsItSaysYesAbout(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	closed := start.Add(90 * time.Minute)

	failed := run("deploy", v1.RunResponse_STATUS_FAILED, start, &closed)
	running := run("build", v1.RunResponse_STATUS_RUNNING, start, nil)

	for _, test := range []struct {
		filter string
		failed bool
		runnin bool
	}{
		{filter: `status == "FAILED"`, failed: true},
		{filter: `status != "FAILED"`, runnin: true},
		{filter: `workflow_id == "build"`, runnin: true},
		{filter: `workflow_id.startsWith("dep")`, failed: true},
		{filter: `run_id == "deploy-run"`, failed: true},
		{filter: `status in ["FAILED", "RUNNING"]`, failed: true, runnin: true},
		{filter: `start_time >= timestamp("2026-08-01T12:00:00Z")`, failed: true, runnin: true},
		{filter: `finished`, failed: true},
		{filter: `!finished`, runnin: true},

		// The guarded form, which is the one an author is told to write. It has to
		// answer for the running run rather than erroring, and that is not a
		// property of this filter — it is CEL absorbing the error from the side a
		// false `&&` never needs.
		{filter: `finished && close_time - start_time > duration("1h")`, failed: true},
	} {
		t.Run(test.filter, func(t *testing.T) {
			t.Parallel()

			filter, err := v1.NewRunFilter(test.filter)
			require.NoError(t, err)

			matched, err := filter.Match(t.Context(), failed)
			require.NoError(t, err)
			require.Equal(t, test.failed, matched, "the failed run")

			matched, err = filter.Match(t.Context(), running)
			require.NoError(t, err)
			require.Equal(t, test.runnin, matched, "the running run")
		})
	}
}

// TestAFilterOnNameMatchesTheWorkflowsDeclaredName covers the one field this
// package does not derive from the memo or from a built-in Temporal field —
// see [RunSummary.Name]'s own doc for why WorkflowType cannot answer this.
//
// Also pins the field's honest failure mode: a run this deployment never
// indexed carries the empty string, and that is a value like any other —
// `name == ""` matches it, `name == "anything else"` does not, and neither
// case is an error. A filter is never told "this run has no opinion"; it is
// told what the run actually carries, which for an unindexed run is nothing.
func TestAFilterOnNameMatchesTheWorkflowsDeclaredName(t *testing.T) {
	t.Parallel()

	named := run("etl", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	named.Name = "nightly-etl"

	unindexed := run("legacy", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	// Name left at its zero value: an unindexed run.

	for _, test := range []struct {
		filter string
		named  bool
		legacy bool
	}{
		{filter: `name == "nightly-etl"`, named: true},
		{filter: `name != "nightly-etl"`, legacy: true},
		{filter: `name == ""`, legacy: true},
		{filter: `name.startsWith("nightly")`, named: true},
	} {
		t.Run(test.filter, func(t *testing.T) {
			t.Parallel()

			filter, err := v1.NewRunFilter(test.filter)
			require.NoError(t, err)

			matched, err := filter.Match(t.Context(), named)
			require.NoError(t, err)
			require.Equal(t, test.named, matched, "the indexed run")

			matched, err = filter.Match(t.Context(), unindexed)
			require.NoError(t, err)
			require.Equal(t, test.legacy, matched, "the unindexed run")
		})
	}
}

// TestNoFilterKeepsEverything pins that absence is not a filter.
//
// A nil filter matching everything is what lets `List` apply one unconditionally
// rather than branching, and a nil that matched nothing would empty every listing
// that did not ask a question.
func TestNoFilterKeepsEverything(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{"", "   ", "\t\n"} {
		filter, err := v1.NewRunFilter(expression)
		require.NoError(t, err)
		require.Nil(t, filter)

		matched, err := filter.Match(t.Context(), run("any", v1.RunResponse_STATUS_RUNNING, time.Now(), nil))
		require.NoError(t, err)
		require.True(t, matched, "no filter excluded a run")
	}
}

// TestAnUnguardedCloseTimeErrorsRatherThanLying is the case the whole null
// modelling exists for.
//
// Binding an unfinished run's close time to the epoch would make this filter
// answer *false* — quietly, plausibly, and wrongly: the run has not finished, so
// it has no close time, and "did it close before 2020" has no true answer. An
// error is the honest one, and it is what pushes an author to the guarded form.
func TestAnUnguardedCloseTimeErrorsRatherThanLying(t *testing.T) {
	t.Parallel()

	filter, err := v1.NewRunFilter(`close_time < timestamp("2020-01-01T00:00:00Z")`)
	require.NoError(t, err)

	_, err = filter.Match(t.Context(), run("build", v1.RunResponse_STATUS_RUNNING, time.Now(), nil))
	require.Error(t, err,
		"a filter compared an unfinished run's close time and got an answer, so every "+
			"running run reads as having finished at the epoch")

	// And the same filter is fine about a run that did finish, so the error is
	// about the missing value rather than about the expression.
	closed := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	matched, err := filter.Match(t.Context(),
		run("old", v1.RunResponse_STATUS_COMPLETED, closed.Add(-time.Hour), &closed))
	require.NoError(t, err)
	require.True(t, matched)
}

// TestAFilterThatIsNotAConditionIsRefused covers the mistake CEL is happy with.
//
// `status` on its own type-checks — it is a string — and would evaluate happily
// forever, matching nothing, with no indication of why. Refusing it at compile
// time names what is wrong while the caller still has the expression in front of
// them.
func TestAFilterThatIsNotAConditionIsRefused(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{`status`, `start_time`, `1 + 1`, `["FAILED"]`} {
		_, err := v1.NewRunFilter(expression)
		require.Error(t, err, "a filter that is not a condition was accepted: %s", expression)
		require.Contains(t, err.Error(), "must be a condition")
	}
}

// TestAMisspeltStatusIsRefusedWithTheRealOnes is the diagnostic that CEL cannot
// give.
//
// `status == "FAILD"` is a well-typed comparison of two strings. It compiles, it
// runs, it matches nothing, and the listing comes back empty — which is
// indistinguishable from a listing that legitimately has nothing in it. The rule
// this repository holds to is that silently doing nothing gives the author no
// reason to doubt what they wrote.
func TestAMisspeltStatusIsRefusedWithTheRealOnes(t *testing.T) {
	t.Parallel()

	_, err := v1.NewRunFilter(`status == "FAILD"`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `"FAILD"`)

	// The valid names are offered, because a diagnostic that says what is wrong
	// and not what to write instead is half a diagnostic.
	for _, name := range []string{"FAILED", "RUNNING", "COMPLETED", "CANCELED", "TERMINATED", "TIMED_OUT"} {
		require.Contains(t, err.Error(), name)
	}

	// Both sides of the comparison, and `!=` as well as `==`, since an author
	// writes all four.
	for _, expression := range []string{
		`"FAILD" == status`,
		`status != "FAILD"`,
		`status == "STATUS_FAILED"`,
	} {
		_, err := v1.NewRunFilter(expression)
		require.Error(t, err, "not refused: %s", expression)
	}
}

// TestUNSPECIFIEDIsNotAStatusAnyoneCanFilterFor is a small negative direction with
// a real reason behind it.
//
// The zero value is what protobuf gives a field nobody set — a run the server
// failed to describe, not a state a workload is ever in. Offering it would invite
// a filter for something that is not a question about anybody's runs.
func TestUNSPECIFIEDIsNotAStatusAnyoneCanFilterFor(t *testing.T) {
	t.Parallel()

	require.False(t, v1.StatusNames()["UNSPECIFIED"])

	_, err := v1.NewRunFilter(`status == "UNSPECIFIED"`)
	require.Error(t, err)
}

// TestStatusNamesComeFromTheSchema is what keeps the vocabulary honest.
//
// The point of deriving them from the descriptor is that a status added to the
// schema is filterable, printable and named in the diagnostic without anybody
// remembering to update a list. This asserts the derivation rather than a
// hand-written set, which a hand-written expectation here would quietly undo.
func TestStatusNamesComeFromTheSchema(t *testing.T) {
	t.Parallel()

	names := v1.StatusNames()

	values := v1.RunResponse_Status(0).Descriptor().Values()
	for i := range values.Len() {
		full := string(values.Get(i).Name())
		short := strings.TrimPrefix(full, "STATUS_")

		require.NotEqual(t, full, short,
			"a status is not prefixed the way buf lint requires, so the short name is wrong")

		if short == "UNSPECIFIED" {
			continue
		}
		require.True(t, names[short], "status %s is in the schema and not in the vocabulary", short)
	}

	require.Len(t, names, values.Len()-1, "the vocabulary has a name the schema does not")
}

// TestAFilterIsCostBounded is the bound this needs and the reason it is lower
// than everywhere else.
//
// A filter is evaluated once per execution a listing reads, and how many that is
// has its own bound — so the work one request can ask for is the product of the
// two. The budget here is deliberately far below the one an ordinary expression
// gets, and this asserts it is reached rather than merely present: a bound nothing
// hits is a bound nothing tests.
func TestAFilterIsCostBounded(t *testing.T) {
	t.Parallel()

	// A comprehension over a large range, which is the cheapest way to write an
	// expensive predicate and the shape a caller would reach for by accident.
	filter, err := v1.NewRunFilter(
		`[1,2,3,4,5,6,7,8,9,10].all(a, [1,2,3,4,5,6,7,8,9,10].all(b, ` +
			`[1,2,3,4,5,6,7,8,9,10].all(c, [1,2,3,4,5,6,7,8,9,10].all(d, ` +
			`workflow_id.startsWith("x")))))`)
	require.NoError(t, err)

	_, err = filter.Match(t.Context(), run("x", v1.RunResponse_STATUS_RUNNING, time.Now(), nil))
	require.Error(t, err, "an expensive filter ran to completion, so the cost bound is not reached")
	require.Contains(t, strings.ToLower(err.Error()), "cost")
}

// TestAMisspeltStatusInAListIsRefusedToo covers the form the first version of the
// check could not see.
//
// `status in ["FAILED", "RUNNING"]` is a supported and natural way to ask for
// either — it is in the table above — and a misspelling inside that list compiled
// happily, matched nothing, and produced the empty listing the whole diagnostic
// exists to prevent. Checking `==` and `!=` and not `in` meant the promise held
// for two spellings of one question and not the third.
func TestAMisspeltStatusInAListIsRefusedToo(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`status in ["FAILD"]`,
		`status in ["FAILED", "FAILD"]`,
		`status in ["FAILD", "RUNNING"]`,
	} {
		_, err := v1.NewRunFilter(expression)
		require.Error(t, err, "a misspelt status in a list was accepted: %s", expression)
		require.Contains(t, err.Error(), `"FAILD"`)
	}

	// And the good form still compiles, so the check did not simply learn to
	// refuse `in`.
	_, err := v1.NewRunFilter(`status in ["FAILED", "RUNNING"]`)
	require.NoError(t, err)
}

// TestAShadowedStatusIsNotTheRunsStatus is the direction that makes this a scope
// question rather than a spelling one.
//
// A comprehension macro binds its iteration variable inside its body, so the
// `status` in `["x"].exists(status, status == "x")` is the macro's and has nothing
// to do with a run. A check that matched on the name alone would refuse this —
// reporting `"x"` as an invalid run status, on an expression that is completely
// correct.
//
// That is precisely the mistake CLAUDE.md records `flow fix` making twice: a
// rewriter that knew less about scope than the language does. The lesson arriving
// in a new place is why this test names it.
func TestAShadowedStatusIsNotTheRunsStatus(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`["x"].exists(status, status == "x")`,
		`["x"].all(status, status != "not-a-run-status")`,
		`["x"].exists_one(status, status == "x")`,

		// Nested, so the subtraction has to survive more than one level.
		`["x"].exists(status, ["y"].exists(other, status == "x" && other == "y"))`,
	} {
		_, err := v1.NewRunFilter(expression)
		require.NoError(t, err,
			"a comprehension variable named status was mistaken for the run's: %s", expression)
	}

	// The other side of the same boundary: a macro's *range* is evaluated outside
	// the binding, so the run's status is still the run's status there — and a
	// misspelling in that position is still caught.
	_, err := v1.NewRunFilter(`[status].exists(s, s == "FAILED")`)
	require.NoError(t, err)

	_, err = v1.NewRunFilter(`status == "FAILD" && ["x"].exists(status, status == "x")`)
	require.Error(t, err,
		"shadowing inside one comprehension stopped the check seeing a real mistake beside it")
	require.Contains(t, err.Error(), `"FAILD"`)
}
