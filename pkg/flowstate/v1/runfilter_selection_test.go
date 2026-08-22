package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The three names a listing gained so that an operator can say *which runs*
// before saying what to do about them: `labels`, `starter`, `worker_version`.
//
// Each is exercised in both directions — the runs it selects and the runs it
// leaves alone — because a selector that matches everything and a selector that
// matches the right things look identical from the matching side alone.

// TestAFilterOnLabelsSelectsByWhatTheAuthorDeclared is the ordinary direction,
// plus the one guard the vocabulary requires.
func TestAFilterOnLabelsSelectsByWhatTheAuthorDeclared(t *testing.T) {
	t.Parallel()

	payments := run("etl", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	payments.Labels = map[string]string{"team": "payments", "cost-center": "cc-1234"}

	search := run("index", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	search.Labels = map[string]string{"team": "search"}

	// Labels left nil: a run of a workflow that declared none, and equally a run
	// started before labels were recorded at all. The two are the same answer to
	// the only question a filter asks.
	unlabelled := run("legacy", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)

	for _, test := range []struct {
		filter     string
		payments   bool
		search     bool
		unlabelled bool
	}{
		{filter: `"team" in labels && labels["team"] == "payments"`, payments: true},
		{filter: `"team" in labels && labels["team"] != "payments"`, search: true},
		{filter: `"team" in labels`, payments: true, search: true},

		// The negative question, and the reason `labels` binds to an empty map
		// rather than to null: "which runs did nobody label" has to be askable,
		// and a binding that errored would refuse it on exactly the runs it is
		// asking about.
		{filter: `!("team" in labels)`, unlabelled: true},

		{filter: `"cost-center" in labels && labels["cost-center"] == "cc-1234"`, payments: true},
		{filter: `labels.size() == 0`, unlabelled: true},
	} {
		t.Run(test.filter, func(t *testing.T) {
			t.Parallel()

			filter, err := v1.NewRunFilter(test.filter)
			require.NoError(t, err)

			for _, subject := range []struct {
				name string
				run  *v1.RunSummary
				want bool
			}{
				{name: "payments", run: payments, want: test.payments},
				{name: "search", run: search, want: test.search},
				{name: "unlabelled", run: unlabelled, want: test.unlabelled},
			} {
				matched, err := filter.Match(t.Context(), subject.run)
				require.NoError(t, err)
				require.Equal(t, subject.want, matched, "the %s run", subject.name)
			}
		})
	}
}

// TestAnUnguardedLabelIndexErrorsRatherThanLying is `close_time`'s lesson, on a
// map.
//
// Binding an unlabelled run's labels to a map containing an empty string for
// every key would make this filter answer *false* — quietly and wrongly, because
// "is this run's team payments" has no answer for a run with no team. CEL's own
// answer for indexing an absent key is an error, and the error is what pushes an
// author to the guarded form the help text and the example both write.
func TestAnUnguardedLabelIndexErrorsRatherThanLying(t *testing.T) {
	t.Parallel()

	filter, err := v1.NewRunFilter(`labels["team"] == "payments"`)
	require.NoError(t, err)

	_, err = filter.Match(t.Context(), run("legacy", v1.RunResponse_STATUS_RUNNING, time.Now(), nil))
	require.Error(t, err,
		"indexing a label an unlabelled run does not carry produced an answer, so every "+
			"unlabelled run reads as carrying an empty value for every key")

	// The same filter is fine about a run that carries the key, so the error is
	// about the missing value rather than about the expression.
	labelled := run("etl", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	labelled.Labels = map[string]string{"team": "payments"}

	matched, err := filter.Match(t.Context(), labelled)
	require.NoError(t, err)
	require.True(t, matched)
}

// TestAFilterOnWorkerVersionSelectsTheRunsOnOneBuild is the bad-deploy question:
// which runs are pinned to the version that just turned out to be wrong.
func TestAFilterOnWorkerVersionSelectsTheRunsOnOneBuild(t *testing.T) {
	t.Parallel()

	bad := run("on-the-bad-build", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	bad.WorkerVersion = "flowstate.417"

	good := run("on-the-good-build", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	good.WorkerVersion = "flowstate.418"

	// A deployment that does not use Worker Deployment Versioning at all, which
	// is the unconfigured default: not pinned, rather than unknown.
	unversioned := run("unpinned", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)

	for _, test := range []struct {
		filter      string
		bad         bool
		good        bool
		unversioned bool
	}{
		{filter: `worker_version == "flowstate.417"`, bad: true},
		{filter: `worker_version.startsWith("flowstate.")`, bad: true, good: true},

		// Selectable rather than an error: on a deployment with versioning off,
		// every run answers this way, and asking "which of these is not pinned"
		// must not be a question that fails.
		{filter: `worker_version == ""`, unversioned: true},
	} {
		t.Run(test.filter, func(t *testing.T) {
			t.Parallel()

			filter, err := v1.NewRunFilter(test.filter)
			require.NoError(t, err)

			for _, subject := range []struct {
				name string
				run  *v1.RunSummary
				want bool
			}{
				{name: "bad build", run: bad, want: test.bad},
				{name: "good build", run: good, want: test.good},
				{name: "unversioned", run: unversioned, want: test.unversioned},
			} {
				matched, err := filter.Match(t.Context(), subject.run)
				require.NoError(t, err)
				require.Equal(t, subject.want, matched, "the %s run", subject.name)
			}
		})
	}
}

// TestAFilterOnStarterComparesTheQualifiedForm pins that the filter sees exactly
// the string the schema says it sees.
//
// The qualified `issuer#subject` and nothing prettier: a caller comparing a
// rendered short form would have written a check that passes for a subject
// minted by a different identity provider, which is why [v1.RunSummary.Starter]
// carries the raw form in the first place.
func TestAFilterOnStarterComparesTheQualifiedForm(t *testing.T) {
	t.Parallel()

	alice := run("hers", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)
	alice.Starter = v1.QualifiedSubject("https://issuer.example", "alice")

	// A run whose starter was never recorded, or could not be read: empty, with
	// no placeholder that could compare equal to a real identity.
	anonymous := run("nobodys", v1.RunResponse_STATUS_RUNNING, time.Now(), nil)

	for _, test := range []struct {
		filter    string
		alice     bool
		anonymous bool
	}{
		{filter: `starter == "https://issuer.example#alice"`, alice: true},
		{filter: `starter.endsWith("#alice")`, alice: true},
		{filter: `starter == ""`, anonymous: true},

		// The subject alone is not the starter, and must not match one. This is
		// the negative direction that matters: a filter written against a bare
		// subject selects nothing rather than selecting everyone that issuer
		// ever minted a token for.
		{filter: `starter == "alice"`},
	} {
		t.Run(test.filter, func(t *testing.T) {
			t.Parallel()

			filter, err := v1.NewRunFilter(test.filter)
			require.NoError(t, err)

			matched, err := filter.Match(t.Context(), alice)
			require.NoError(t, err)
			require.Equal(t, test.alice, matched, "alice's run")

			matched, err = filter.Match(t.Context(), anonymous)
			require.NoError(t, err)
			require.Equal(t, test.anonymous, matched, "the run with no recorded starter")
		})
	}
}
