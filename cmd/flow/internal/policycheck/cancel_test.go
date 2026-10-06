package policycheck_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

// cancelAfter is a context that reports itself live for the first n calls to Err
// and cancelled from then on: a cancellation arriving part-way through a check,
// at a point a test can name rather than race for.
type cancelAfter struct {
	context.Context
	left atomic.Int64
}

func newCancelAfter(n int64) *cancelAfter {
	c := &cancelAfter{Context: context.Background()}
	c.left.Store(n)

	return c
}

func (c *cancelAfter) Err() error {
	if c.left.Add(-1) < 0 {
		return context.Canceled
	}

	return nil
}

// An interrupted check is an error, never a decision: the engine folds a
// cancelled evaluation into its fixed refusal, which would print `refused` and
// satisfy `--expect refused`.
func TestEvaluateOfACancelledContextIsAnErrorNotARefusal(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)
	gates := []policycheck.Gate{signal("approve"), signal("crash"), debugGate}
	subject := policycheck.Subject{Sender: id("ops@example.com", "team=sre")}

	before, cancel := context.WithCancel(t.Context())
	cancel()

	decisions, err := policycheck.Evaluate(before, wf, gates, subject)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, decisions, "no decision is returned for a check that did not finish")

	// Cancelled mid-way: the first gate is answered, the second is not, and the
	// answer to the first is not handed back as if it were the whole.
	decisions, err = policycheck.Evaluate(newCancelAfter(2), wf, gates, subject)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, decisions)

	// And the same check, left alone, answers.
	decisions, err = policycheck.Evaluate(t.Context(), wf, gates, subject)
	require.NoError(t, err)
	require.Len(t, decisions, 3)
}

func TestTableKeepsTheNoteOfAnAdmissionNoPolicyProduced(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)
	gates := []policycheck.Gate{signal("note"), signal("crash")}

	var results []policycheck.Result

	for _, name := range []string{"one", "two"} {
		decisions, err := policycheck.Evaluate(t.Context(), wf, gates,
			policycheck.Subject{Sender: id(name+"@example.com", "team=sre")})
		require.NoError(t, err)

		results = append(results, policycheck.Result{Name: name, Decisions: decisions})
	}

	var table bytes.Buffer
	require.NoError(t, policycheck.WriteTable(&table, gates, results))

	out := table.String()
	require.Contains(t, out, "signals.note admits without a policy: no `signals:` policy governs this signal")
	require.Equal(t, 1, bytes.Count(table.Bytes(), []byte("signals.note admits without a policy")),
		"each distinct note once, not once per row")
	require.NotContains(t, out, "signals.crash admits without a policy", "a gate a policy decided carries no note")

	report := policycheck.NewReport(gates, results)
	require.Contains(t, report.Results[0].Decisions[0].Note, "no `signals:` policy")
	require.Empty(t, report.Results[0].Decisions[1].Note)
}
