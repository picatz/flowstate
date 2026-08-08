package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAPendingWaitLineNamesTheSignalToSend is the render half of the parked-waits
// answer, and the reason it is worth rendering at all.
//
// The name travels a long way to be seen: a wait announces it, the progress query
// carries it, the server passes it through, and if the last step is missing the
// feature is present, tested and invisible. What the line has to contain is the
// argument `flow signal` takes, because that is the thing a position cannot say.
//
// Both `flow get` and `flow watch` render through this function, so there is one
// test rather than two: the two surfaces cannot drift because there is one
// renderer.
func TestAPendingWaitLineNamesTheSignalToSend(t *testing.T) {
	t.Parallel()

	now := time.Now()

	lines := pendingWaitLines(&v1.RunProgress{
		StepId: "each",
		PendingWaits: []*v1.PendingWait{{
			StepId:     "gate",
			Path:       []string{"each"},
			SignalName: "approve",
			Policed:    true,
			Deadline:   timestamppb.New(now.Add(90 * time.Second)),
		}},
	}, now)

	require.Equal(t,
		[]string{`waiting at each > gate for signal "approve" (authorized senders only), lapsing in 1m30s`},
		lines)
}

// TestAnUnboundedGateIsNotRenderedAsLapsing is the negative direction, and the one
// that decides whether a reader trusts the line.
//
// A gate with no `timeout:` waits for as long as the run may live, which is a
// different situation from a deadline that has not arrived yet. A renderer that
// printed a countdown for it would be inventing a fact about the workload, and one
// that printed "(authorized senders only)" for an unpoliced name would tell an
// operator their signal might be refused when nothing would refuse it.
func TestAnUnboundedGateIsNotRenderedAsLapsing(t *testing.T) {
	t.Parallel()

	lines := pendingWaitLines(&v1.RunProgress{
		StepId:       "hold",
		PendingWaits: []*v1.PendingWait{{StepId: "hold", SignalName: "nudge"}},
	}, time.Now())

	require.Equal(t, []string{`waiting at hold for signal "nudge"`}, lines)
}

// TestARunWithNoGatesRendersNothing keeps the two absences apart at the surface a
// person reads.
//
// A run that is working is parked on nothing, and a run whose worker did not answer
// reports no progress at all. Neither of them is a line, because the only line
// either could produce would be about the CLI rather than about the run.
func TestARunWithNoGatesRendersNothing(t *testing.T) {
	t.Parallel()

	require.Empty(t, pendingWaitLines(&v1.RunProgress{StepId: "working"}, time.Now()),
		"a run parked on nothing was rendered as waiting for something")
	require.Empty(t, pendingWaitLines(nil, time.Now()),
		"a run whose worker never answered was rendered as parked on nothing")
}

// TestATruncatedAnswerSaysSo keeps a partial answer from reading as the whole of
// it, which is the rule the schema states for the flag and the only reason the flag
// exists.
func TestATruncatedAnswerSaysSo(t *testing.T) {
	t.Parallel()

	lines := pendingWaitLines(&v1.RunProgress{
		PendingWaits:          []*v1.PendingWait{{StepId: "one", SignalName: "a"}},
		PendingWaitsTruncated: true,
	}, time.Now())

	require.Len(t, lines, 2)
	require.Equal(t, "and more gates than this run reports", lines[1])
}
