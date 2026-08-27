package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// The account `flow timeline` prints has one gap it cannot close, and these are
// the tests that the gap is *reported* rather than left for a reader to fall
// into.
//
// A step waiting out a retry backoff has no history event for the attempt that
// just failed — Temporal writes that failure onto the next attempt's start — so
// the rows show a scheduling that never ends and every failure except the most
// recent one. Which is the one somebody reading a stuck run came for.
//
// Some of the claims below are about a sentence appearing, and about it saying
// which moment its answer belongs to — it is a second read, taken after the
// rows, and a note that read as the account's own conclusion would be asserting
// a synchronisation nothing here has.
//
// The rest are about it *not* appearing: on a run that is merely busy, on a run
// whose retry has already started — where the failure is a row on the reader's
// screen — on a run that has finished, on a structured answer a program is
// parsing, and as an unfounded silence when the check could not be made. A note
// that fires on a healthy run is worse than no note, because it teaches a reader
// to ignore it; one that fires on a row already visible is worse still, because
// it is wrong rather than merely noisy.

// theRetryingRun is the segment every case here is about, spelled as a UUID
// because that is what the run id on the wire has to be.
const theRetryingRun = "0198f1e2-0000-7000-8000-000000000000"

// backingOff is the one state this note exists for: an attempt past the first,
// so one has failed, and the next one due at a moment still ahead, so that
// failure has no row yet.
//
// A helper rather than a literal in each case, because a fixture that sets only
// the attempt count describes something else entirely — a retry that is
// *already running*, whose failure the server has by then written into the
// account — and a test reaching for one would be arming its case with the very
// state the note must stay quiet about. That mistake is silent: the note simply
// does not appear, and an assertion that it does not appear passes for the wrong
// reason.
func backingOff(attempt int32, due time.Time) *v1.PendingActivity {
	return &v1.PendingActivity{
		Attempt:                  attempt,
		LastFailure:              "connection refused",
		NextAttemptScheduledTime: timestamppb.New(due),
	}
}

// fakeTimelineService answers the two RPCs `flow timeline` can make, and counts
// the second one.
//
// Its own fake rather than the one the signal and get tests share, and the
// counter is why. Several of the claims here are that a call did not happen, and
// a fake that records the last request it received cannot tell "never asked"
// from "asked, and something else was wrong with the answer" — the shape of
// stand-in that lets a gate regress while its test stays green.
type fakeTimelineService struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	// timeline is answered as-is, so a case describes an account exactly.
	timeline *v1.GetTimelineResponse

	// present is what Get answers with: the run's own now, which is where a
	// retrying step's latest failure lives.
	present    *v1.GetResponse
	presentErr error

	getCalls atomic.Int64
	gotGet   atomic.Pointer[v1.GetRequest]
}

// GetTimeline implements [flowstatev1connect.WorkflowServiceHandler].
func (f *fakeTimelineService) GetTimeline(
	_ context.Context,
	_ *connect.Request[v1.GetTimelineRequest],
) (*connect.Response[v1.GetTimelineResponse], error) {
	timeline := f.timeline
	if timeline == nil {
		timeline = &v1.GetTimelineResponse{}
	}

	return connect.NewResponse(timeline), nil
}

// Get implements [flowstatev1connect.WorkflowServiceHandler].
func (f *fakeTimelineService) Get(
	_ context.Context,
	req *connect.Request[v1.GetRequest],
) (*connect.Response[v1.GetResponse], error) {
	f.getCalls.Add(1)
	f.gotGet.Store(req.Msg)

	if f.presentErr != nil {
		return nil, f.presentErr
	}

	present := f.present
	if present == nil {
		present = &v1.GetResponse{}
	}

	return connect.NewResponse(present), nil
}

// serveTimelineFake stands the fake up and points the CLI at it, through the
// environment the --address flag defaults from — so a command built afterwards
// reaches it without any test naming the flag.
func serveTimelineFake(t *testing.T, fake *fakeTimelineService) {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(fake))

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// t.Setenv restores it afterwards and forbids t.Parallel, which is why
	// nothing that calls this is parallel.
	t.Setenv("FLOWSTATE_ADDRESS", server.URL)
}

// timelineCommand builds the real command, so the flags these tests drive are
// the flags `flow timeline` declares rather than a second set that could drift
// away from them.
func timelineCommand(t *testing.T) (*cobra.Command, *strings.Builder, *strings.Builder) {
	t.Helper()

	var out, errOut strings.Builder

	cmd := newTimelineCommand()
	addServerFlags(cmd)
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

// runningAccount is an account with no ending in it: a step scheduled, one
// attempt already failed, and nothing saying the segment stopped.
func runningAccount() *v1.GetTimelineResponse {
	return &v1.GetTimelineResponse{
		RunId: theRetryingRun,
		Entries: []*v1.TimelineEntry{
			{EventId: 5, Kind: v1.TimelineEntry_KIND_STEP_SCHEDULED, Step: "`charge`", Attempt: 1},
			{
				EventId: 9,
				Kind:    v1.TimelineEntry_KIND_STEP_FAILED,
				Step:    "`charge`",
				Attempt: 2,
				Failure: "connection refused",
			},
		},
	}
}

// TestATimelineReportsTheRetryNoRowCanHold is the whole feature end to end: a
// run whose account stops at a scheduling, and a reader who is told why.
func TestATimelineReportsTheRetryNoRowCanHold(t *testing.T) {
	fake := &fakeTimelineService{
		timeline: runningAccount(),
		present: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_RUNNING,
			PendingActivities: []*v1.PendingActivity{
				backingOff(3, time.Now().Add(12*time.Second)),
			},
		},
	}
	serveTimelineFake(t, fake)

	cmd, out, errOut := timelineCommand(t)
	require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Equal(t, int64(1), fake.getCalls.Load(),
		"an account with no ending in it did not ask what the run is doing now")

	// The segment the rows are about, not whichever is current: a workload can
	// continue as new between the two calls.
	assert.Equal(t, theRetryingRun, fake.gotGet.Load().GetRunId(),
		"the note asked about whichever run is latest, which may not be the one "+
			"the account above describes")

	notes := errOut.String()
	assert.Contains(t, notes, "one step is retrying")
	assert.Contains(t, notes, "attempt 3 is due in ",
		"the attempt and when it is due are the whole reason to read this line")
	assert.Contains(t, notes, "flow get flowstate-workflow-3f7c --run-id "+theRetryingRun,
		"the note says a fact is missing without saying what would report it")
	assert.Contains(t, notes, "no row of its own here until the next one starts",
		"nothing told the reader that paging further will not find the failure")

	// The account itself is the answer, and it goes to the stream a pipe reads.
	assert.NotContains(t, out.String(), "retrying",
		"prose about the run's present reached the stream `flow timeline | ...` consumes")
}

// TestARetryThatIsAlreadyRunningIsNotAMissingRow is the case an attempt count
// alone gets backwards.
//
// The server writes the previous attempt's failure into the account the moment
// the next attempt *starts*, so a step whose retry is already running has that
// failure on the reader's screen. A note there does not merely repeat it: it
// says the failure has no row, which is false, and points at another command to
// find something already three lines up. That is a wrong claim rather than a
// stale one, which is why it is worth its own test (Codex, #1142).
//
// The second case is the same state arriving differently. The schema says the
// next-attempt field is unset while an attempt runs, but the server fills it
// from Temporal's `scheduled_time` rather than its `next_attempt_schedule_time`,
// and only the latter is documented to be null for a scheduled or started
// activity — so a running attempt can reach the CLI carrying the moment it was
// itself scheduled, which is behind us. A time already past is therefore read as
// silence rather than as an overdue attempt.
func TestARetryThatIsAlreadyRunningIsNotAMissingRow(t *testing.T) {
	for _, c := range []struct {
		name    string
		pending *v1.PendingActivity
		because string
	}{
		{
			name: "the next attempt has started, so it has no schedule",
			pending: &v1.PendingActivity{
				Attempt:     3,
				LastFailure: "connection refused",
			},
			because: "the failure this note claims has no row was written into the " +
				"account when this attempt started, so the note points at a row the " +
				"reader can already see",
		},
		{
			name: "the schedule it carries is already behind us",
			pending: &v1.PendingActivity{
				Attempt:                  3,
				LastFailure:              "connection refused",
				NextAttemptScheduledTime: timestamppb.New(time.Now().Add(-time.Minute)),
			},
			because: "a moment already past is the attempt's own scheduling rather than " +
				"a wait still to come, and claiming a missing row on it is a guess",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeTimelineService{
				timeline: runningAccount(),
				present: &v1.GetResponse{
					Status:            v1.RunResponse_STATUS_RUNNING,
					PendingActivities: []*v1.PendingActivity{c.pending},
				},
			}
			serveTimelineFake(t, fake)

			cmd, _, errOut := timelineCommand(t)
			require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}))

			require.Equal(t, int64(1), fake.getCalls.Load(),
				"the run's present was never read, so this case proves nothing")

			notes := errOut.String()
			assert.NotContains(t, notes, "is retrying", c.because)
			assert.NotContains(t, notes, "asked after the rows above",
				"the note was printed for a step whose failure is already a row")
		})
	}
}

// TestABusyRunIsNotDescribedAsAStuckOne is the other negative direction, and the
// one that decides whether this note is worth having at all.
//
// Temporal reports every activity that is scheduled or started and not yet
// finished, so a perfectly healthy run doing its first attempt at one step has a
// pending activity. That step's scheduling is already in the account, with
// nothing missing from it — announcing a retry there would put a warning on
// every running workload, and a warning that is always on is one nobody reads.
func TestABusyRunIsNotDescribedAsAStuckOne(t *testing.T) {
	for _, c := range []struct {
		name    string
		present *v1.GetResponse
		because string
	}{
		{
			name:    "nothing pending at all",
			present: &v1.GetResponse{Status: v1.RunResponse_STATUS_RUNNING},
			because: "a run with no pending activity was described as retrying one",
		},
		{
			name: "a first attempt in flight",
			present: &v1.GetResponse{
				Status:            v1.RunResponse_STATUS_RUNNING,
				PendingActivities: []*v1.PendingActivity{{Attempt: 1}},
			},
			because: "a step running its first attempt has failed at nothing, and its " +
				"scheduling is already a row in the account above",
		},
		{
			name: "a first attempt still waiting for a worker",
			present: &v1.GetResponse{
				Status: v1.RunResponse_STATUS_RUNNING,
				PendingActivities: []*v1.PendingActivity{{
					Attempt:                  1,
					NextAttemptScheduledTime: timestamppb.New(time.Now().Add(30 * time.Second)),
				}},
			},
			because: "a schedule in the future is only half of it — nothing has failed, " +
				"so no row is missing and the wait is ordinary queueing",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeTimelineService{timeline: runningAccount(), present: c.present}
			serveTimelineFake(t, fake)

			cmd, _, errOut := timelineCommand(t)
			require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}))

			require.Equal(t, int64(1), fake.getCalls.Load(),
				"the run's present was never read, so this case proves nothing")

			notes := errOut.String()
			assert.NotContains(t, notes, "retrying", c.because)

			// The opening clause too, so a refactor that emits the note
			// unconditionally and leaves the claim empty is caught as well as
			// one that emits the claim.
			assert.NotContains(t, notes, "asked after the rows above",
				"the note was printed for a run that has nothing to say it about")
		})
	}
}

// TestTheRetryNoteSaysWhereItsAnswerCameFrom is the honest half of a race this
// verb cannot close from here.
//
// GetTimeline reads a history and the note is a second read taken afterwards,
// so the two describe two moments. Opening with the claim would assert a
// synchronisation that does not exist: a step can begin retrying after the rows
// were read, and the note would then read as the account's own last line about
// something no row above mentions. Naming the moment first makes it a later
// observation instead.
//
// The other direction — a retry that ends between the two reads, so no note is
// printed for a gap the rows really had — is not assertable from here and is
// the residual this wording chooses to pay. `noteRetryingSteps` says why, and
// the fix is one server-side snapshot rather than anything this file can do.
func TestTheRetryNoteSaysWhereItsAnswerCameFrom(t *testing.T) {
	t.Parallel()

	const opening = "the run's present, asked after the rows above: "

	due := time.Now().Add(30 * time.Second)

	for _, c := range []struct {
		name string
		msg  *v1.GetResponse
	}{
		{
			name: "one waiting step",
			msg:  &v1.GetResponse{PendingActivities: []*v1.PendingActivity{backingOff(3, due)}},
		},
		{
			name: "several waiting steps",
			msg: &v1.GetResponse{PendingActivities: []*v1.PendingActivity{
				backingOff(3, due),
				backingOff(5, due),
			}},
		},
		{
			name: "a clipped list it cannot answer from",
			msg: &v1.GetResponse{
				PendingActivities:          []*v1.PendingActivity{{Attempt: 1}},
				PendingActivitiesTruncated: true,
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			footer := retryingStepsFooter("flowstate-workflow-3f7c", theRetryingRun, c.msg, time.Now())

			require.NotEmpty(t, footer, "the case proves nothing if there is no note")
			assert.True(t, strings.HasPrefix(footer, opening),
				"the note leads with a present-tense claim, which reads as the account's "+
					"own conclusion about a moment the account was never read at:\n%s", footer)
		})
	}
}

// TestAFinishedAccountNeverAsksWhatTheRunIsDoing is the gate that keeps this
// from being a second round trip on every invocation.
//
// A segment whose account carries its own ending is not RUNNING, and pending
// activities are reported only for a run that is — so there is nothing to ask
// about. That is the common case for this verb, whose whole reason to exist is
// the question left once a run has finished.
//
// The fake is armed with a step waiting out a backoff on purpose: if the gate
// stopped working, the note would appear, and this test would say so rather than
// merely counting.
func TestAFinishedAccountNeverAsksWhatTheRunIsDoing(t *testing.T) {
	for _, c := range []struct {
		name   string
		ending v1.TimelineEntry_Kind
	}{
		{name: "a run that ended", ending: v1.TimelineEntry_KIND_RUN_ENDED},
		{name: "a segment that continued as new", ending: v1.TimelineEntry_KIND_RUN_CONTINUED},
	} {
		t.Run(c.name, func(t *testing.T) {
			account := runningAccount()
			account.Entries = append(account.Entries,
				&v1.TimelineEntry{EventId: 12, Kind: c.ending})

			fake := &fakeTimelineService{
				timeline: account,
				present: &v1.GetResponse{
					Status: v1.RunResponse_STATUS_RUNNING,
					PendingActivities: []*v1.PendingActivity{
						backingOff(4, time.Now().Add(30*time.Second)),
					},
				},
			}
			serveTimelineFake(t, fake)

			cmd, _, errOut := timelineCommand(t)
			require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}))

			assert.Equal(t, int64(0), fake.getCalls.Load(),
				"a second round trip was spent on a segment that has already stopped")
			assert.NotContains(t, errOut.String(), "retrying",
				"a finished segment was reported as retrying a step")
		})
	}
}

// TestTheRetryNoteStaysOffAStructuredAnswer is the rule every verb here follows:
// a document a program indexes into gets fields, not sentences.
//
// `flow get -o json` withholds its own retrying lines the same way, and there
// the fact is a field of the answer rather than prose beside it. So the second
// call is not made either — a round trip whose only output is suppressed is a
// cost with no reader.
func TestTheRetryNoteStaysOffAStructuredAnswer(t *testing.T) {
	fake := &fakeTimelineService{
		timeline: runningAccount(),
		present: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_RUNNING,
			PendingActivities: []*v1.PendingActivity{
				backingOff(3, time.Now().Add(30*time.Second)),
			},
		},
	}
	serveTimelineFake(t, fake)

	cmd, out, errOut := timelineCommand(t)
	require.NoError(t, cmd.Flags().Set("output", string(FormatJSON)))
	require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}))

	// The document is the whole of stdout, which is only true if nothing else
	// was written there.
	var back v1.GetTimelineResponse
	require.NoError(t, protojson.Unmarshal([]byte(out.String()), &back),
		"stdout no longer parses as the server's own answer:\n%s", out.String())
	require.Len(t, back.GetEntries(), 2)

	assert.NotContains(t, out.String(), "retrying",
		"prose reached a document a program is parsing")
	assert.NotContains(t, errOut.String(), "retrying",
		"a structured answer carried prose on the side")
	assert.Equal(t, int64(0), fake.getCalls.Load(),
		"a round trip was spent to produce a sentence this format suppresses")
}

// TestTheRetryNoteAndTheTruncationNoteReadAsOneEnding is the two of them
// together, which is where they could contradict each other.
//
// Truncation offers a reader --after-event-id and says the account is partial.
// A waiting step's latest failure is not further along that account — it is
// nowhere in it — so the two notes have to compose into one ending rather than
// leaving a reader paging forever for a row that will not arrive until the next
// attempt starts.
func TestTheRetryNoteAndTheTruncationNoteReadAsOneEnding(t *testing.T) {
	account := runningAccount()
	account.Truncated = true

	fake := &fakeTimelineService{
		timeline: account,
		present: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_RUNNING,
			PendingActivities: []*v1.PendingActivity{
				backingOff(3, time.Now().Add(30*time.Second)),
			},
		},
	}
	serveTimelineFake(t, fake)

	cmd, _, errOut := timelineCommand(t)
	require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}))

	notes := errOut.String()

	clipped := strings.Index(notes, "not the whole of this run's account")
	retrying := strings.Index(notes, "one step is retrying")

	require.NotEqual(t, -1, clipped, "the truncation note stopped being printed")
	require.NotEqual(t, -1, retrying, "the retry note is missing beside a truncation note")

	assert.Less(t, clipped, retrying,
		"the note about something no account can hold was printed before the notes "+
			"about how much of the account this is, which reads as two afterthoughts "+
			"rather than one ending")

	assert.Contains(t, notes, "no row of its own here until the next one starts",
		"a reader just handed --after-event-id was left to page forward for a row "+
			"that is not further along the account")
}

// TestAnUnreadablePresentIsSaidRatherThanTakenForSilence is what stops this note
// from teaching a lie.
//
// Once the sentence exists, its absence reads as "nothing is mid-retry". A check
// that never happened must therefore not be reported the same way as one that
// came back empty. It is still not an error: the account is what the command was
// asked for, and it arrived.
func TestAnUnreadablePresentIsSaidRatherThanTakenForSilence(t *testing.T) {
	fake := &fakeTimelineService{
		timeline:   runningAccount(),
		presentErr: connect.NewError(connect.CodeUnavailable, errors.New("no worker answered")),
	}
	serveTimelineFake(t, fake)

	cmd, out, errOut := timelineCommand(t)
	require.NoError(t, runTimeline(cmd, []string{"flowstate-workflow-3f7c"}),
		"a failed aside about the run's present turned a successful read into a failure")

	notes := errOut.String()
	assert.Contains(t, notes, "whether a step is retrying is not known here",
		"a check that could not be made was indistinguishable from one that found nothing")
	assert.Contains(t, notes, "no worker answered",
		"the reason the check failed was dropped, so nobody can act on it")
	assert.NotContains(t, notes, "one step is retrying",
		"a claim was made about an answer that never arrived")

	// The rows still printed, which is the point of not failing.
	assert.Contains(t, out.String(), "charge")
}

// TestTheRetryNoteCountsWhatItCanAndInventsNothing pins the sentence itself,
// against a fixed clock so the countdown is a claim rather than a race.
//
// The heads differ and the tail does not, which is deliberate: the tail is what
// says *why* a failure is missing, and it has to be true whether one step is
// waiting or forty.
func TestTheRetryNoteCountsWhatItCanAndInventsNothing(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 9, 0, 0, 0, time.UTC)

	// Spelled out rather than built from retryNoteProvenance, so a change to the
	// clause has to be made twice on purpose rather than once by accident.
	const opening = "the run's present, asked after the rows above: "

	const tail = "; a failed attempt has no row of its own here until the next one " +
		"starts, so `flow get flowstate-workflow-3f7c --run-id " + theRetryingRun +
		"` is what reports it"

	for _, c := range []struct {
		name    string
		msg     *v1.GetResponse
		want    string
		because string
	}{
		{
			name: "one waiting step names its attempt and when it is due",
			msg: &v1.GetResponse{PendingActivities: []*v1.PendingActivity{
				backingOff(3, now.Add(12*time.Second)),
			}},
			want: opening + "one step is retrying — attempt 3 is due in 12s" + tail,
			because: "attempt 3 and the countdown are one attempt, not two, so naming a " +
				"further `next attempt` after it would invent a try nobody is waiting for",
		},
		{
			name: "a retry that is already running says nothing",
			msg: &v1.GetResponse{PendingActivities: []*v1.PendingActivity{{
				Attempt:     3,
				LastFailure: "connection refused",
			}}},
			want: "",
			because: "the failure is a row by the time the next attempt starts, so this " +
				"would claim a missing row the reader can see",
		},
		{
			name: "a schedule already behind us says nothing",
			msg: &v1.GetResponse{PendingActivities: []*v1.PendingActivity{
				backingOff(3, now.Add(-time.Minute)),
			}},
			want: "",
			because: "a moment already past is the running attempt's own scheduling " +
				"rather than a wait still to come",
		},
		{
			name: "several waiting steps name the furthest along",
			msg: &v1.GetResponse{PendingActivities: []*v1.PendingActivity{
				backingOff(2, now.Add(time.Minute)),
				backingOff(7, now.Add(time.Minute)),
				backingOff(3, now.Add(time.Minute)),
			}},
			want: opening + "3 steps are retrying — the furthest on attempt 7" + tail,
			because: "a run where one step is on attempt 9 is a different situation from " +
				"one where four are on attempt 2",
		},
		{
			name: "a first attempt in flight is not counted as a retry",
			msg: &v1.GetResponse{PendingActivities: []*v1.PendingActivity{
				{Attempt: 1, NextAttemptScheduledTime: timestamppb.New(now.Add(time.Minute))},
				backingOff(4, now.Add(45*time.Second)),
			}},
			want:    opening + "one step is retrying — attempt 4 is due in 45s" + tail,
			because: "the busy step's scheduling is already a row, so nothing about it is missing",
		},
		{
			name:    "a run with nothing pending says nothing",
			msg:     &v1.GetResponse{},
			want:    "",
			because: "a note on every finished run is a note nobody reads",
		},
		{
			name: "a clipped list with nothing waiting in it admits it cannot tell",
			msg: &v1.GetResponse{
				PendingActivities:          []*v1.PendingActivity{{Attempt: 1}},
				PendingActivitiesTruncated: true,
			},
			want: opening + "this run has more steps pending than it reports, so whether " +
				"one of them is retrying cannot be told from here" + tail,
			because: "the steps past the bound may be the waiting ones, and silence " +
				"would report some of them as all of them",
		},
		{
			name: "a clipped list beside a waiting step says both",
			msg: &v1.GetResponse{
				PendingActivities:          []*v1.PendingActivity{backingOff(3, now.Add(20*time.Second))},
				PendingActivitiesTruncated: true,
			},
			want: opening + "one step is retrying — attempt 3 is due in 20s, and more " +
				"steps are pending than this reports" + tail,
			because: "pending rather than retrying, because that is all the flag says",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, c.want,
				retryingStepsFooter("flowstate-workflow-3f7c", theRetryingRun, c.msg, now),
				c.because)
		})
	}
}

// TestTheRetryNoteNeverCarriesTheWorkloadsOwnText is the containment claim.
//
// The sentence that failed is the workload's, and this line does not reproduce
// it: it names the count, the attempt, and the command that reports the rest.
// So there is nothing here for a newline to fabricate a line out of, or for an
// escape to restyle a terminal with — and the two names that *are* interpolated
// come from outside this process, so both are escaped.
func TestTheRetryNoteNeverCarriesTheWorkloadsOwnText(t *testing.T) {
	t.Parallel()

	const nasty = "boom\nflowstate: everything is fine\x1b[31m\tno it is not"

	footer := retryingStepsFooter(
		"flowstate-workflow-3f7c\nfake\x1b[31m",
		theRetryingRun,
		&v1.GetResponse{PendingActivities: []*v1.PendingActivity{{
			Attempt:                  3,
			LastFailure:              nasty,
			NextAttemptScheduledTime: timestamppb.New(time.Now().Add(30 * time.Second)),
		}}},
		time.Now(),
	)

	require.NotEmpty(t, footer, "the case proves nothing if there is no note")

	assert.NotContains(t, footer, nasty,
		"the workload's own failure sentence was reproduced on this line, where "+
			"`flow get` is what reports it")
	assert.NotContains(t, footer, "\n",
		"a note that promises one line invented a second one")
	assert.NotContains(t, footer, "\x1b[",
		"text this process did not write chose how the reader's terminal looks")
	assert.Contains(t, footer, `\n`,
		"the workflow id's newline was dropped rather than shown")
}

// TestTheRetryNoteNamesNoRunItCannotAddress covers the one interpolation that
// can be absent.
//
// A run id is what makes `flow get` address the same segment the rows above
// describe, and the note names it — but a `--run-id ` with nothing after it is a
// command that does not run, which is worse than one that resolves the latest
// segment.
func TestTheRetryNoteNamesNoRunItCannotAddress(t *testing.T) {
	t.Parallel()

	footer := retryingStepsFooter("flowstate-workflow-3f7c", "",
		&v1.GetResponse{PendingActivities: []*v1.PendingActivity{
			backingOff(3, time.Now().Add(30*time.Second)),
		}},
		time.Now())

	assert.Contains(t, footer, "`flow get flowstate-workflow-3f7c` is what reports it")
	assert.NotContains(t, footer, "--run-id",
		"the note offered a flag with no value after it")
}
