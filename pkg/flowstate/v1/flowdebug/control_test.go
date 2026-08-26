package flowdebug_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Moving a run without typing at it.

// walked runs a session over a list of steps on a goroutine of its own, the way
// the engine does, and reports what came back.
//
// The channel is buffered so the run's goroutine never outlives the test
// waiting to hand over a result nobody read — a leaked goroutine holding a
// session is exactly what these tests would otherwise produce on every failure.
func walked(t *testing.T, session *flowdebug.Session, ids ...string) <-chan error {
	t.Helper()

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{},
	})

	finished := make(chan error, 1)
	go func() {
		for _, id := range ids {
			if err := session.BeforeStep(t.Context(), markStep(id), scope); err != nil {
				finished <- err

				return
			}
		}
		finished <- nil
	}()

	return finished
}

// TestARunCanBeWalkedWithoutATerminal is the seam, end to end: a session with
// no In, no Console and nothing to type at holds the run and moves it on
// command.
//
// This is the shape a debug adapter is: no stream anywhere, a request arriving
// on a socket, and a run that has to stop and start on it. Before this the
// session had no way to be told anything — [Session.BeforeStep] parks blocked
// on a line of text, and a session with nothing to read resumed at every stop
// rather than waiting.
func TestARunCanBeWalkedWithoutATerminal(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	finished := walked(t, session, "build", "test", "deploy")

	// The run stops before its first step, which is what `--debug` with no
	// breakpoints means. Nothing is polled for here: the command waits for the
	// run to reach a prompt, so the test does not race the run's goroutine.
	at, err := session.Step(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "test", at.Step,
		"stepping from the first stop did not arrive at the second step")
	assert.False(t, at.Autopsy)

	at, err = session.Step(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "deploy", at.Step)

	// And letting it go finishes the run rather than holding it.
	require.NoError(t, session.Control(t.Context(), "continue"))

	select {
	case runErr := <-finished:
		require.NoError(t, runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("the run did not finish after being told to continue")
	}
}

// TestACommandIssuedBeforeTheRunStartsWaitsForTheStopAfterIt is the ordering
// the seam turns on, and the one a test written the obvious way cannot see.
//
// A caller may command a run that has not started: an adapter configures its
// breakpoints and asks for the first stop before handing the workflow to the
// engine, which is `launch` then `configurationDone` in DAP's own order. The
// command then waits for a prompt, and the pause it is delivered into is one
// that did not exist when the caller asked.
//
// So "wait for a pause newer than the one I left" cannot be measured by the
// caller. The first version of this read the generation before sending and
// walked a three-step run reporting step one twice — and every test that
// commanded an *already paused* run passed it, because there the two readings
// agree. The generation therefore comes back from the boundary that took the
// command, which is the only place it is knowable.
//
// The window below is what puts the command in flight before the run exists;
// it is not what the assertion rests on. Nothing can answer a command while
// there is no boundary to receive it, and the whole sequence is asserted, so a
// walk shifted by one stop fails whichever way the scheduler goes.
func TestACommandIssuedBeforeTheRunStartsWaitsForTheStopAfterIt(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	first := make(chan flowdebug.Position, 1)
	go func() {
		at, stepErr := session.Step(t.Context())
		assert.NoError(t, stepErr)
		first <- at
	}()

	// With no run there is no boundary to take the command, so an answer here
	// would mean the session invented one.
	select {
	case at := <-first:
		t.Fatalf("a command was answered before the run existed, at %q", at.Step)
	case <-time.After(100 * time.Millisecond):
	}

	finished := walked(t, session, "build", "test", "deploy")

	at := <-first
	assert.Equal(t, "test", at.Step,
		"the command was taken at the run's first stop, so the run should have moved "+
			"past it rather than reporting the stop that took it")

	at, err = session.Step(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "deploy", at.Step, "the walk is a stop behind where it should be")

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}

// TestTypedAndProgrammaticControlWalkTheSameRun is the property that makes the
// seam worth having rather than a second debugger.
//
// A command delivered through [Session.Control] is dispatched by the same loop,
// through the same `dispatch`, as one somebody typed — so the two fronts cannot
// disagree about what `step` does. A programmatic front that resumed the run by
// writing the session's own fields would be a second implementation of every
// verb, and this repository's most-paid-for shape is one meaning written down
// twice.
//
// Asserted as the *same walk* rather than as two walks that each look
// plausible: the positions and the account both have to match, position for
// position.
func TestTypedAndProgrammaticControlWalkTheSameRun(t *testing.T) {
	t.Parallel()

	steps := []string{"build", "test", "deploy"}

	// Typed: the same three commands down a stream, with no console anywhere.
	var typedOut strings.Builder
	typed, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("step\nstep\ncontinue\n"),
		Out: &typedOut,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = typed.Close() })

	require.NoError(t, <-walked(t, typed, steps...))

	// Programmatic: the same three commands, no stream at all.
	var drivenOut strings.Builder
	driven, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &drivenOut})
	require.NoError(t, err)
	t.Cleanup(func() { _ = driven.Close() })

	finished := walked(t, driven, steps...)

	var reached []string
	for range 2 {
		at, stepErr := driven.Step(t.Context())
		require.NoError(t, stepErr)
		reached = append(reached, at.Step)
	}
	require.NoError(t, driven.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)

	assert.Equal(t, []string{"test", "deploy"}, reached,
		"the programmatic walk did not visit the steps the same commands visit when typed")

	// The account is the same too, which is the half a position comparison
	// cannot see: a front that moved the run correctly and announced it
	// differently would still be two debuggers.
	assert.Equal(t,
		strings.ReplaceAll(typedOut.String(), flowdebug.Prompt, ""),
		drivenOut.String(),
		"the same commands through the two fronts printed different accounts of one run")

	// And the recording agrees, so a programmatic session replays as a typed
	// one — the record-and-replay half this vocabulary is shared for.
	assert.Equal(t, typed.Script(), driven.Script(),
		"a programmatically driven session recorded a script a person could not have typed")
}

// TestAControlledSessionOutlivesItsStream keeps the two fronts composable.
//
// A session may have both: a script that sets things up and an adapter that
// takes over. A stream running out is the end of *the stream*, and the reader
// treated it as the end of the debugging — resuming the run to the end,
// unattended, while a controller was still holding it. That is the message on
// that path doing exactly what it says, arriving because the two fronts were
// composed and only one was consulted.
//
// The script's one command deliberately does not move the run, so the two
// fronts are never driving at once: mixing a stream and a controller
// *concurrently* is a caller mistake this does not pretend to arbitrate, and a
// test that did it would be asserting a coin toss.
func TestAControlledSessionOutlivesItsStream(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		In:         strings.NewReader("break deploy\n"),
		Out:        &out,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	finished := walked(t, session, "build", "test", "deploy")

	// The handover, observed rather than assumed: once the script's command is
	// recorded, the stream is spent and the session is holding the run on the
	// controller alone.
	require.Eventually(t, func() bool { return len(session.Script()) == 1 },
		10*time.Second, time.Millisecond,
		"the script's command was never accepted")

	at, err := session.Continue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "deploy", at.Step,
		"the run was let go when the script ran out, so the controller lost it")

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}

// stalling is a console that never answers, which is what a real terminal with
// nobody at it does.
type stalling struct{ release chan struct{} }

func (c *stalling) Prompt() (string, error) {
	<-c.release

	return "", io.EOF
}

// TestAControllerIsNotBlockedByAConsoleNobodyIsTypingAt is the composition
// deadlocking on its third stop.
//
// The reader is asked for a line at each boundary and the request is tracked in
// a one-token channel, which was documented as "at most one request is
// outstanding" — true while a line could only ever come from the reader. Once a
// control command can satisfy a boundary instead, the reader keeps owing a line
// nobody collects: the next boundary queues a second token, and the one after
// that blocks forever on a full channel, in a select with no context arm and no
// control arm to rescue it (Codex, #1122).
//
// Three stops is what it takes to show, so this walks four steps. The console
// here never answers at all, which is not exotic — it is a terminal with nobody
// at it, beside an adapter doing the driving.
func TestAControllerIsNotBlockedByAConsoleNobodyIsTypingAt(t *testing.T) {
	t.Parallel()

	console := &stalling{release: make(chan struct{})}
	// Released at the end so the reader goroutine does not outlive the test
	// parked inside Prompt.
	t.Cleanup(func() { close(console.release) })

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Console:    console,
		Out:        &out,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	finished := walked(t, session, "build", "test", "deploy", "ship")

	// Bounded, so a boundary that can no longer hear a command fails as an
	// error naming the stop rather than as a test timeout.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	for _, want := range []string{"test", "deploy", "ship"} {
		at, stepErr := session.Step(ctx)
		require.NoError(t, stepErr,
			"the run stopped somewhere its controller could no longer reach it, on the "+
				"way to %q", want)
		assert.Equal(t, want, at.Step)
	}

	require.NoError(t, session.Control(ctx, "continue"))
	require.NoError(t, <-finished)
}

// TestControlIsRefusedOnASessionThatCannotBeControlled is a hang turned into an
// answer.
//
// Without [Options.Controlled] a session with nothing to read resumes at every
// stop, so nothing is ever waiting on the other end of a command. Accepting one
// would park the caller until its context expired, which is a hang with a
// timeout on it — and the caller would have no way to tell that from a run that
// was simply slow.
func TestControlIsRefusedOnASessionThatCannotBeControlled(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = session.Control(ctx, "step")
	assert.ErrorIs(t, err, flowdebug.ErrNotControlled)
	assert.NotErrorIs(t, err, context.DeadlineExceeded,
		"the refusal arrived as a timeout, which a caller cannot tell from a slow run")

	// And the property that refusal protects: such a session still resumes at
	// every stop rather than holding a run nobody can move.
	require.NoError(t, <-walked(t, session, "build", "test"))
}

// TestAWaitEndsWhenTheSessionDoes is the answer to a question this package
// cannot ask.
//
// [v1.Debugger] is called before each step and [v1.RunObserver] after each one.
// Neither has a callback for the run *finishing*, so a session whose run
// completed cleanly is indistinguishable from one whose next step has not
// arrived yet. Whoever owns the run does know, and closing the session is how
// they say so — which has to end a caller's wait rather than leave it parked
// on a run that will never stop again.
func TestAWaitEndsWhenTheSessionDoes(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)

	finished := walked(t, session, "build")

	// One step, and the run is over — with nothing to announce that it is.
	waited := make(chan error, 1)
	go func() {
		_, stepErr := session.Step(t.Context())
		waited <- stepErr
	}()

	require.NoError(t, <-finished)
	require.NoError(t, session.Close())

	select {
	case err := <-waited:
		assert.ErrorIs(t, err, flowdebug.ErrRunOver,
			"a caller waiting on a run that had ended got something other than the fact")
	case <-time.After(10 * time.Second):
		t.Fatal("closing the session left a caller waiting for a stop that cannot come")
	}
}

// TestClosingReleasesARunHeldByNobody is the escape [Options.Controlled]
// promises.
//
// A controlled session waits at every stop, which is the point — and a
// controller that goes away therefore holds someone's run open indefinitely.
// A reading session learns of [Session.Close] because its reader exits and
// closes the channel the boundary is parked on; a controlled one has no reader
// to hear it from, so without the session's own done channel in that select
// the run is parked on a command that is never coming.
func TestClosingReleasesARunHeldByNobody(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)

	finished := walked(t, session, "build", "test")

	// Held: nothing has been sent, and nothing will be.
	require.Eventually(t, func() bool { _, paused := session.Paused(); return paused },
		10*time.Second, time.Millisecond,
		"the run never stopped, so this cannot be testing what happens when it is let go")

	require.NoError(t, session.Close())

	select {
	case runErr := <-finished:
		require.NoError(t, runErr,
			"closing a session under a held run failed the run rather than letting it finish")
	case <-time.After(10 * time.Second):
		t.Fatal("closing the session left the run parked on a command that is never coming")
	}
}

// TestAMovementCommandAtAnAutopsyEndsTheWait is the stop that does not come.
//
// An autopsy takes `step`, `continue` and `until` as requests to *leave*: all
// three land in the clause that records `quit` and returns, which the prompt's
// own help says out loud. So a movement verb there has no next stop by
// construction — and a wait for one blocked until [Session.Close] or the
// context expired.
//
// The quiet version is worse than the hang. A session reused for a second case
// would eventually see that case's first pause and hand it back as the result
// of moving a run that had already finished (Codex, #1122).
func TestAMovementCommandAtAnAutopsyEndsTheWait(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), nil,
			[]string{"a failure"})
	}()

	require.Eventually(t, func() bool {
		at, paused := session.Paused()

		return paused && at.Autopsy
	}, 10*time.Second, time.Millisecond, "the autopsy never took hold")

	// Bounded well under the ten seconds a hang would take, so this fails as a
	// wrong answer rather than as a timeout somebody has to interpret.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	at, err := session.Continue(ctx)
	assert.ErrorIs(t, err, flowdebug.ErrRunOver,
		"moving a finished run waited for a stop that cannot come")
	assert.NotErrorIs(t, err, context.DeadlineExceeded,
		"the answer arrived as a timeout, which a caller cannot tell from a slow run")
	assert.Empty(t, at.Step)

	// And the command still did what it says at that prompt: it left.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the autopsy did not take the movement command as a request to leave")
	}
}

// TestASecondCallerCanGiveUpWaitingForTheControlSlot is [Session.Control]'s own
// promise, kept where it was broken.
//
// Commands are serialized, because a run has one position and two callers
// moving it at once is a caller mistake rather than something to arbitrate.
// The slot was a [sync.Mutex], which has no cancellable acquire — so a second
// caller blocked on it *before* reaching any of the context-aware selects, and
// if the first command was never consumed it never returned at all, whatever
// its deadline said (Codex, #1122).
func TestASecondCallerCanGiveUpWaitingForTheControlSlot(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	// No run, so nothing can ever take this command: the first caller holds
	// the slot for as long as the session lives.
	held, cancelHeld := context.WithCancel(t.Context())
	defer cancelHeld()

	holding := make(chan struct{})
	go func() {
		close(holding)
		_ = session.Control(held, "step")
	}()
	<-holding

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err = session.Control(ctx, "continue")
	require.Error(t, err, "a second command was accepted while the first still held the slot")
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"the second caller did not come back when its context expired, so its deadline "+
			"bounded nothing")
}

// TestACommandRacingCloseIsEitherDispatchedOrRefusedButNotBoth is the shutdown
// race, asserted as the invariant rather than as an outcome.
//
// When [Session.Close] lands on a controller already parked in its send, both
// the control arm and the done arm of the boundary's select are ready, and Go
// picks between ready arms at random. So a pending `quit` could be dispatched
// *after* shutdown — turning the clean release Close promises into an
// abandoned run — while its sender was told [ErrRunOver] by the same race one
// layer up (Codex, #1122).
//
// Which side wins is not the property and is not worth pinning: a close and a
// command issued at the same instant have no true order. What must hold is
// that the two agree. A caller told its command was refused must be able to
// rely on the run not having taken it, because "refused" is what it will act
// on.
//
// Run many times because each iteration only samples one interleaving; the
// assertion inside one iteration is exact either way, so this fails on the
// first inconsistent pair rather than on a distribution.
//
// What it does *not* do is pin the two guards that make the invariant hold.
// Checked by reintroducing each, and neither fails this: they are complementary,
// so each is masked by the other, and the window they close needs the boundary
// and the sender to choose their non-shutdown arms at the same instant. This is
// the end-to-end statement of the property; the guards themselves are argued in
// their own comments rather than asserted here, because nothing outside this
// package can tell them apart.
func TestACommandRacingCloseIsEitherDispatchedOrRefusedButNotBoth(t *testing.T) {
	t.Parallel()

	dispatched, refused := 0, 0

	for range 200 {
		var out strings.Builder
		session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
		require.NoError(t, err)

		finished := walked(t, session, "build", "test")

		require.Eventually(t, func() bool { _, paused := session.Paused(); return paused },
			10*time.Second, time.Millisecond, "the run never stopped")

		// Released together, so the close and the command genuinely contend.
		start := make(chan struct{})
		controlled := make(chan error, 1)
		go func() {
			<-start
			controlled <- session.Control(t.Context(), "quit")
		}()
		go func() {
			<-start
			_ = session.Close()
		}()
		close(start)

		controlErr := <-controlled
		runErr := <-finished

		// `quit` at a breakpoint ends the run; a session let go by Close
		// instead resumes it. So the run's own answer says which happened.
		took := runErr != nil

		switch {
		case took:
			dispatched++
			assert.NoError(t, controlErr,
				"the run took the command and its sender was told it had not been delivered")
			assert.Contains(t, session.Script(), "quit",
				"the run ended on a command the session did not record")
		default:
			refused++
			assert.ErrorIs(t, controlErr, flowdebug.ErrRunOver,
				"the command was refused and its sender was told it had been delivered")
		}

		if t.Failed() {
			return
		}
	}

	// Not an assertion about the split — either side may dominate on any
	// machine — but a session that only ever took one path would make the
	// consistency above true for a reason that has nothing to do with the fix.
	t.Logf("dispatched %d, refused %d", dispatched, refused)
}

// TestCloseUnparksASenderRatherThanLeavingItsCommandToBeTaken is why the
// shutdown race is narrow, stated so the guards that close it are not mistaken
// for the whole story.
//
// A command parked in its send is not left lying there for a later boundary to
// pick up: [Session.deliver]'s send waits on the session's done channel too, so
// [Session.Close] releases the sender with [ErrRunOver] and the command is
// never handed over. That is what makes "dispatched after shutdown" require
// genuine simultaneity — the boundary choosing the control arm at the same
// instant the sender chooses its own send — rather than being the ordinary
// outcome of closing a session somebody is driving.
//
// Worth stating plainly: this passes with the guards removed. It pins the
// surrounding behaviour, not them. See the reply on #1122 for why neither
// guard is separately observable from outside.
func TestCloseUnparksASenderRatherThanLeavingItsCommandToBeTaken(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)

	sent := make(chan struct{})
	refused := make(chan error, 1)
	go func() {
		close(sent)
		// Parks: there is no run, so nothing can take it.
		refused <- session.Control(t.Context(), "quit")
	}()
	<-sent

	require.NoError(t, session.Close())

	select {
	case err := <-refused:
		assert.ErrorIs(t, err, flowdebug.ErrRunOver,
			"closing the session did not release a caller parked on a command nobody could take")
	case <-time.After(10 * time.Second):
		t.Fatal("a parked sender outlived the session it was sending to")
	}

	// And the command really is gone rather than queued: a run started
	// afterwards is released to finish, not abandoned by a late `quit`.
	require.NoError(t, <-walked(t, session, "build", "test"),
		"a command sent before Close was taken by a boundary after it")
	assert.NotContains(t, session.Script(), "quit",
		"a closed session recorded a command it should never have taken")
}

// TestAControlledCommandIsOneLineAndBounded is the bound this surface owes.
//
// [MaxCommandBytes] is enforced by whatever reads a typed line, so the text
// path is bounded by the surface it arrives on — and a caller reaching
// [Session.Control] arrives on no such surface. The line break is the other
// half: the reader hands the loop one command with no terminator in it, so a
// caller able to deliver two would be sending something no prompt could have
// produced, with the second landing wherever the first left the run.
func TestAControlledCommandIsOneLineAndBounded(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = session.Control(ctx, strings.Repeat("a", flowdebug.MaxCommandBytes+1))
	require.Error(t, err, "a command past the bound was delivered")
	assert.NotErrorIs(t, err, context.DeadlineExceeded)

	err = session.Control(ctx, "inspect 1\nquit")
	require.Error(t, err, "two commands arrived as one line")
	assert.NotErrorIs(t, err, context.DeadlineExceeded)

	// A step id is one word, because the line this composes has no way to
	// quote one: `until "a b"` would silently become `until a`.
	_, err = session.Until(ctx, "a b")
	require.Error(t, err)
	assert.NotErrorIs(t, err, context.DeadlineExceeded)

	_, err = session.Until(ctx, "deploy\x1b[31m")
	require.Error(t, err, "a step id carrying a terminal escape was composed into a command")
}

// TestUntilRunsToTheNamedStep is the third movement verb, and the one whose
// argument comes from outside.
func TestUntilRunsToTheNamedStep(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	finished := walked(t, session, "build", "test", "lint", "deploy")

	at, err := session.Until(t.Context(), "deploy")
	require.NoError(t, err)
	assert.Equal(t, "deploy", at.Step,
		"`until` stopped somewhere other than the step it was given")

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}

// TestTheValueSurfaceAnswersAtAControlledStop is the join between the two
// halves.
//
// Moving a run is worth little without asking about where it stopped, and
// asking is worth little without being able to move. A debug adapter needs
// both against the same pause, from the same goroutine, with no terminal in
// sight — and each landed without the other having been checked against it.
func TestTheValueSurfaceAnswersAtAControlledStop(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"build": {NamedValues: map[string]*v1.Value{"artifact": v1.NewLiteral("web.tar.gz")}},
		},
	})

	finished := make(chan error, 1)
	go func() {
		for _, id := range []string{"build", "deploy"} {
			if err := session.BeforeStep(t.Context(), markStep(id), scope); err != nil {
				finished <- err

				return
			}
		}
		finished <- nil
	}()

	at, err := session.Step(t.Context())
	require.NoError(t, err)
	require.Equal(t, "deploy", at.Step)

	position, paused := session.Paused()
	require.True(t, paused, "the run reported a stop and then reported not being paused")
	assert.Equal(t, "deploy", position.Step)

	text, value, err := session.Evaluate(t.Context(), "steps.build.artifact")
	require.NoError(t, err)
	assert.Equal(t, `"web.tar.gz"`, text)
	assert.NotNil(t, value)

	groups, err := session.Scope()
	require.NoError(t, err)
	assert.Equal(t, []string{"build"}, namesOf(groups, "steps"))

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}
