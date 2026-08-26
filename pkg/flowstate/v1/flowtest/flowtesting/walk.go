package flowtesting

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Walking a case from a Go test.
//
// # Why this exists
//
// [flowtest.RunOptions.Debugger] has always accepted a session, and until now
// the only things that set it were `flow test --debug` and `flow run local
// --debug` — both of which drive it from a terminal. A Go test could not: the
// session's control surface took a *line of text*, and the run parks inside
// the debugger blocked reading one, so there was nobody to type.
//
// [flowdebug.Session.Control] changed that, and this is the rung between it and
// an author. Without it, walking a case from Go means installing a session,
// starting the run on a goroutine of your own, driving it from another, and
// getting the shutdown right — which is a page of concurrency to write before
// the first assertion, and the wrong page is a test that hangs rather than one
// that fails.
//
// This package's rule is that a capability lands when the surface an author
// actually uses can reach it. A stepping seam nobody can step from a `go test`
// is scaffolding, however green the package is.
//
// # What a Walk is not
//
// It is not a second debugger. Every method here is one call into
// [flowdebug.Session] plus the [testing.TB]-shaped verdict this package exists
// to add — the same relationship this package has to [flowtest], and the same
// reason it is a separate package. [Walk.Session] is the escape hatch to the
// full vocabulary, so nothing here needs to grow a method per command.

// WithWalk holds one case at every step boundary and calls drive with a [Walk]
// while that case runs.
//
// The case is named rather than positional because a subtest is addressed by
// name here, and a walk pinned to "the third case" would silently move when
// somebody inserts one. A name no case answers to fails the test before
// anything runs: a walk that quietly never happened is a test asserting nothing
// while reporting green.
//
// drive runs on the walked subtest's own goroutine, and the case runs on a
// helper one. That direction is the whole contract: drive is your code, it will
// contain `require`, and [testing.TB.FailNow] is defined only on the goroutine
// running the test. Assert through [Walk.T], which is that subtest — not the
// `t` of the function that called [RunFile], which belongs to an enclosing
// test.
//
// When drive returns — normally, or because an assertion stopped it — the
// session is closed and the run is waited for. That is the reason this is an
// option rather than a pattern to copy: a walk that ends without letting go
// leaves the case parked forever, and one that ends without waiting leaves a
// run holding the task registry the next case needs.
//
// Only one case is walked. Interactive by nature, and `flow test --debug`
// refuses more than one for the same reason: stepping through "the" run of
// several is a question with no answer.
func WithWalk(caseName string, drive func(*Walk)) Option {
	return func(c *config) {
		c.walkSet = true
		c.walkCase = caseName
		c.walkDrive = drive
	}
}

// Walk drives one paused case and asserts on what it finds.
type Walk struct {
	t       testing.TB
	ctx     context.Context
	session *flowdebug.Session
}

// T is the [testing.TB] of the subtest this case is running as, for assertions
// the methods below do not cover.
//
// It is the one to assert against, and not the `t` of the function that called
// [RunFile]. drive runs on this subtest's own goroutine, so `require`'s
// [testing.TB.FailNow] does what it says here and would be undefined behaviour
// against an enclosing test's — `testing` only supports FailNow from the
// goroutine running that test (Codex, #1123).
func (w *Walk) T() testing.TB { return w.t }

// Session is the whole vocabulary, for anything these methods do not cover:
// breakpoints, the completer, the raw text of an answer, a verb added later.
//
// Exported rather than mirrored, so this type never becomes a second list of
// commands to keep in step with the prompt's.
func (w *Walk) Session() *flowdebug.Session { return w.session }

// Step runs the next step and waits for the run to stop again. ok is false
// once the run has finished, which is the ordinary end of a walk:
//
//	for at, ok := walk.Step(); ok; at, ok = walk.Step() {
//		…
//	}
//
// Assert against [Walk.T], not the `t` of the function that called [RunFile]:
// drive runs on the subtest's own goroutine, and that is the only one
// `require`'s FailNow is defined on.
//
// A run that has ended is not a failure and must not be reported as one — a
// walk cannot know in advance how many stops a case has — so it is the boolean
// rather than an error. Anything else that goes wrong fails the test, because
// it is a defect here or a cancelled test rather than a fact about the run.
func (w *Walk) Step() (flowdebug.Position, bool) {
	w.t.Helper()

	return w.moved(w.session.Step(w.ctx))
}

// Continue resumes the run and waits for the next breakpoint, with ok false
// where there is none. See [Walk.Step] for why the end of a run is a boolean.
func (w *Walk) Continue() (flowdebug.Position, bool) {
	w.t.Helper()

	return w.moved(w.session.Continue(w.ctx))
}

// Until runs to a named step and waits for the run to stop there.
func (w *Walk) Until(step string) (flowdebug.Position, bool) {
	w.t.Helper()

	return w.moved(w.session.Until(w.ctx, step))
}

// moved turns one movement's answer into a verdict: the end of a run is a
// boolean, and everything else is this package's or the caller's mistake.
func (w *Walk) moved(at flowdebug.Position, err error) (flowdebug.Position, bool) {
	w.t.Helper()

	switch {
	case err == nil:
		return at, true

	case isRunOver(err):
		return flowdebug.Position{}, false

	default:
		w.t.Fatalf("flowtesting: moving the run failed: %v", err)

		return flowdebug.Position{}, false
	}
}

// Value evaluates one CEL expression against the scope the run is paused in and
// hands back the plain Go value, which is what an assertion wants.
//
// Nil where the session is withholding the value's shape — see
// [flowdebug.Session.Evaluate], which is also where to go for the rendered text
// and the typed form.
//
// An expression that does not compile fails the test. That is the opposite of
// the prompt's rule, and deliberately: at a prompt a person is *asking*, and
// some questions will not parse; in a test the expression is part of what is
// being asserted, and one that cannot be evaluated has not checked anything.
func (w *Walk) Value(expression string) any {
	w.t.Helper()

	_, value, err := w.session.Evaluate(w.ctx, expression)
	if err != nil {
		w.t.Fatalf("flowtesting: evaluating %q against the paused run failed: %v", expression, err)

		return nil
	}
	if value == nil {
		return nil
	}

	return value.Value()
}

// Names lists what the paused run can reach, grouped as `scope` groups it.
func (w *Walk) Names() []flowdebug.Names {
	w.t.Helper()

	groups, err := w.session.Scope()
	if err != nil {
		w.t.Fatalf("flowtesting: listing the paused run's scope failed: %v", err)

		return nil
	}

	return groups
}

// isRunOver reports the one error a walk treats as an answer rather than a
// failure.
func isRunOver(err error) bool {
	return err != nil && strings.Contains(err.Error(), flowdebug.ErrRunOver.Error())
}

// ranCase is what the helper goroutine reports back: the case's result, or the
// panic it met on a goroutine that had nobody to report it to.
type ranCase struct {
	result   flowtest.RunResult
	panicked bool
	value    any
	stack    []byte
}

// walked runs one case under a session drive can move, and returns the case's
// result exactly as an unwalked run would.
//
// The ordering is the whole of it. drive starts first and immediately parks on
// its first command, because there is no run yet to take it; the run then
// starts and stops at its first boundary, where that command is waiting. When
// drive returns the session is closed, which is what releases the run — a
// session that is merely abandoned holds the case forever.
func walked(t testing.TB, cfg config, run func(v1.Debugger) flowtest.RunResult) flowtest.RunResult {
	t.Helper()

	var (
		mu    sync.Mutex
		lines strings.Builder
	)

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		// Collected rather than logged as it arrives: the session writes from
		// the run's goroutine, and a [testing.TB] must not be written to from
		// one this function has not joined. Logged below, once both are done,
		// on the same channel the case's transcript uses.
		Emit: func(text string, _ flowdebug.Tone) {
			mu.Lock()
			defer mu.Unlock()

			lines.WriteString(text)
		},
	})
	if err != nil {
		t.Fatalf("flowtesting: building the debug session failed: %v", err)

		return flowtest.RunResult{}
	}

	// The *run* goes to the helper goroutine and drive stays on this one, which
	// is the subtest's. That direction is not a preference: drive is the
	// caller's code, and the caller writes `require`, whose [testing.TB.FailNow]
	// is only defined on the goroutine running the test — from anywhere else it
	// stops that goroutine and the failure is recorded somewhere nobody reads.
	// A panic is worse: outside the runner's own recovery it takes the whole
	// test binary down instead of failing this case (Codex, #1123).
	//
	// `flowtest.Run` is safe to move because it touches no [testing.TB]; it
	// takes a context and a file and reports what happened.
	ran := make(chan ranCase, 1)
	go func() {
		// A panic in here — a stub's own callback, anything `flowtest.Run`
		// reaches — is on a goroutine `testing` does not wrap, where it takes
		// the whole test binary down instead of failing this case. It is the
		// exact hazard that moving drive *off* a helper goroutine removed,
		// arriving on the half that moved onto one, and the answer is to carry
		// it back rather than to move the run again (Codex, #1123).
		defer func() {
			if value := recover(); value != nil {
				_ = session.Close()
				ran <- ranCase{panicked: true, value: value, stack: debug.Stack()}
			}
		}()

		result := run(session)

		// The run is over, and this package cannot learn that any other way:
		// [v1.Debugger] is called before each step and [v1.RunObserver] after
		// each one, so a session whose run completed is indistinguishable from
		// one whose next step has not arrived. Closing here is what tells a
		// walk still waiting for a stop that none is coming — without it, a
		// loop that steps to exhaustion waits forever on a run that finished,
		// which is the same hang this option exists to prevent wearing the
		// other hat.
		_ = session.Close()

		ran <- ranCase{result: result}
	}()

	// Whatever happens in drive, the run is let go *and waited for*, and both
	// halves are deferred because of how a driver most often ends: `require`
	// calls [testing.TB.FailNow], which is [runtime.Goexit], so the statements
	// after this call are never reached. Deferred work still runs.
	//
	// Closing releases the run — it covers the case that never reaches a step
	// boundary at all, a workflow that does not load, where nothing would take
	// the walk's first command and the walk would park and hang the test
	// instead of reporting the case's own diagnostic. Every waiting movement
	// ends with [flowdebug.ErrRunOver].
	//
	// Joining is what keeps a failed subtest from leaving a run behind. Without
	// it, a driver that fails an assertion exits this goroutine while
	// `flowtest.Run` carries on in the other one — holding
	// [v1.LockDefaultRegistry], which the *next* case needs — so a later case
	// blocks or overlaps with a run whose subtest has already reported
	// (Codex, #1123). Close first, then join: the close is what lets the run
	// reach its end.
	var result flowtest.RunResult

	func() {
		defer func() {
			_ = session.Close()
			finished := <-ran

			// Logged here too, so a walk that ended by failing still shows what
			// the session printed — which is exactly when a reader wants it.
			mu.Lock()
			printed := lines.String()
			mu.Unlock()

			if printed != "" {
				t.Log("walk:\n" + printed)
			}

			// Raised here, on the subtest's own goroutine, where `testing`
			// recovers it into this case's failure. The original stack travels
			// with it because the one this panic would otherwise carry is this
			// defer's, which says nothing about where the run broke.
			if finished.panicked {
				panic(fmt.Sprintf("flowtesting: the walked case panicked: %v\n\n%s",
					finished.value, finished.stack))
			}

			result = finished.result
		}()

		cfg.walkDrive(&Walk{t: t, ctx: t.Context(), session: session})
	}()

	return result
}

// walkedCase reports whether this case is the one a walk was asked for.
func (c config) walkedCase(name string) bool {
	return c.walkDrive != nil && c.walkCase == name
}

// refuseUnknownWalk fails before anything runs when the named case is not in
// the file.
//
// A walk that silently never happened is the worst outcome available here: the
// suite still passes, the subtest still reports green, and the assertions the
// author wrote were never reached. Named separately from [refusal] because that
// one is about a file being addressable at all, and this is about an argument
// the caller got wrong.
func refuseUnknownWalk(t testing.TB, file *flowtest.File, cfg config) {
	t.Helper()

	if !cfg.walkSet {
		return
	}

	// A nil function is not "no walk asked for". Treating it as one accepts any
	// case name, runs no assertions, and reports green — the silent no-op the
	// name check below exists to prevent, arriving through the argument instead
	// of the name (Codex, #1123).
	if cfg.walkDrive == nil {
		t.Fatalf("flowtesting: WithWalk was given a nil function, so the walk would run no " +
			"assertions and the suite would still report green; pass the driver, or drop the option")

		return
	}

	// The same conflict `flow test --debug` refuses, for the same reason it
	// gives: a seeded exploration runs each case many times under different
	// schedules, and stepping through "the" run of a case that is about to be
	// run many times is a question with no answer. Here it is worse than
	// unanswerable — one session spans every execution, so a walk stepping to
	// exhaustion would run off the end of the baseline into the first step of
	// the next seed and report the lot as one continuous walk.
	if cfg.budget.Schedules > 0 || cfg.budget.Pinned != nil {
		t.Fatalf("flowtesting: WithWalk steps through one run and WithSchedules runs each case " +
			"many times under different schedules; the walk would step out of one execution " +
			"into the next. Drop one of them")

		return
	}

	names := make([]string, 0, len(file.Tests))
	for _, test := range file.Tests {
		if test.Name == cfg.walkCase {
			return
		}
		names = append(names, fmt.Sprintf("%q", test.Name))
	}

	t.Fatalf("flowtesting: WithWalk names case %q, which this suite does not have; its cases are %s",
		cfg.walkCase, strings.Join(names, ", "))
}
