package flowstatev1

import "context"

// What a task is doing while it is doing it.
//
// A step that takes a minute is opaque between its start and its end. `flow watch`
// can say which step a run is on and what a retrying activity last failed with
// (#131), and neither answers "it has been on `upload` for four minutes — is it
// working?". This is the channel for that answer: a task says which phase it has
// reached, and whichever driver is running it decides where that goes.
//
// # A Go type rather than a schema one
//
// [Phase] never travels as data. What travels is whatever a driver chooses to
// write — the durable driver writes the phase's name into an activity heartbeat —
// and the value itself exists only for the length of one call. Per CLAUDE.md's
// exception, that is said here so nobody moves it into the proto: a schema
// describes things that cross a boundary, and this one is defined by refusing to.
//
// # Why a phase cannot be built from a string
//
// [Phase] wraps an unexported field and there is no constructor. Every phase is
// one of the package-level values below, and no caller anywhere can make another.
//
// That is not fastidiousness, it is invariant 7. The durable driver puts this into
// an activity heartbeat, and heartbeat details are written into workflow history —
// which is durable and broadly readable, and is exactly where a secret must never
// go. A `func ReportProgress(ctx, string)` would be an open channel from inside a
// task, holding resolved inputs and a bearer token, straight into history; the
// mistake would look like `ReportProgress(ctx, "requesting "+url)` and would read
// as helpful.
//
// So the type makes it impossible rather than discouraged. It is the same
// discipline the secret scrubber uses when it holds material in a closure:
// containment by construction beats containment by review, because review is a
// thing somebody has to remember to do.
type Phase struct {
	// Unexported and unreachable: a value of this type can only be one of the
	// package-level phases below, so what reaches history is a constant chosen by
	// this file and never a value derived from a task's inputs.
	name string
}

// String returns the phase's name, which is what a driver records.
func (p Phase) String() string { return p.name }

// The phases a task can report. Adding one is deliberately a change to this file,
// reviewed here, where the reason the vocabulary is closed is written down.
var (
	// PhaseRequesting is set once a request has been built and authorized and is
	// about to leave the worker. A step sitting here is waiting on somebody else.
	PhaseRequesting = Phase{"requesting"}

	// PhaseReadingResponse is set once a response's headers have arrived and its
	// body is being read. Distinct from the above because they fail differently
	// and because the difference is the whole diagnosis: a step stuck requesting
	// is waiting for a peer that has said nothing, and a step stuck reading has a
	// peer that answered and then stopped talking.
	PhaseReadingResponse = Phase{"reading the response"}

	// PhaseCallingPlugin is set around a call into a plugin process, which is the
	// other place a step spends real time and the one whose duration is decided by
	// code this repository did not write.
	PhaseCallingPlugin = Phase{"calling the plugin"}
)

// progressKey carries the reporter a driver installed.
type progressKey struct{}

// ContextWithProgress installs the function a task's phase reports go to.
//
// A context value rather than a parameter, for the reason the logger is one: a
// task's signature is the execution-independent contract both drivers implement
// against, and threading a driver's reporting mechanism through it would put a
// durable-execution concept into the shape of every task ever written.
func ContextWithProgress(ctx context.Context, report func(Phase)) context.Context {
	if report == nil {
		return ctx
	}

	return context.WithValue(ctx, progressKey{}, report)
}

// ReportProgress records that a task has reached a phase.
//
// A no-op where no driver installed a reporter, which is the local driver and
// every test — so a task can say where it is unconditionally, and nothing about
// calling it needs to know which driver is running.
//
// Deliberately without an error return and without a report of whether anything
// was listening. This is an aside about work in progress; a task that could fail
// because nobody was watching it would be a worse task.
func ReportProgress(ctx context.Context, phase Phase) {
	report, ok := ctx.Value(progressKey{}).(func(Phase))
	if !ok {
		return
	}

	report(phase)
}
