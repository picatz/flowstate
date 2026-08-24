// Package flowdebug is the interactive step debugger for a local run
// (issue #928, slice 1).
//
// A [Session] implements [v1.Debugger], so it is asked at each step boundary
// whether the run may proceed, and [v1.RunObserver], so it can say what each
// step actually produced. Those two seams are the whole of its contact with
// the engine: it drives the real local driver rather than interpreting a
// workflow itself, which is #928's binding constraint — a debugger that forked
// the interpreter would re-create the two-drivers problem inside one driver.
//
// # Commands are a stream, which is what makes a session replayable
//
// A session reads commands from an [io.Reader] and writes to an [io.Writer].
// A terminal is one such pair; a string of newline-separated commands is
// another. That is not a testing convenience, it is the record-and-replay
// design the owner greenlit on #928: the session records every command it
// accepted, [Session.Script] hands that back, and feeding it to a new session
// reproduces the run decision for decision. A debugging session is therefore a
// test artifact rather than an ephemeral one, which is the same discipline DST
// states as "every failure is a seed" (#477).
//
// # What a session may see
//
// Everything the run's own scope holds, and nothing more, because `inspect`
// evaluates through [v1.Scope.Activation] — the run's own activation, the run's
// own evaluator, and therefore the run's own cost bound ([v1.DefaultCostLimit]).
// Two consequences worth stating, both inherited rather than enforced here:
//
//   - A `${secret(...)}` reference cannot be resolved by an inspection,
//     because it cannot be resolved by an activation at all: resolving one
//     "would produce a value in workflow code, and anything a workflow computes
//     can end up in history" (eval.go, StepsOutputActivation.resolveValue).
//     The debugger did not have to refuse it; there is nothing there to read.
//   - An expression an author types is bounded exactly as an expression they
//     write in a file is. A debugger prompt is an untrusted-input surface like
//     any other (CLAUDE.md), and it is bounded by reusing the bound rather than
//     by inventing a second one.
//
// # Local driver only
//
// [v1.Debugger] is a local-driver seam, like [v1.Scheduler] and
// [v1.RunObserver] before it. Pausing a durable run is slice 2 of #928 and a
// different mechanism for stated reasons; nothing here is it.
package flowdebug
