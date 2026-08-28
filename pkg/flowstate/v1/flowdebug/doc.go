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
// # What crosses a wire, and what refuses to
//
// The schema describes the answers, not the session (`debug.proto`, #928's
// durable-debug arc, stage 1). [Session.PositionProto], [Session.ScopeProto],
// [Session.StepWindowProto] and [Session.SessionProto] build those messages
// from a live session, and `wire.go` states why that is a bridge rather than a
// replacement of the types here.
//
// The rest of a session is a type defined by a boundary it refuses to cross,
// which is CLAUDE.md's proto-first exception, and it is written down here so
// that nobody "fixes" one of these into the proto:
//
//   - [Session] itself. It holds an open reader and writer, a [Console], a
//     clock and a run's own goroutine parked at a step boundary. A live
//     resource is not a value, and a serialized one would name a pause that
//     the process holding it had already left.
//   - The redactors [Session.SetRedactor] and [Session.SetValueRedactor]
//     install. They are closures over secret material, held that way precisely
//     because reflection cannot reach a captured variable — a schema type for
//     one would be a field holding the thing it exists to withhold.
//   - The [ref.Val] [Session.Evaluate] returns beside the rendered answer. A
//     caller in this process needs a value's shape to decide whether a
//     variable expands; a wire message carrying it would put the structured
//     half of an inspection on a new surface, which is the hole the redaction
//     seam closes. `DebugBinding` carries text, and only text.
//   - [Tone]. It classifies a fragment of *output* so a terminal can colour it,
//     and streaming a session's output is stage 3's question rather than this
//     one's; a vocabulary landed before the surface that reads it would be
//     guessed from the domain.
//
// # Local driver only
//
// [v1.Debugger] is a local-driver seam, like [v1.Scheduler] and
// [v1.RunObserver] before it. Pausing a durable run is slice 2 of #928 and a
// different mechanism for stated reasons; nothing here is it — which is also
// why [Session.SessionProto] reports a local session and nothing else.
package flowdebug
