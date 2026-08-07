// Package embed is the curated surface for embedding Flowstate as a Go
// library: compile a Flowfile from bytes, run it locally in-process or
// durably against a Temporal worker the embedding program owns, and register
// the program's own Go functions as tasks a workflow can call.
//
// # This package is the stable one, v1 is not
//
// [pkg/flowstate/v1] is the interpreter and the generated schema types "v1"
// names the edition of, not a Go compatibility promise — see its own doc.
// This package is: a deliberately small, curated surface built for an
// embedder to hold onto across an upgrade, reaching into v1 on that
// embedder's behalf so it does not have to import v1 directly. Prefer it,
// even where v1 could do the same thing more directly — that directness is
// exactly what breaks silently on the next interpreter refactor.
//
// # The four things an embedder does
//
//  1. Compile a Flowfile from bytes with [Compile], the same compile
//     boundary `flow validate` uses.
//  2. Register any Go functions the embedding program wants to expose as
//     tasks with a [Tasks] set, and [Tasks.Install] it before compiling a
//     Flowfile that names one — see [Tasks]'s doc for why installing and
//     running read two different registries on purpose.
//  3. Run the compiled workflow with [RunLocal], in-process, for a
//     one-off job or a rehearsal of what a durable run will do.
//  4. Or run it durably with [RunDurable], which registers the interpreter
//     on a Temporal worker the embedding program owns and operates — this
//     package never opens a Temporal connection itself.
//
// [RunOptions] bundles what running a workflow well takes: bound inputs, the
// custom tasks and worker-side secret authority available to it, an egress
// policy for the http task, and — for [RunLocal] — a clock and a way to
// deliver signals. Its zero value is fail closed throughout: see
// [RunOptions]'s own doc for exactly what "the same posture as an
// unconfigured `flow run local`" means field by field.
//
// # What this package deliberately does not curate
//
// Everything here compiles a single file's worth of Flowfile with no `call:`
// support (compiling from bytes has no directory to resolve one against —
// see [Compile]), and does not expose Flowstate's server, its schedules, or
// its plugin-process hosting model. Those are real capabilities of the
// system this package embeds, and each is its own curation problem for a
// later slice rather than something bolted onto this one.
//
// [pkg/flowstate/v1]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1
package embed
