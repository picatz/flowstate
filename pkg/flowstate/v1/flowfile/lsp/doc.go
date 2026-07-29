// Package lsp implements a Language Server Protocol server for the Flowfile DSL.
//
// The server exists because the only other way to check a Flowfile is to run it,
// and running a workload has side effects. Reporting a mistake while the author
// is still typing is the one point at which it costs nothing.
//
// # Everything is derived
//
// Nothing here maintains its own idea of what a Flowfile may contain. Task
// names, input names, input and output types, required-ness, and the value
// constraints shown on hover all come from the task registry's TaskDef
// descriptors; the language profile comes from the evaluator. That is
// deliberate: a language server holding its own copy of the schema eventually
// offers completion for something the engine rejects, which is worse than
// offering nothing. Registering a new task makes it appear in completion,
// hover, and diagnostics with no change to this package.
//
// # False diagnostics are worse than missing ones
//
// A squiggle under correct code teaches an author to ignore squiggles. Where a
// check cannot be made soundly, this package stays silent. In particular,
// expressions are parsed but never type-checked: what a step's outputs will
// contain is not statically known for every task, so a type error reported
// against a correct expression would be indistinguishable from a real one.
//
// # Layout
//
//   - position.go   converts between the four coordinate systems in play.
//   - store.go      holds open documents; each is an immutable snapshot.
//   - parse.go      builds the positional model of a Flowfile from its YAML AST.
//   - outline.go    a line-based scan that also works on unparseable text.
//   - schema.go     renders protobuf descriptors and protovalidate constraints.
//   - diagnostics.go, hover.go, completion.go, symbols.go — one feature each.
//   - server.go     JSON-RPC dispatch and the protocol lifecycle.
package lsp
