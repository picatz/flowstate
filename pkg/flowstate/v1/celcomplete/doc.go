// Package celcomplete answers one question for every surface that asks it:
// given what an expression may name at this point, and the partial name someone
// has typed, what can they write next?
//
// It is the editor's completer with the editor taken out. Every rule here —
// bindings are bare and steps are rooted, a root is offered with the dot that
// continues it, `steps.<id>.<output>` has three levels and nothing has four,
// functions come last because there are sixty of them and two bindings — was
// written for `flow lsp` and lived in [flowfile/lsp]'s completion.go as
// `refScope`, `refCandidate` and `completeInExpression`. What made it worth
// moving is that the *scope* those rules run over need not come from a
// document: the debugger has a better one, because a paused run knows which
// outputs actually exist rather than which a task declares.
//
// So this package holds the rules and knows nothing about where a scope came
// from. `lsp` builds a [Scope] from a parsed Flowfile and renders the answer as
// LSP completion items; `flowdebug` builds one from the run's own [v1.Scope]
// and renders it at a terminal. Two callers, one idea of what a name may be —
// which is the point, because an editor and a debugger disagreeing about what
// is in scope would be two answers to a question the language has one answer
// to.
//
// # A candidate is a name, never a value
//
// Nothing here reads a value, and [Candidate] has no field one could be put in.
// That is deliberate rather than incidental: the debugger completes over a live
// run, where a scope holds real data, and a completion popup that previewed
// values would be a disclosure channel around every redaction seam its caller
// installed. Names come from the file; values come from the run. This package
// offers the first kind.
//
// # Bounded, because a scope is not a fixed size
//
// A completion request is untrusted input like any other (CLAUDE.md): the
// number of candidates grows with the document or the run, not with what was
// typed, and both are chosen by somebody else. [MaxCandidates] bounds the
// answer, and [Result.Truncated] says when it was reached — an editor renders
// that as an incomplete list and a terminal says how many it did not print,
// because a silently short list is one nobody can tell from a complete one.
package celcomplete
