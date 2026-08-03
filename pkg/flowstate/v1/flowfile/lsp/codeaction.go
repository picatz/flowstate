package lsp

import (
	"fmt"

	"github.com/sourcegraph/go-lsp"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Code actions are the editor half of a decision recorded in [flowfile.Fix]:
// surface syntax gets no deprecation window, and what makes that affordable is
// that the migration is a program someone runs in a second. On the command line
// that program is `flow fix`. In an editor it has to be something an author can
// reach from the diagnostic telling them to run it, which is this.
//
// The action runs [flowfile.Fix] over the document's text and hands back what it
// wrote. Nothing here rewrites anything itself, so the editor and the command
// cannot disagree about what a migration produces — the same property the
// formatting handler holds against `flow fmt`.
//
// # Why every action carries the whole document
//
// [flowfile.FixResult] reports a change as a line number and a sentence. It does
// not report the range the edit covered, and it cannot be asked to: a single
// change may replace a run of lines with a different number of lines (a `task:`
// block becoming the task's own key), delete lines, or insert one above another
// (the stamped `edition:`), and several rounds of rewriting compose on top of each
// other before the result is returned. Reconstructing a per-change span from a
// line number and the source would mean guessing where each edit began and ended.
//
// A rewriter guessing at spans is exactly how `flow fix` corrupted valid files
// twice — see CLAUDE.md, "A rewriter has to know what the grammar binds" — and an
// editor assembling partial edits out of an API that does not describe them would
// be making that same mistake one layer up, with the author's buffer as the thing
// at stake. So every action this file offers carries one full-document
// [lsp.TextEdit] holding [flowfile.FixResult.Source] verbatim, and the titles say
// so. A quickfix offered on a line is a way of *reaching* the migration from where
// the problem is; it is not a claim that only that line changes.
//
// # Refusals draw nothing
//
// Where Fix refuses — a `task:` in flow style, a binding written through an alias
// it cannot resolve — it leaves that region alone and says where. No action is
// derived from a refusal, because the only edit this file could attach to one is
// the guess Fix declined to make. A document whose sole problem is a refusal
// produces no changed bytes and therefore no action at all.
//
// Refusals are deliberately not published as diagnostics either. A refusal
// describes what the migration could not do, and most of them are reachable on a
// file that is already current and entirely correct — telling an author their file
// is wrong because a rewriter they did not run would have had to guess is a false
// diagnostic, which is worse than a missing one.

// codeActionKindSourceFixAll is `source.fixAll`, the kind editors bind to
// fix-on-save and to an explicit "fix all" command.
//
// Spelled here because the vendored go-lsp predates it: its [lsp.CodeActionKind]
// constants stop at `source.organizeImports`. The value is the protocol's, not
// this server's.
const codeActionKindSourceFixAll lsp.CodeActionKind = "source.fixAll"

// maxQuickFixes bounds how many line-level actions one request answers with.
//
// The count is chosen by the document, and a document is text an outside party
// wrote: a whole-file selection over a file with four hundred migratable steps
// would otherwise build four hundred actions, each carrying a full copy of the
// rewritten document. The migration is reachable from the `source.fixAll` action
// regardless, so the bound costs an author nothing but a longer menu.
const maxQuickFixes = 16

// A codeAction is one entry in the lightbulb menu.
//
// Declared here because go-lsp models the request and not the response: it has
// [lsp.CodeActionParams] and no code action type, from the era when the only legal
// answer was a [lsp.Command]. The fields are the protocol's.
type codeAction struct {
	Title string `json:"title"`

	Kind lsp.CodeActionKind `json:"kind,omitempty"`

	// Diagnostics are the problems this action addresses, which is what lets an
	// editor attach the lightbulb to a squiggle rather than only to a menu.
	Diagnostics []lsp.Diagnostic `json:"diagnostics,omitempty"`

	Edit *lsp.WorkspaceEdit `json:"edit,omitempty"`
}

// codeActionParams is [lsp.CodeActionParams] plus the one field the vendored
// go-lsp's context is missing.
type codeActionParams struct {
	TextDocument lsp.TextDocumentIdentifier `json:"textDocument"`
	Range        lsp.Range                  `json:"range"`
	Context      codeActionContext          `json:"context"`
}

// codeActionContext carries the client's filter alongside the diagnostics.
//
// `only` is how an editor asks for one kind of action — it is what fix-on-save
// sends, naming `source.fixAll` — and answering a filtered request with actions of
// other kinds is how a "fix all on save" binding ends up running something the user
// did not ask for.
type codeActionContext struct {
	Diagnostics []lsp.Diagnostic     `json:"diagnostics"`
	Only        []lsp.CodeActionKind `json:"only,omitempty"`
}

// codeActions returns the migrations offered for a document over a range.
//
// Nil when there is nothing to offer: a document that does not parse, one Fix
// leaves byte-identical, or one whose only findings are refusals.
func codeActions(doc *document, params codeActionParams) []codeAction {
	if doc.tooLarge || doc.parseErr != nil {
		// A document the server did not analyze, or one that is not YAML at all.
		// Fix would refuse the second outright, and offering an action computed
		// from text nobody could parse is the failure mode formatting avoids for
		// the same reason.
		return nil
	}

	result, err := flowfile.Fix([]byte(doc.text))
	if err != nil || !result.Changed() {
		return nil
	}
	// Belt and braces against a rewriter that records a change and writes the same
	// bytes: an edit an editor applies has to actually be an edit, or an author gets
	// a dirty buffer and an undo entry for nothing.
	if string(result.Source) == doc.text {
		return nil
	}

	edit := &lsp.WorkspaceEdit{
		Changes: map[string][]lsp.TextEdit{
			string(doc.uri): {{
				Range:   wholeDocumentRange(doc),
				NewText: string(result.Source),
			}},
		},
	}

	var actions []codeAction
	if wants(params.Context.Only, codeActionKindSourceFixAll) {
		actions = append(actions, codeAction{
			Title: fmt.Sprintf("Migrate to edition %s (%s, rewrites the whole file)",
				flowfile.CurrentEdition, plural(len(result.Changes), "change")),
			Kind: codeActionKindSourceFixAll,
			Edit: edit,
		})
	}
	if wants(params.Context.Only, lsp.CAKQuickFix) {
		actions = append(actions, quickFixes(doc, params, result, edit)...)
	}
	return actions
}

// quickFixes returns one action per change the request's range covers.
//
// The point of these is reachability: an author sees a diagnostic saying to run
// `flow fix` and asks the editor what it can do about *this line*. Each carries the
// same whole-document edit as the `source.fixAll` action — see the note at the top
// of this file about why a partial one cannot be built honestly — with a title
// naming the change that sits under the cursor, so the menu says what prompted it
// without claiming to be narrower than it is.
func quickFixes(doc *document, params codeActionParams, result flowfile.FixResult, edit *lsp.WorkspaceEdit) []codeAction {
	var (
		actions []codeAction
		seen    = map[string]bool{}
	)
	for _, change := range result.Changes {
		// Fix counts lines from one and the protocol counts from zero.
		//
		// The line is where the rewriter made the change in the document as it
		// stood when it made it, and Fix runs to a fixed point — so for a file
		// needing several rounds, a later round's line is a position in an
		// intermediate document rather than in the author's buffer. That is why
		// this decides only *whether to offer* a menu entry and never what the edit
		// covers: an entry offered a line or two off is a menu that is slightly
		// wrong, and an edit computed from the same number would be a rewrite that
		// is silently wrong. A line past the end of the buffer is dropped rather
		// than clamped, for the same reason.
		line := change.Line - 1
		if line < 0 || line >= doc.index.lineCount() {
			continue
		}
		if line < params.Range.Start.Line || line > params.Range.End.Line {
			continue
		}
		// Two steps migrated the same way produce the same sentence, and two
		// identical entries in a menu is a menu that looks broken.
		if seen[change.Message] {
			continue
		}
		seen[change.Message] = true

		actions = append(actions, codeAction{
			Title:       fmt.Sprintf("%s — applies the whole migration", change.Message),
			Kind:        lsp.CAKQuickFix,
			Diagnostics: diagnosticsOnLine(params.Context.Diagnostics, line),
			Edit:        edit,
		})
		if len(actions) == maxQuickFixes {
			break
		}
	}
	return actions
}

// diagnosticsOnLine returns the diagnostics the client sent whose range covers a
// line, so an action can say which problem it answers.
func diagnosticsOnLine(diagnostics []lsp.Diagnostic, line int) []lsp.Diagnostic {
	var out []lsp.Diagnostic
	for _, d := range diagnostics {
		if d.Range.Start.Line <= line && line <= d.Range.End.Line {
			out = append(out, d)
		}
	}
	return out
}

// wants reports whether a client asking for `only` these kinds wants one.
//
// An empty filter wants everything, which is what a client sends when a user opens
// the lightbulb menu. Otherwise a requested kind matches a kind that is it or is
// under it — `source` asks for `source.fixAll` — which is the protocol's rule and
// the reason `only: ["source"]` from a fix-on-save binding is not silently unmet.
func wants(only []lsp.CodeActionKind, kind lsp.CodeActionKind) bool {
	if len(only) == 0 {
		return true
	}
	for _, want := range only {
		if kind == want {
			return true
		}
		if len(kind) > len(want) && kind[:len(want)] == want && kind[len(want)] == '.' {
			return true
		}
	}
	return false
}

// plural renders a count with its noun, because "1 changes" in a menu title reads
// as a bug in the tool offering to rewrite your file.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
