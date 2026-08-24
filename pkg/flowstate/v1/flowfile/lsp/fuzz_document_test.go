package lsp

import (
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/require"
)

// FuzzLSPDocumentEdits fuzzes the language server's document path: an
// incremental edit applied at a range the client chose, and every position
// request answered over the result.
//
// This is picatz/flowstate#403's item 3. The issue frames it as the frame
// decoder, and reading the code moves it one layer in — which is the point of
// reading before writing. The frames themselves are jsonrpc2's, a dependency
// this repository does not own and whose decoding is the same for every server
// using it; what *is* this repository's is what happens to the bytes after they
// are framed, and there the untrusted values are not the envelope but the
// coordinates inside it:
//
//	text = text[:start] + c.Text + text[end:]   (documentStore.change)
//
// start and end come from [lineIndex.offsetOfPosition] over an
// [lsp.Range] the editor sent, and that splice is a panic for any pair of
// offsets that is out of range or out of order. Both are clamped today —
// offsetOfPosition clamps the line and byteOfUTF16 clamps the column, and change
// swaps a reversed pair — which is a set of guards written deliberately (their
// doc comments say why: "positions can arrive from an editor that has already
// applied an edit the server has not seen yet"). A guard nothing tests is a
// guard the next refactor removes.
//
// Four coordinate systems meet in position.go and no two agree — LSP counts
// UTF-16 code units, the YAML parser counts code points from one, CEL counts
// code points from one with columns from zero, and Go indexes bytes. The file
// says so itself, and says that for ASCII all four coincide, "which is exactly
// why confusing them goes unnoticed". A fuzzer is the instrument for that: it
// reaches non-ASCII, lone surrogates, combining marks and unpaired UTF-8 in
// positions no hand-written case thinks to put them.
//
// # What is driven, and what is not
//
// The store and the analyzers directly, rather than a connection. Everything
// upstream of them — framing, the JSON-RPC envelope, method dispatch — is either
// jsonrpc2's or a switch statement, and [FlowfileServer.Handle] already converts
// a panic in any of it into a JSON-RPC error, so a crash reached through the
// connection would be reported as an error rather than as a crash and this
// target would be blind to exactly the defect it exists to find. Driving the
// analyzers directly is what makes a panic a failure.
//
// The URI is fixed and deliberately not a `file://` one. A file URI sends
// [readCalleeSource] to the filesystem to resolve a `call:` step, and a fuzzer
// that does disk I/O per execution is a fuzzer that does a few hundred
// executions a second instead of a few thousand — and one whose findings depend
// on what happened to be on disk. `call:` resolution is covered by
// callinputs.go's own tests, which control what they read.
//
// The invariant: no panic, no hang, and every request answers. Answers are not
// asserted on — there is no oracle for what a hover over a fuzzed document at a
// fuzzed position should say — beyond the one structural claim below, which is
// the protocol's rather than this package's: a position handed back to the
// client has to be one the client can resolve, so no range may run backwards.
func FuzzLSPDocumentEdits(f *testing.F) {
	for _, seed := range lspFuzzSeeds {
		f.Add(seed.text, seed.edit, seed.startLine, seed.startChar, seed.endLine, seed.endChar)
	}

	const uri = lsp.DocumentURI("untitled:fuzz.yaml")

	f.Fuzz(func(t *testing.T, text, edit string, startLine, startChar, endLine, endChar int) {
		// The document store is per-execution. Sharing one across executions
		// would make each input's result depend on which inputs ran before it,
		// which is how a fuzz finding becomes unreproducible.
		var docs documentStore

		opened := docs.open(uri, 1, text, nil)
		require.NotNil(t, opened, "opening a document returned nothing")

		changed := docs.change(uri, 2, []lsp.TextDocumentContentChangeEvent{{
			Range: &lsp.Range{
				Start: lsp.Position{Line: startLine, Character: startChar},
				End:   lsp.Position{Line: endLine, Character: endChar},
			},
			Text: edit,
		}}, nil)
		require.NotNil(t, changed, "an edit at a newer version was treated as stale")

		// Both ends of the edit, and one position past the end of the document
		// entirely — the last being the case the clamps in position.go were
		// written for and the one an editor produces when it has applied an edit
		// the server has not seen yet.
		positions := []lsp.Position{
			{Line: startLine, Character: startChar},
			{Line: endLine, Character: endChar},
			{Line: changed.index.lineCount(), Character: 0},
		}

		for _, doc := range []*document{opened, changed} {
			// Whole-document answers, which do not depend on a position.
			for _, d := range diagnose(doc) {
				requireForwardRange(t, d.Range, "a published diagnostic")
			}
			for _, e := range formatEdits(doc) {
				requireForwardRange(t, e.Range, "a formatting edit")
			}
			for _, s := range documentSymbols(doc) {
				requireForwardRange(t, s.Location.Range, "a document symbol")
			}

			for _, pos := range positions {
				if hover := hoverAt(doc, pos); hover != nil && hover.Range != nil {
					requireForwardRange(t, *hover.Range, "a hover")
				}
				if completions := completeAt(doc, pos); completions != nil {
					for _, item := range completions.Items {
						if item.TextEdit != nil {
							requireForwardRange(t, item.TextEdit.Range, "a completion edit")
						}
					}
				}
				for _, location := range definitionAt(doc, pos) {
					requireForwardRange(t, location.Range, "a definition")
				}

				params := codeActionParams{
					TextDocument: lsp.TextDocumentIdentifier{URI: uri},
					Range:        lsp.Range{Start: pos, End: pos},
					Context:      codeActionContext{Only: []lsp.CodeActionKind{lsp.CAKQuickFix}},
				}
				for _, action := range codeActions(doc, params) {
					if action.Edit == nil {
						continue
					}
					for _, edits := range action.Edit.Changes {
						for _, e := range edits {
							requireForwardRange(t, e.Range, "a code action edit")
						}
					}
				}
			}
		}
	})
}

// requireForwardRange fails unless r runs forwards.
//
// The one structural claim this target makes about an answer, and it is the
// protocol's rather than this package's: an editor applying a text edit whose
// end precedes its start either refuses it or corrupts the buffer, and a
// diagnostic with a backwards range underlines nothing. It is checkable without
// an oracle for the answer's content, which is what makes it worth asserting on
// a fuzzed document where nothing else is.
func requireForwardRange(t *testing.T, r lsp.Range, what string) {
	t.Helper()
	require.True(t,
		r.Start.Line < r.End.Line || (r.Start.Line == r.End.Line && r.Start.Character <= r.End.Character),
		"%s came back with a range that runs backwards: %+v", what, r)
}

// TestCompletionAtANegativePositionIsApplicable is the named regression for the
// defect [FuzzLSPDocumentEdits] found on its own seed corpus, the first time it
// was run.
//
// A client that sends `character: -1` — not legal LSP, and so not something any
// hand-written case here had thought to send — got a completion item whose edit
// range was `-1:0` to `-1:-1`: a range running backwards, at a line that does not
// exist. [rangeBack] clamps the start it computes and hands the request's own
// position back as the end, so exactly one of the two ends was guarded.
//
// Written out as a test as well as left in the corpus, because a corpus entry
// says "this input once failed" and a test says which property it violated.
func TestCompletionAtANegativePositionIsApplicable(t *testing.T) {
	t.Parallel()

	var docs documentStore
	doc := docs.open("untitled:negative.yaml", 1,
		"edition: v2026.3\nname: n\nsteps:\n- id: a\n  log:\n    message: hi\n", nil)

	for _, pos := range []lsp.Position{
		{Line: -1, Character: -1},
		{Line: -1, Character: 0},
		{Line: 0, Character: -1},
	} {
		completions := completeAt(doc, pos)
		require.NotNil(t, completions, "completion at %+v returned nothing at all", pos)
		for _, item := range completions.Items {
			if item.TextEdit == nil {
				continue
			}
			requireForwardRange(t, item.TextEdit.Range, "a completion edit")
			require.GreaterOrEqual(t, item.TextEdit.Range.Start.Line, 0,
				"a completion edit named a line the protocol cannot express: %+v", item.TextEdit.Range)
			require.GreaterOrEqual(t, item.TextEdit.Range.Start.Character, 0,
				"a completion edit named a character the protocol cannot express: %+v", item.TextEdit.Range)
		}
	}
}

// An lspFuzzSeed is a document and one edit over it.
type lspFuzzSeed struct {
	text                                   string
	edit                                   string
	startLine, startChar, endLine, endChar int
}

// lspFuzzSeeds are the document-and-edit pairs the fuzzer explores outward from.
//
// Seeded with documents that *parse*, because the analyzers this target drives
// mostly return early on one that does not: [codeActions] refuses a document
// with a parse error outright, [formatEdits] draws no edits from one, and
// hover and completion have no positional model to look anything up in. A seed
// set of garbage would fuzz the early returns. The edits are then chosen to take
// a parsing document apart in the ways an editor does — deleting a key, typing
// inside an expression, replacing a whole line — so that the fuzzer starts from
// both sides of the parse boundary and can cross it in either direction.
var lspFuzzSeeds = []lspFuzzSeed{
	// No edit at all, at the very start: the identity case, whose only job is to
	// exercise every analyzer over a document that is entirely well-formed.
	{"edition: v2026.3\nname: hello\nsteps:\n- id: a\n  log:\n    message: hi\n", "", 0, 0, 0, 0},
	// Typing inside a fence, which is where an expression is half-written on
	// every keystroke an author makes and where completion does most of its
	// work.
	{"edition: v2026.3\nname: e\nsteps:\n- id: a\n  log:\n    message: ${steps.}\n", "a", 5, 21, 5, 21},
	// Deleting a whole line, which is how a document stops parsing.
	{"edition: v2026.3\nname: d\nsteps:\n- id: a\n  log:\n    message: hi\n", "", 3, 0, 4, 0},
	// An edit whose range runs backwards, which documentStore.change is
	// documented as swapping rather than refusing.
	{"edition: v2026.3\nname: b\nsteps:\n- id: a\n  log:\n    message: hi\n", "x", 5, 10, 2, 0},
	// An edit past the end of the document in both coordinates, which is what
	// the clamps in position.go exist for.
	{"edition: v2026.3\nname: p\nsteps:\n- id: a\n  log:\n    message: hi\n", "y", 99, 99, 999, 999},
	// Negative coordinates, which are not legal LSP and which an
	// implementation still has to survive being sent.
	{"edition: v2026.3\nname: n\nsteps:\n- id: a\n  log:\n    message: hi\n", "z", -1, -1, -5, -5},
	// Non-ASCII on the line being edited, so that the UTF-16, code point and
	// byte columns genuinely disagree — the confusion position.go's own header
	// says goes unnoticed precisely because ASCII hides it. An emoji is two
	// UTF-16 code units and four bytes; a combining accent is one of each but
	// two code points.
	{"edition: v2026.3\nname: u\nsteps:\n- id: a\n  log:\n    message: 🙂🙂 café\n", "!", 5, 14, 5, 16},
	// A document with an older edition, so the migration code actions have
	// something to offer and the code-action path is not always the empty one.
	{"edition: 2026.1\nname: old\nsteps:\n- id: a\n  echo:\n    message: hi\n- id: b\n  log:\n    message: ${a.result}\n", "x", 8, 0, 8, 0},
	// Control flow, which is where the outline's scope tracking and the loop
	// bindings live — the four names CLAUDE.md records the grammar binding
	// bare, which completion and hover both have to resolve.
	{"edition: v2026.3\nname: c\nsteps:\n- id: loop\n  for_each:\n    items: [1, 2]\n    as: n\n    steps:\n    - id: body\n      log:\n        message: ${n}\n", "n", 10, 20, 10, 21},
	// A `call:` step, which is the one shape whose resolution wants a path this
	// document does not have — so what it exercises is the refusal, on purpose.
	{"edition: v2026.3\nname: k\nsteps:\n- id: a\n  call: ./other.yaml\n  with:\n    x: 1\n", "y", 6, 7, 6, 8},
}
