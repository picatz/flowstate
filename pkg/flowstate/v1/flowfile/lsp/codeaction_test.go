package lsp

import (
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The property every test here is written around: the action an editor applies has
// to produce the bytes `flow fix` writes, compared as bytes.
//
// Asserting that the result still validates is what let two rewriter defects
// through — see CLAUDE.md, "A rewriter has to know what the grammar binds" — and it
// would be an even weaker check here, since a code action can differ from the
// command by a whole formatting pass and still produce something valid. So each
// test applies the returned [lsp.TextEdit] the way an editor does, splicing it into
// the buffer by range, and compares the resulting string with [flowfile.Fix]'s.

// legacySource is a document written in the retired grammar: an old `edition:`, a
// `task:` block, and a bare step reference. It also carries a comment, which is
// what distinguishes the migration's output from the formatter's.
const legacySource = `# header, written by a human
edition: 2026.1
name: greeter
steps:
  - id: greet
    task:
      name: echo
      inputs:
        message: hello
  - id: show
    log:
      message: "${greet.result}"
`

// fixedSource returns what flowfile.Fix writes for a document, which is the answer
// every action is measured against.
func fixedSource(t *testing.T, src string) string {
	t.Helper()
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, result.Changed(), "this document was chosen because Fix rewrites it")
	return string(result.Source)
}

// actionOfKind returns the single action of a kind, failing when there is not
// exactly one.
func actionOfKind(t *testing.T, actions []codeAction, kind lsp.CodeActionKind) codeAction {
	t.Helper()
	var found []codeAction
	for _, a := range actions {
		if a.Kind == kind {
			found = append(found, a)
		}
	}
	require.Len(t, found, 1, "expected exactly one %s action, got %d", kind, len(found))
	return found[0]
}

// TestCodeActionAppliesTheBytesFixWrites is the feature's reason for existing, and
// the strongest form of it: the edit is applied as an editor would apply it and the
// buffer that results is compared with `flow fix`'s output byte for byte.
func TestCodeActionAppliesTheBytesFixWrites(t *testing.T) {
	t.Parallel()

	const uri = "file:///migrate.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, legacySource)

	actions := c.codeAction(uri, wholeOf(legacySource), nil, nil)
	require.NotEmpty(t, actions, "a document written in the retired grammar draws no migration")

	migrate := actionOfKind(t, actions, codeActionKindSourceFixAll)
	assert.Contains(t, migrate.Title, flowfile.CurrentEdition,
		"the title of a migration should name the edition it migrates to")

	assert.Equal(t, fixedSource(t, legacySource), applyEdit(t, uri, legacySource, migrate.Edit))
}

// TestCodeActionEveryActionCarriesTheSameWholeDocumentEdit pins the honesty
// decision recorded at the top of codeaction.go: [flowfile.FixResult] describes a
// change as a line and a sentence, never as a span, so no action assembles a
// partial edit. A quickfix titled after one line still carries the whole migration,
// and its edit is identical to the fixAll action's.
func TestCodeActionEveryActionCarriesTheSameWholeDocumentEdit(t *testing.T) {
	t.Parallel()

	const uri = "file:///whole.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, legacySource)

	actions := c.codeAction(uri, wholeOf(legacySource), nil, nil)
	require.Greater(t, len(actions), 1, "a whole-file selection should reach the line actions too")

	want := fixedSource(t, legacySource)
	for _, action := range actions {
		edits := action.Edit.Changes[uri]
		require.Len(t, edits, 1, "%q carries more than one edit", action.Title)
		assert.Equal(t, want, applyEdit(t, uri, legacySource, action.Edit),
			"%q produces something other than what `flow fix` writes", action.Title)

		// The range has to reach the end of the buffer, or the replacement is
		// prepended to whatever was left behind.
		assert.Equal(t, 0, edits[0].Range.Start.Line)
		assert.Equal(t, 0, edits[0].Range.Start.Character)
		assert.Equal(t, newLineIndex(legacySource).lineCount()-1, edits[0].Range.End.Line)
	}
}

// TestCodeActionQuickFixIsOfferedWhereTheChangeIs checks the half that makes the
// migration reachable from a squiggle: a cursor on a line the rewriter changes
// draws a quickfix naming that change, and a cursor on a line it does not draws
// none.
func TestCodeActionQuickFixIsOfferedWhereTheChangeIs(t *testing.T) {
	t.Parallel()

	const uri = "file:///quickfix.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, legacySource)

	// The line holding the bare `${greet.result}`, which is what the rooting
	// diagnostic underlines and where an author would open the menu.
	reference := strings.Index(legacySource, "${greet.result}")
	require.Positive(t, reference)
	line := strings.Count(legacySource[:reference], "\n")

	only := []lsp.CodeActionKind{lsp.CAKQuickFix}
	at := c.codeAction(uri, atLine(line), only, nil)
	require.NotEmpty(t, at, "no quickfix on the line the migration rewrites")
	assert.Contains(t, at[0].Title, "rooted",
		"the title should name the local change, not just say `flow fix`")
	assert.Equal(t, fixedSource(t, legacySource), applyEdit(t, uri, legacySource, at[0].Edit))

	// `message: hello` — an input the migration carries through untouched.
	untouched := strings.Count(legacySource[:strings.Index(legacySource, "message: hello")], "\n")
	assert.Empty(t, c.codeAction(uri, atLine(untouched), only, nil),
		"a quickfix was offered on a line the migration does not change")
}

// TestCodeActionQuickFixCarriesTheDiagnosticItAnswers checks the attachment an
// editor uses to put the lightbulb on the squiggle rather than only in a menu.
func TestCodeActionQuickFixCarriesTheDiagnosticItAnswers(t *testing.T) {
	t.Parallel()

	const uri = "file:///attached.yaml"
	c := newClient(t)
	c.initialize()
	published := c.open(uri, legacySource)
	require.NotEmpty(t, published.Diagnostics, "this document should be reported as needing migration")

	// The `edition:` line, which is the one place this document draws both a
	// diagnostic and a change: the diagnostic that says to run `flow fix`, and the
	// rewrite that performs it. An older edition suppresses every other complaint,
	// since they would be describing the wrong grammar.
	edition := strings.Count(legacySource[:strings.Index(legacySource, "edition:")], "\n")
	require.Equal(t, edition, published.Diagnostics[0].Range.Start.Line,
		"this test assumes the edition diagnostic sits on the edition line")

	// The client echoes back what it was published, which is what a real editor
	// sends in the request's context.
	actions := c.codeAction(uri, atLine(edition),
		[]lsp.CodeActionKind{lsp.CAKQuickFix}, published.Diagnostics)
	require.NotEmpty(t, actions, "no quickfix on the line whose diagnostic says to run `flow fix`")

	var attached bool
	for _, d := range actions[0].Diagnostics {
		if d.Range.Start.Line == edition {
			attached = true
		}
	}
	assert.True(t, attached,
		"the quickfix names no diagnostic on its own line, so an editor cannot attach it to one")
}

// TestCodeActionOnACurrentDocumentOffersNothing is the other direction, and the one
// that keeps a fix-on-save binding quiet: a document Fix returns byte-identical
// draws no action at all.
func TestCodeActionOnACurrentDocumentOffersNothing(t *testing.T) {
	t.Parallel()

	src := fixedSource(t, legacySource)
	require.Equal(t, src, fixedSource2(t, src), "the migration is not idempotent, which this test assumes")

	const uri = "file:///current.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, src)

	assert.Empty(t, c.codeAction(uri, wholeOf(src), nil, nil))
}

// fixedSource2 is [fixedSource] without the "something changed" requirement, for
// the document that is already current.
func fixedSource2(t *testing.T, src string) string {
	t.Helper()
	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.False(t, result.Changed(), "this document was chosen because Fix leaves it alone")
	return string(result.Source)
}

// TestCodeActionOnARefusalOffersNothing is the property the whole design rests on:
// where the rewriter refuses to guess, the editor offers nothing rather than a
// guess of its own.
//
// Both refusal shapes are covered — a `task:` in flow style, which has no line
// structure to rewrite, and the binding written through an unresolvable alias that
// [flowfile.Fix] learned to refuse rather than subtract the wrong name for.
func TestCodeActionOnARefusalOffersNothing(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"a task written in flow style": `edition: v2026.2
name: x
steps:
  - id: greet
    task: {name: echo, inputs: {message: hi}}
`,
		"a binding through an unresolvable alias": `edition: v2026.2
name: x
steps:
  - id: loop
    for_each:
      items: "${['a']}"
      as: *nope
      steps:
        - id: inner
          log:
            message: "${loop.said}"
`,
	}

	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// The premise, asserted rather than assumed: Fix has something to say
			// about this document and declines to write anything.
			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.NotEmpty(t, result.Refusals, "this document does not reach a refusal")
			require.False(t, result.Changed(), "this document is not purely a refusal")

			uri := "file:///refused.yaml"
			c := newClient(t)
			c.initialize()
			c.open(uri, src)

			assert.Empty(t, c.codeAction(uri, wholeOf(src), nil, nil),
				"an action was offered for something the rewriter refused to do")
		})
	}
}

// TestCodeActionOnAnUnparsableDocumentOffersNothing covers the case an editor hits
// constantly, since a file is unparsable for most of the time it is being typed.
func TestCodeActionOnAnUnparsableDocumentOffersNothing(t *testing.T) {
	t.Parallel()

	const src = "name: x\n  steps: [\n"

	const uri = "file:///broken.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, src)

	assert.Empty(t, c.codeAction(uri, wholeOf(src), nil, nil))
}

// TestCodeActionOnAnUnopenedDocumentOffersNothing checks the empty list rather than
// a null, which is what lets a client tell "nothing to do" from a failure.
func TestCodeActionOnAnUnopenedDocumentOffersNothing(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	assert.Empty(t, c.codeAction("file:///never-opened.yaml", atLine(0), nil, nil))
}

// TestCodeActionHonorsTheClientsKindFilter checks the filtering a fix-on-save
// binding depends on. A client asking for `source.fixAll` must not be handed the
// quickfixes, or a binding that runs actions on save runs one the user did not ask
// for; and the protocol's rule that a requested kind covers the kinds beneath it
// means `source` has to reach `source.fixAll`.
func TestCodeActionHonorsTheClientsKindFilter(t *testing.T) {
	t.Parallel()

	const uri = "file:///filtered.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, legacySource)

	whole := wholeOf(legacySource)

	fixAll := c.codeAction(uri, whole, []lsp.CodeActionKind{codeActionKindSourceFixAll}, nil)
	require.Len(t, fixAll, 1)
	assert.Equal(t, codeActionKindSourceFixAll, fixAll[0].Kind)

	under := c.codeAction(uri, whole, []lsp.CodeActionKind{lsp.CAKSource}, nil)
	require.Len(t, under, 1, "`source` must reach `source.fixAll`, which is beneath it")
	assert.Equal(t, codeActionKindSourceFixAll, under[0].Kind)

	quick := c.codeAction(uri, whole, []lsp.CodeActionKind{lsp.CAKQuickFix}, nil)
	require.NotEmpty(t, quick)
	for _, a := range quick {
		assert.Equal(t, lsp.CAKQuickFix, a.Kind)
	}

	assert.Empty(t, c.codeAction(uri, whole, []lsp.CodeActionKind{lsp.CAKRefactor}, nil),
		"a kind this server does not offer must draw nothing rather than everything")
}

// TestCodeActionEditIsFixsOutputRatherThanMarshals is the join the two rewriters
// must not be conflated across.
//
// Formatting and migrating both replace the whole document, and both are reachable
// from the same buffer, so a handler that reached for the wrong one would still
// produce a valid file — and would silently throw away every comment in a diff an
// author was about to read as a migration. The two are told apart here by the thing
// that distinguishes them: `flow fix` copies untouched lines through byte for byte,
// and Marshal renders from the parsed model and keeps no comments at all.
func TestCodeActionEditIsFixsOutputRatherThanMarshals(t *testing.T) {
	t.Parallel()

	const uri = "file:///both.yaml"
	c := newClient(t)
	c.initialize()
	c.open(uri, legacySource)

	// The legacy document does not compile — that is what an edition boundary is —
	// so formatting has nothing to render and says so, while the migration is on
	// offer. Each handler answers for its own rewriter.
	assert.Empty(t, c.format(uri),
		"a document in the retired grammar has no workflow for Marshal to render")

	migrated := applyEdit(t, uri, legacySource,
		actionOfKind(t, c.codeAction(uri, wholeOf(legacySource), nil, nil), codeActionKindSourceFixAll).Edit)
	require.Equal(t, fixedSource(t, legacySource), migrated)

	assert.Contains(t, migrated, "# header, written by a human",
		"the migration dropped a comment, which means it went through the formatter")

	// And the two really are distinguishable on this document: what Marshal writes
	// for the migrated file is not what Fix wrote, so the assertion above is not
	// vacuous.
	workflow, err := flowfile.Unmarshal([]byte(migrated))
	require.NoError(t, err, "the migration produced something that does not compile")
	formatted, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	require.NotEqual(t, migrated, string(formatted),
		"Fix and Marshal agree on this document, so it cannot tell the two apart")

	// Once migrated, the roles swap: there is nothing left to migrate, and
	// formatting is the only rewriter with an opinion.
	c.change(uri, migrated, 2)
	assert.Empty(t, c.codeAction(uri, wholeOf(migrated), nil, nil))

	edits := c.format(uri)
	require.Len(t, edits, 1)
	assert.Equal(t, string(formatted), edits[0].NewText)
}
