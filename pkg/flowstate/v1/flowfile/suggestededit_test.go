package flowfile_test

import (
	"errors"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// What a suggested edit promises is not that it is a good idea. It is that a
// program which cannot read English can apply it and be no worse off, and these
// tests are written from that side of the contract: the applier below reads the
// schema message and the file's bytes, and nothing else. It never looks at the
// Go diagnostic, never re-parses the document to find the key, and never uses a
// message to decide anything.
//
// That restriction is the test. An applier allowed to look at the source model
// could quietly repair a range that was slightly wrong, and the repair would
// then be this package's rather than the checker's. What must hold is that the
// range is right on its own.
//
// The negative directions get their own tests, per CLAUDE.md's "Test that A
// cannot reach B": a guard asserted only by a case where it does not fire is a
// guard nothing holds in place.
//
// There is deliberately no test asserting which diagnostics carry edits, in the
// way TestDiagnosticCodesAreAssigned asserts every code is used. Edits are
// sparse on purpose and will stay sparse: most checks cannot name a replacement,
// and a coverage test over them would be a standing invitation to invent one so
// the count goes up. That is the exact pressure that produced two corrupted
// files.

// applyBlind applies a suggested edit to source, reading only the schema message
// and the bytes.
//
// Changes are applied last-first so that each one's offsets are still the
// offsets of the document it was measured against. They never overlap, which the
// schema states, so ordering by start position is enough.
func applyBlind(t *testing.T, source []byte, edit *v1.SuggestedEdit) []byte {
	t.Helper()

	changes := make([]*v1.TextChange, len(edit.GetChanges()))
	copy(changes, edit.GetChanges())
	sort.SliceStable(changes, func(i, j int) bool {
		a, b := changes[i].GetRange(), changes[j].GetRange()
		if a.GetStartLine() != b.GetStartLine() {
			return a.GetStartLine() > b.GetStartLine()
		}
		return a.GetStartColumn() > b.GetStartColumn()
	})

	out := source
	for _, change := range changes {
		r := change.GetRange()
		start := offsetOf(t, out, int(r.GetStartLine()), int(r.GetStartColumn()))
		end := offsetOf(t, out, int(r.GetEndLine()), int(r.GetEndColumn()))
		require.LessOrEqual(t, start, end, "a range that runs backwards")
		out = append(append(append([]byte{}, out[:start]...), change.GetNewText()...), out[end:]...)
	}
	return out
}

// offsetOf converts a 1-based line and a 1-based code point column, which is
// what the schema says a [v1.SourceRange] holds, into a byte offset.
func offsetOf(t *testing.T, source []byte, line, column int) int {
	t.Helper()
	require.Positive(t, line, "a range naming no line cannot be applied")
	require.Positive(t, column, "a range naming no column cannot be applied")

	offset, current := 0, 1
	for current < line {
		next := strings.IndexByte(string(source[offset:]), '\n')
		require.GreaterOrEqual(t, next, 0, "line %d is past the end of the file", line)
		offset += next + 1
		current++
	}
	for range column - 1 {
		require.Less(t, offset, len(source), "column %d is past the end of the file", column)
		_, size := utf8.DecodeRune(source[offset:])
		offset += size
	}
	return offset
}

// problems returns every diagnostic a source produces, as the schema messages a
// machine surface would receive.
//
// A file the compiler refuses reports through the error rather than through the
// slice, and a caller reading `flow validate --output json` cannot tell the two
// apart, so neither does this.
//
// A failure that is not diagnostics at all, which is what a document that no
// longer parses produces, is carried through as one rather than failing here.
// It is a thing a blind applier can cause, so it has to be a thing the report
// can say: a test that stopped at it would report "an error that is not
// diagnostics" where what happened is that applying an edit broke the file.
func problems(t *testing.T, source []byte) []*v1.Diagnostic {
	t.Helper()

	ds, err := flowfile.ValidateSource(source)
	if err != nil {
		var compiled flowfile.Diagnostics
		if errors.As(err, &compiled) {
			ds = append(ds, compiled...)
		} else {
			ds = append(ds, flowfile.Diagnostic{Message: err.Error()})
		}
	}
	return ds.Report("test.yaml").GetDiagnostics()
}

// messages returns the diagnostics' messages, for comparing one report against
// another.
func messages(ds []*v1.Diagnostic) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.GetMessage())
	}
	sort.Strings(out)
	return out
}

// TestSuggestedEditsApplyBlind is the whole promise of the field, checked at the
// depth an agent would experience it: apply the edit knowing nothing but the
// message and the bytes, and the problem is gone.
//
// The second half is the one that matters more. "The diagnostic went away" is
// also true of an edit that deleted the step, so the assertion is that the
// report afterwards is a *subset* of the report before: the named problem is
// gone and nothing took its place.
//
// Every edit a fixture offers is applied, not only the one the case is named
// after, and the last fixture offers none. That is not filler: it is the file
// the duplicate guard exists for, and it is here so that this test, and not
// only the guard's own, notices if the guard stops firing. An edit there writes
// a second `timeout:` into the mapping, and the parser refuses a duplicate
// mapping key outright, so the file that came back would carry a syntax error
// the file that went in did not have.
func TestSuggestedEditsApplyBlind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		// want is the message the edit is expected to resolve, or empty when
		// this file must offer no edit at all.
		want string
	}{
		{
			name: "a step's own key",
			src: `edition: v2026.2
name: blind
steps:
  - id: a
    timeou: 5s
    log:
      message: hi
`,
			want: `unknown key "timeou"`,
		},
		{
			name: "a key of the document itself",
			src: `edition: v2026.2
nam: blind
steps:
  - id: a
    log:
      message: hi
`,
			want: `unknown key "nam"`,
		},
		{
			name: "a key nested inside a step's retry block",
			src: `edition: v2026.2
name: blind
steps:
  - id: a
    retry:
      attempt: 3
    log:
      message: hi
`,
			want: `unknown key "attempt"`,
		},
		{
			name: "a key inside a loop",
			src: `edition: v2026.2
name: blind
vars:
  things: ["a", "b"]
steps:
  - id: a
    for_each:
      item: ${vars.things}
      steps:
        - id: b
          log:
            message: hi
`,
			want: `unknown key "item"`,
		},
		{
			name: "a mapping that already has the suggestion offers nothing",
			src: `edition: v2026.2
name: blind
steps:
  - id: a
    timeou: 5s
    timeout: 10s
    log:
      message: hi
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			before := problems(t, []byte(test.src))
			had := map[string]bool{}
			for _, m := range messages(before) {
				had[m] = true
			}

			applied := 0
			for _, target := range before {
				for _, edit := range target.GetEdits() {
					applied++
					require.NotEmpty(t, edit.GetTitle(), "an unlabeled alternative")

					after := problems(t, applyBlind(t, []byte(test.src), edit))

					// Nothing new appeared. A repair that trades one diagnostic
					// for another is not a repair, and this is the half that
					// catches a range covering one character too many, or a
					// rename into a key the mapping already had.
					for _, m := range messages(after) {
						require.True(t, had[m], "applying %q introduced: %s", edit.GetTitle(), m)
					}

					for _, d := range after {
						require.NotEqual(t, target.GetMessage(), d.GetMessage(),
							"the edit did not resolve the problem it was offered for")
					}

					if test.want != "" {
						require.Contains(t, target.GetMessage(), test.want)
					}
				}
			}

			if test.want == "" {
				require.Zero(t, applied, "this file must offer no edit")
				return
			}
			require.Equal(t, 1, applied, "one repair, offered as the only alternative")
		})
	}
}

// TestSuggestedEditsConvergeToValid walks the loop an agent actually writes:
// validate, apply what came back, validate again, stop when there is nothing
// left to apply.
//
// Two properties, and the second is the one a single-edit test cannot see. That
// the loop *terminates* in the number of rounds the file's mistakes predict, so
// an edit cannot re-report the problem it just fixed; and that what it converges
// on is a file the validator accepts, rather than merely a file with fewer
// diagnostics.
func TestSuggestedEditsConvergeToValid(t *testing.T) {
	t.Parallel()

	// Four misspellings at four depths: the document, a step, a block inside a
	// step, and a loop's own mapping. Depth is the variable because the ranges
	// come from different levels of the parser's recursion, and a level that
	// measured its columns against the wrong node would only show up here.
	const src = `edition: v2026.2
nam: converge
vars:
  things: ["a", "b"]
steps:
  - id: a
    timeou: 5s
    retry:
      attempt: 3
    log:
      message: hi
  - id: b
    for_each:
      item: ${vars.things}
      steps:
        - id: c
          log:
            message: ${item}
`

	// One edit applied per round, which is the honest worst case: an applier
	// that took all of a round's edits at once would finish sooner, and the
	// count would then say nothing about whether each edit stands on its own.
	const wantRounds = 4

	source := []byte(src)
	rounds := 0
	for {
		ds := problems(t, source)

		var edit *v1.SuggestedEdit
		for _, d := range ds {
			if len(d.GetEdits()) > 0 {
				edit = d.GetEdits()[0]
				break
			}
		}
		if edit == nil {
			break
		}

		rounds++
		require.LessOrEqual(t, rounds, wantRounds+1, "the loop is not converging")
		source = applyBlind(t, source, edit)
	}

	require.Equal(t, wantRounds, rounds, "one round per mistake, and no round that changed nothing")

	ds, err := flowfile.ValidateSource(source)
	require.NoError(t, err, "the converged file does not compile")
	require.Empty(t, ds, "the converged file still has problems: %v", ds)
}

// TestNoSuggestedEditThroughAMergeKey is the first negative direction, and the
// reason [field.merged] exists.
//
// A key that arrived through `<<:` is written once in the anchor and read by
// every mapping merging it. Replacing the source the diagnostic names would edit
// the anchor, so an agent repairing what it thought was one step would rewrite
// every step sharing the boilerplate. The diagnostic is still reported at both
// sites, which is correct: both steps do have an unknown key. What is refused is
// the claim that a program can fix it without looking.
func TestNoSuggestedEditThroughAMergeKey(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.2
name: merged
steps:
  - <<: &base
      timeou: 5s
    id: a
    log:
      message: hi
  - <<: *base
    id: b
    log:
      message: hi
`

	ds := problems(t, []byte(src))

	reported := 0
	for _, d := range ds {
		if !strings.Contains(d.GetMessage(), `unknown key "timeou"`) {
			continue
		}
		reported++
		require.Empty(t, d.GetEdits(), "an edit that would rewrite the anchor")
	}
	require.Equal(t, 2, reported, "both steps should still be told about the key")
}

// TestNoSuggestedEditWhenTheSuggestionIsAlreadyWritten is the second negative
// direction, and the one whose failure is silent.
//
// A mapping that already has `timeout:` and also has `timeou:` gets the same
// nearest-match sentence as any other typo, and the sentence is fine: the author
// can see both keys and decide. An *edit* is not, because applying it writes a
// second `timeout:` into the mapping, and YAML resolves a duplicate key by
// keeping one of them. The file would then quietly lose a value it wrote, which
// is worse than the diagnostic it started with, and nothing downstream would
// report it.
func TestNoSuggestedEditWhenTheSuggestionIsAlreadyWritten(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.2
name: already
steps:
  - id: a
    timeou: 5s
    timeout: 10s
    log:
      message: hi
`

	ds := problems(t, []byte(src))

	found := false
	for _, d := range ds {
		if !strings.Contains(d.GetMessage(), `unknown key "timeou"`) {
			continue
		}
		found = true
		require.Contains(t, d.GetMessage(), `did you mean "timeout"?`,
			"the sentence is still offered; only the edit is withheld")
		require.Empty(t, d.GetEdits(), "an edit that would write a duplicate key")
	}
	require.True(t, found, "the unknown key should still be reported")
}

// TestNoSuggestedEditForAQuotedKey is the third guard, which is about the span
// rather than about the file.
//
// The span of a quoted key covers its quotes, because that is what the token's
// source text is. Replacing it with a bare name deletes them, which is harmless
// for `"timeou"` and is not harmless in general: a key that needed quoting to be
// a string at all would stop being one. Rather than reason about which quoting
// styles survive a rename, the edit is offered only where the source text and
// the name are the same string.
func TestNoSuggestedEditForAQuotedKey(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.2
name: quoted
steps:
  - id: a
    "timeou": 5s
    log:
      message: hi
`

	ds := problems(t, []byte(src))

	found := false
	for _, d := range ds {
		if !strings.Contains(d.GetMessage(), `unknown key "timeou"`) {
			continue
		}
		found = true
		require.Empty(t, d.GetEdits(), "an edit whose span covers the quotes")
	}
	require.True(t, found, "the unknown key should still be reported")
}
