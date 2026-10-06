package flowdebug

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The autopsy answers a smaller vocabulary than the prompt, because the run is
// over. It used to keep that set as a second list beside `commands` — a switch,
// a completer set and a help text, written separately — and the second list
// charged this repository's most-paid-for price: `complete` was added to the
// table and to the main dispatch, and at the autopsy came back "unknown
// command" (Codex, #1117), at the prompt where completion is worth most, since
// the bindings a failed case was judged under live only there.
//
// The table now says it: a verb's `fronts` carries [frontAutopsy] if the autopsy
// answers it, the completer and `help` read that column, and a verb on the prompt
// that moves the run leaves the autopsy, which is what a person typing
// `continue` there means. What is left to check is the switch itself, the one
// place a case label can still be written by hand.

// TestTheAutopsySwitchIsTheTable reads [Session.Autopsy]'s switch out of the
// source and requires its labels to be exactly the verbs the table gives the
// autopsy, less the ones that leave through the movement rule: a verb answered
// there and not offered hides a command that works, and one offered and not
// answered advertises a command that does not.
func TestTheAutopsySwitchIsTheTable(t *testing.T) {
	t.Parallel()

	answered := autopsySwitchLabels(t)

	for _, c := range commandsOn(frontAutopsy) {
		leaves := c.effect == effectMoves && c.onFront(frontPrompt)
		switch {
		case leaves && answered[c.verb]:
			t.Errorf("%q leaves the autopsy through the movement rule, so it needs no case of its own", c.verb)
		case !leaves && !answered[c.verb]:
			t.Errorf("the table gives the autopsy %q but its switch does not answer it, so the completer offers a command that fails; the switch answers: %s",
				c.verb, spellingsOf(answered))
		}
	}
	for verb := range answered {
		if !autopsyVerbs[verb] {
			t.Errorf("the autopsy answers %q but the table does not give it to the autopsy, so the completer hides a command that works", verb)
		}
	}
}

// TestEveryPromptVerbIsAnsweredLeftOrRefusedAtTheAutopsy drives the autopsy
// with each verb the prompt knows. Whatever it answers, it never answers "unknown
// command" for a verb in the vocabulary: that sends the author looking for a
// misspelling that is not there.
func TestEveryPromptVerbIsAnsweredLeftOrRefusedAtTheAutopsy(t *testing.T) {
	t.Parallel()

	for _, c := range commands {
		for _, spelling := range append([]string{c.verb}, c.aliases...) {
			t.Run(spelling, func(t *testing.T) {
				t.Parallel()

				var out strings.Builder
				s, err := New(Options{In: strings.NewReader(spelling + "\nquit\n"), Out: &out})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				s.Autopsy(t.Context(), &v1.Scope{}, nil, []string{"a failure"})

				if strings.Contains(out.String(), "unknown command") {
					t.Errorf("%q is in the vocabulary but the autopsy called it unknown:\n%s", spelling, out.String())
				}
			})
		}
	}
}

// autopsySwitchLabels are the canonical verbs [Session.Autopsy]'s switch names —
// read out of the source rather than restated, since restating it is the thing
// that went wrong.
//
// Every case label counts. The movement verbs are not among them: they leave
// through the table's effect, ahead of the switch.
func autopsySwitchLabels(t *testing.T) map[string]bool {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "session.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	labels := map[string]bool{}

	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Autopsy" {
			return true
		}

		ast.Inspect(fn.Body, func(inner ast.Node) bool {
			clause, ok := inner.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expression := range clause.List {
				literal, ok := expression.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				spelling, err := strconv.Unquote(literal.Value)
				if err != nil {
					continue
				}
				// Aliases resolve through the table, so `p` counts as
				// `inspect` — the same resolution the dispatch does.
				if known, ok := resolve(spelling); ok {
					labels[known.verb] = true
				}
			}

			return true
		})

		return false
	})

	if len(labels) == 0 {
		t.Fatal("walked Autopsy and found no verbs at all, which means this test cannot fail for the reason it exists")
	}

	return labels
}

// spellingsOf is what the walk actually read, so a failure names it rather than
// leaving the reader to guess whether the walk or the switch is wrong.
func spellingsOf(labels map[string]bool) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	slices.Sort(names)

	return strings.Join(names, ", ")
}

// TestTheAutopsyHelpNamesEveryVerbItOffers drives the prompt rather than
// reading the source, because this is the one list a person sees. A verb the
// switch answers and the completer offers is still undiscoverable to somebody
// who types `help` at an unfamiliar prompt and reads what comes back — and
// `help` is the only thing they know to type.
func TestTheAutopsyHelpNamesEveryVerbItOffers(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := New(Options{In: strings.NewReader("help\nquit\n"), Out: &out})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	session.Autopsy(t.Context(), &v1.Scope{}, nil, []string{"a failure"})

	printed := out.String()
	for verb := range autopsyVerbs {
		if !startsALine(printed, verb) {
			t.Errorf("the autopsy offers %q and its `help` does not list it, so nothing at that prompt says the command exists:\n\n%s", verb, printed)
		}
	}
}

// startsALine reports whether the verb opens one of the printed lines, which is
// the shape of a help row. Anything looser passes on the verb appearing inside
// a sentence — `scope` matches the intro line either way, and a row is what the
// test is about.
//
// [Prompt] is treated as a line break because it is one to a reader: the prompt
// is written without a newline after it, so the first row of any answer shares
// its line.
func startsALine(printed, verb string) bool {
	for line := range strings.SplitSeq(strings.ReplaceAll(printed, Prompt, "\n"), "\n") {
		rest, ok := strings.CutPrefix(strings.TrimLeft(line, " \t"), verb)
		if !ok {
			continue
		}
		if rest == "" || strings.HasPrefix(rest, " ") || strings.HasPrefix(rest, ",") {
			return true
		}
	}

	return false
}
