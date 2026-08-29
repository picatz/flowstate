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

// The vocabulary is written down twice: once in `commands`, which the main
// prompt dispatches from and the completer renders, and once as the switch
// inside [Session.Autopsy], which answers a different and smaller set because
// the run is over. Two lists for one vocabulary is this repository's
// most-paid-for shape, and it charged again here — `complete` was added to the
// table and to the main dispatch, and at the autopsy came back "unknown
// command" (Codex, #1117). That is the prompt where completion is worth most,
// since the bindings a failed case was judged under live only there.
//
// So the second list is checked against the first. A verb added to `commands`
// now fails this test until somebody decides, in writing, whether the autopsy
// answers it.

// notAtAutopsy are the verbs the autopsy deliberately does not offer, each with
// the reason it does not.
//
// Movement is the whole of it: the run is over, so a command whose only effect
// is to move it has nothing to move. Three of those spellings are still
// *accepted* by the switch — they leave, because a person typing `continue` at
// an autopsy means "I am done here" — but being accepted is not the same as
// being part of the vocabulary the autopsy offers, and it is the offering this
// list is about. So an entry here says two things: the autopsy owes this verb
// no answer, and `autopsyVerbs` must not name it.
var notAtAutopsy = map[string]string{
	"step":        "the run is over, so there is no next step to take",
	"continue":    "the run is over; typing it leaves, which `quit` says better",
	"until":       "the run is over, so there is no step to run until",
	"break":       "a breakpoint is a decision about a run that has not happened yet",
	"delete":      "there are no breakpoints to remove once nothing will be reached",
	"breakpoints": "listing them says nothing about a run that has finished",
	"info":        "it describes the step the run is stopped at, and it is stopped at none",
}

// TestEveryVerbIsDecidedAtTheAutopsy reads [Session.Autopsy]'s switch out of the
// source and requires every verb in `commands` to be either answered there or
// listed in notAtAutopsy with a reason — and requires the two readings of the
// answered set, the switch and `autopsyVerbs`, to agree.
func TestEveryVerbIsDecidedAtTheAutopsy(t *testing.T) {
	t.Parallel()

	answered := autopsySwitchLabels(t)

	for _, c := range commands {
		reason, excluded := notAtAutopsy[c.verb]
		switch {
		case excluded && reason == "":
			t.Errorf("notAtAutopsy lists %q with no reason; the reason is the point of the entry", c.verb)

		case excluded:
			// Deliberately not part of the autopsy's vocabulary, so it must
			// also not be offered there.
			if autopsyVerbs[c.verb] {
				t.Errorf("%q is excluded from the autopsy but autopsyVerbs offers it: %s", c.verb, reason)
			}

		case !answered[c.verb]:
			t.Errorf(`the autopsy does not answer %q, and notAtAutopsy does not say why.

A verb in `+"`commands`"+` reaches the main prompt's dispatch. The autopsy has its
own switch, so a verb added to one and not the other is accepted at a
breakpoint and reported as an unknown command a moment later — which is what
a script hits, since it cannot see that the vocabulary changed.

Either answer it in [Session.Autopsy]'s switch, or add

    %q: "…why the finished run has nothing to say to it…",

to notAtAutopsy in autopsyverbs_internal_test.go. The switch answers: %s.`,
				c.verb, c.verb, spellingsOf(answered))

		case !autopsyVerbs[c.verb]:
			t.Errorf("the autopsy answers %q but autopsyVerbs does not offer it, so the completer hides a command that works", c.verb)
		}
	}

	for verb := range notAtAutopsy {
		if !slices.ContainsFunc(commands, func(c command) bool { return c.verb == verb }) {
			t.Errorf("notAtAutopsy lists %q, which is not a verb any more; delete the entry", verb)
		}
	}
}

// autopsySwitchLabels are the canonical verbs [Session.Autopsy]'s switch names —
// read out of the source rather than restated, since restating it is the thing
// that went wrong.
//
// Every case label counts, the leaving one included. `quit` is only ever
// reachable through that clause and is exactly what the autopsy advertises, so
// a walk that skipped the clause would report the verb the prompt names as
// unanswered. The movement spellings sharing it are separated by notAtAutopsy
// instead — a sentence somebody wrote, rather than an inference from the shape
// of a case body.
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
