package flowdebug

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// A recorded script, read back.
//
// [Session.Script] hands back the commands a session accepted and the
// `flowstate_debug` tool answers with the same list, so the *recording* half of
// #928's record-and-replay has existed since slice 1. This is the half that
// reads one: the bounds a script file is admitted under, and the checks that
// turn a script the run will disagree with into a diagnostic before anything
// runs. `flow debug replay` is the verb over it (#1111, item 3).
//
// # A script is the command stream, not a format
//
// The file is the lines a session reads, verbatim. That is the whole design,
// and it is a decision rather than a shortcut: a session already reads its
// commands as a *stream* ([Options.In], and [Console]'s doc states the property
// in as many words — a console's line and a script's line take the same path
// from there), and `flowstate_debug` builds exactly this by joining its
// `commands` array with newlines (`cmd/flow/mcpdebug.go`). So a file of lines
// is not a new spelling of anything; it is what the only two producers in the
// tree already emit.
//
// The consequence worth stating is what it buys. `flow debug replay script wf`
// and `flow run local --debug wf < script` cannot disagree about what a command
// *does*, because there is no transformation between the file and the stream
// for them to disagree about. The moment replay pre-processed the file —
// stripping comments, dropping blank lines, "normalising" anything — it would
// be a second answer to a question the session already answers, which is the
// failure CLAUDE.md's two-drivers section is about.
//
// The two are not identical, and the difference is worth naming precisely
// because it is the only one: [CheckScript] refuses some files that stdin would
// have accepted and run — a misspelled verb, a `break` on a step nothing
// declares, a blank line. That is a pre-flight over an artifact, in the same
// category as `flow validate` refusing a file the engine would happily have
// executed differently than its author meant; it changes what *starts*, never
// what a line means once it does. Nothing here reinterprets a line the run then
// executes.
//
// Two things follow from that, and both are load-bearing:
//
//   - A comment is understood by [Session.dispatch] itself rather than removed
//     here. See [IsComment]: `#` had no meaning at the prompt, and giving it one
//     in the one place every front already goes through is what keeps the file
//     and the stream identical.
//   - A blank line is `step` — that is what [Session.dispatch] does with one, so
//     that is what it does here — and [CheckScript] therefore *refuses* one
//     rather than reinterpreting it. In a file a blank line reads as spacing,
//     and the failure is silent and expensive: the first draft of
//     `examples/loop-accumulate/debug.script` separated its comment paragraphs
//     with blank lines and stepped six times through a loop nobody asked it to
//     walk, in a file written by whoever had just documented the hazard.
//     Refusing it costs a real recording nothing, because a recording cannot
//     contain one: [Session.dispatch] answers an empty line by setting the verb
//     to `step` and recording *that word*, and the only other way into the
//     recording — [Session.Control] — refuses a line holding a break before it
//     is dispatched at all. So [Session.Script] never emits a blank, and this
//     refuses exactly the hand-written mistake and nothing else.
//
// # No header
//
// A script names no workflow, and the workflow is the replay verb's second
// argument.
//
// The alternative — a header naming what the session was recorded against — is
// tempting because it makes a reproduction one file. It was refused for two
// reasons. Nothing in the tree writes one: [Session.Script] returns commands and
// nothing else, and a header would have to be hand-written onto the artifact
// the only producers emit, so replay would refuse what `flowstate_debug` just
// answered with until somebody edited it. And what a header could carry is a
// *path*, which is a fact about the machine that recorded it rather than about
// the session — a reproduction pasted into an issue names a workflow the reader
// has under a different path, or does not have at all. A header that was not
// checked would be a lie, and a checked one would be a check on the wrong
// thing.
//
// Comments are free-form for exactly that reason: a script may carry a sentence
// saying what it reproduces, and nothing here reads it, so there is no syntax
// to be wrong about.

// MaxScriptProblems bounds how many problems [CheckScript] reports at once.
//
// The resource is the refusal, and the ratio is the attacker's: a file at
// [MaxScriptCommands] lines whose every line is a misspelled verb is a
// hundred thousand diagnostics, which is a message nobody reads and a
// slice sized by whoever wrote the file. Twenty is what fits on a screen —
// the same reasoning [MaxScopeNames] states for a list a person is meant
// to scan — and the count of what was found travels beside them, so a
// bounded report never reads as a short one.
const MaxScriptProblems = 20

// ScriptProblem is one thing wrong with a recorded script, positioned in it.
//
// A position, what is wrong, and what to do instead: the standard
// `flowfile/validate.go` sets and CLAUDE.md restates. The type is this
// package's own rather than [flowfile.Diagnostic] because that type is shaped
// by the language it reports on — it carries a step, a field, a kind key — and
// none of those are things a line of a debugging script has.
type ScriptProblem struct {
	// Line is the 1-based line in the script.
	Line int

	// Column is the 1-based column within Line, counting characters rather
	// than bytes, as [flowfile.Diagnostic.Column] does.
	Column int

	// Message states the problem and, where possible, how to fix it.
	Message string
}

// String renders one problem as `<line>:<column>: <message>` — the tail a
// caller joins to the file name, which is the one shape every positioned
// diagnostic in this repository takes (#384, and `cmd/flow/diagnostics.go`'s
// positionLine for the join).
func (p ScriptProblem) String() string {
	return fmt.Sprintf("%d:%d: %s", p.Line, p.Column, p.Message)
}

// ReadScript reads a recorded script's lines from r.
//
// The lines come back verbatim and complete — comments and blank lines
// included — because they are the stream a session reads and not a filtered
// view of it. See this file's own header for why that is the design.
//
// # The bounds
//
// A script is untrusted input chosen by whoever hands it over, and the three
// resources it controls are bounded separately, because bounding one does not
// bound another the writer controls the ratio to (CLAUDE.md):
//
//   - Bytes, before the file is in memory, at [MaxScriptBytes]. The reason to
//     bound it *here* rather than after reading is the reason an HTTP response
//     is bounded before it is read: a bound applied to a value already held is
//     not a bound.
//   - Lines, at [MaxScriptCommands]. Not implied by the byte bound and not
//     close to it: a megabyte of newlines is a million blank lines, and a blank
//     line is a `step` — ten times the count a session will record, from a file
//     comfortably inside the byte bound.
//   - The length of one line, in [CheckScript], at [MaxCommandBytes] — which is
//     the reader's own bound ([New] sizes the scanner by it), so a line past it
//     stops the stream where it sits and every command after it is never read.
//     Reported as a positioned problem rather than refused here, because
//     "line 9 is too long" is worth more than "this file is too long".
//
// The first two numbers are the two a session's own recording is bounded by,
// deliberately, rather than a second pair invented for reading: a script larger
// than [Session.Script] could ever have produced is not a recorded session, and
// two numbers answering one question are two numbers that can disagree.
//
// They are not the same *measurement*, and the difference goes the safe way.
// [MaxScriptBytes] bounds the recording's commands; here it bounds the file,
// which also carries a newline per line and whatever comments somebody wrote.
// So a recording sitting at the very top of its own byte budget, written out,
// is a few tens of kilobytes over this — refused rather than admitted, which is
// the direction to be wrong in, and a script that size stopped being a
// reproduction anybody would read long before it got there.
func ReadScript(r io.Reader) ([]string, error) {
	// One byte over the bound, so that hitting the limit is distinguishable
	// from a file that happens to end exactly there — the same reason [New]
	// sizes the command scanner at MaxCommandBytes+1.
	data, err := io.ReadAll(io.LimitReader(r, MaxScriptBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxScriptBytes {
		return nil, fmt.Errorf("a debug script may be at most %d bytes and this one is larger: "+
			"that is the same bound a session's own recording is held to, so a file this "+
			"large is not one a session produced", MaxScriptBytes)
	}

	lines := splitScriptLines(string(data))
	if len(lines) > MaxScriptCommands {
		return nil, fmt.Errorf("a debug script may be at most %d lines and this one has %d: "+
			"that is the same bound a session's own recording is held to, and a blank line "+
			"is a `step`, so every line here is a command", MaxScriptCommands, len(lines))
	}

	return lines, nil
}

// splitScriptLines cuts a script into the lines a session's reader would see.
//
// Exactly [bufio.ScanLines], because that is what the session's own scanner
// runs and any difference here is a difference between replaying a file and
// redirecting it: a trailing `\r` is dropped, and a final newline ends the last
// line rather than starting an empty one. The second is not a nicety — an extra
// empty line at the end of a script is an extra `step`, which is one more
// boundary of somebody's workflow than they asked for.
func splitScriptLines(text string) []string {
	if text == "" {
		return nil
	}

	lines := strings.Split(text, "\n")
	if last := len(lines) - 1; lines[last] == "" {
		lines = lines[:last]
	}
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}

	return lines
}

// CheckScript reports what is wrong with a script before a session runs it, and
// how many problems there were.
//
// steps are the ids the run may reach — [Options.Steps], which `flow run local
// --debug` and `flow test --debug` both already compute — and an empty set
// means the caller could not work them out, so the checks that need them are
// skipped rather than answered wrongly. A false diagnostic is worse than a
// missing one (CLAUDE.md), and "this workflow declares no steps" is not
// something this package can tell apart from "nobody asked the workflow".
//
// # Why a script is checked when a prompt is not
//
// [Session.dispatch] answers a mistyped command and asks again, deliberately:
// "ending someone's run over a typo is the worst possible reading of an
// ambiguous line". That reading depends on there being a next line from the
// person who typed the last one. A file has no next line — the typo is a defect
// in the artifact, the same as a misspelled key in a Flowfile, and reporting it
// before the run costs nothing and saves a run that silently did something else.
//
// It is a pre-flight rather than a second semantics: nothing here changes what
// a session does with a line it is given. A script that passes runs exactly as
// the same bytes on stdin would.
func CheckScript(lines []string, steps []string) (problems []ScriptProblem, total int) {
	known := make(map[string]struct{}, len(steps))
	for _, id := range steps {
		known[id] = struct{}{}
	}

	report := func(line, column int, format string, args ...any) {
		total++
		if len(problems) >= MaxScriptProblems {
			return
		}
		problems = append(problems, ScriptProblem{Line: line, Column: column, Message: fmt.Sprintf(format, args...)})
	}

	for i, line := range lines {
		number := i + 1

		if len(line) > MaxCommandBytes {
			report(number, 1, "this command is %d bytes and a command may be at most %d: the "+
				"session's reader stops at the first line longer than that, so every command "+
				"after this one would never be read", len(line), MaxCommandBytes)

			continue
		}

		// A comment is nothing, and that is [Session.dispatch]'s answer rather
		// than this function's — see the header. It cannot be wrong, so it is
		// not checked.
		if IsComment(line) {
			continue
		}

		// A blank line is the one place a file and a prompt read the same
		// bytes differently, so it is refused rather than run. See the header
		// for why this costs a recorded script nothing.
		if strings.TrimSpace(line) == "" {
			report(number, 1, "a blank line is `step`, and in a file it reads as spacing: write "+
				"`step` if that is what this line means, or `#` to keep the blank one. A "+
				"session records an empty line as `step`, so a recorded script never has one")

			continue
		}

		verb, rest := split(line)
		verbColumn := columnOf(line, leadingSpace(line))

		command, ok := resolve(verb)
		if !ok {
			if suggestion, found := nearest.Name(verb, spellings()); found {
				report(number, verbColumn, "unknown command %q: did you mean %q?", verb, suggestion)
			} else {
				report(number, verbColumn, "unknown command %q: a session understands %s", verb, verbList())
			}

			continue
		}

		switch command.verb {
		case "until":
			target := strings.TrimSpace(rest)
			if target == "" {
				report(number, verbColumn, "%s", usageUntil)

				continue
			}
			checkStepArgument(report, number, line, target, known)

		case "break":
			id, condition, conditional, err := splitCondition(rest)
			if err != nil {
				report(number, verbColumn, "break: %v", err)

				continue
			}
			if id == "" {
				report(number, verbColumn, "%s", usageBreak)

				continue
			}

			// The condition's *shape*, and no more than that. Whether it
			// compiles is a question about the run's profile and the names in
			// scope where the breakpoint is set, which this function does not
			// have and must not guess at — [compileCondition] asks it at the
			// prompt, against the real environment, and refuses there. What is
			// answerable here is the one shape that is wrong in every scope:
			// an `if` with nothing after it.
			if conditional && strings.TrimSpace(condition) == "" {
				report(number, verbColumn, "break %s: %s", id, usageCondition)

				continue
			}
			checkStepArgument(report, number, line, id, known)

		case "inspect":
			if strings.TrimSpace(rest) == "" {
				report(number, verbColumn, "%s", usageInspect)
			}
		}
	}

	return problems, total
}

// checkStepArgument reports a `break` or `until` naming a step the run cannot
// reach.
//
// Only where the caller supplied the inventory, and the inventory is the same
// one the prompt completes over ([Options.Steps]) rather than a second walk of
// the workflow — including a `call:`'s callee, which `cmd/flow`'s stepIDs
// follows precisely because a breakpoint there is one the run genuinely stops
// at.
func checkStepArgument(
	report func(line, column int, format string, args ...any),
	number int,
	line string,
	id string,
	known map[string]struct{},
) {
	if len(known) == 0 {
		return
	}
	if _, ok := known[id]; ok {
		return
	}

	column := columnOf(line, argumentOffset(line))
	names := make([]string, 0, len(known))
	for name := range known {
		names = append(names, name)
	}

	// Sorted before it is asked, because [nearest.Name] keeps the first
	// candidate at the best distance: over a map's iteration order two equally
	// near ids make the suggestion a coin toss, differing between runs of this
	// same command and between this front and the prompt, which answers the
	// identical question. [stepList] below sorts for its own reasons; this is
	// the answer above it needing the same guarantee.
	slices.Sort(names)

	if suggestion, found := nearest.Name(id, names); found {
		report(number, column, "no step named %q: did you mean %q?", id, suggestion)

		return
	}
	report(number, column, "no step named %q: this workflow declares %s", id, stepList(names))
}

// leadingSpace is the byte offset of a line's first word.
func leadingSpace(line string) int {
	return len(line) - len(strings.TrimLeft(line, " \t"))
}

// argumentOffset is the byte offset of the word after the verb, or the verb's
// own offset when there is none — so a diagnostic about a missing argument
// still points at something on the line rather than past its end.
func argumentOffset(line string) int {
	start := leadingSpace(line)
	rest := line[start:]

	verb, _ := cutWord(rest)
	after := rest[len(verb):]
	gap := len(after) - len(strings.TrimLeft(after, " \t"))
	if len(after) == gap {
		return start
	}

	return start + len(verb) + gap
}

// columnOf turns a byte offset into the 1-based character column a diagnostic
// carries. Characters rather than bytes, matching what every other position in
// this repository counts.
func columnOf(line string, offset int) int {
	if offset > len(line) {
		offset = len(line)
	}

	return utf8.RuneCountInString(line[:offset]) + 1
}

// spellings is every word [resolve] accepts, canonical verbs and aliases alike.
//
// Derived from the vocabulary table rather than written out, so a verb added
// there is a verb this suggests: the table is already the one list `dispatch`,
// `help` and the completer read, and a fourth hand-kept copy is exactly the
// drift it exists to prevent.
func spellings() []string {
	var names []string
	for _, c := range commands {
		names = append(names, c.verb)
		names = append(names, c.aliases...)
	}

	return names
}

// verbList is the canonical verbs, in help's order, for a diagnostic that has
// no closer suggestion to offer.
//
// Aliases left out on purpose: the point is to name the vocabulary, and
// twenty-odd words half of which are one letter is a list somebody skims past.
func verbList() string {
	names := make([]string, 0, len(commands))
	for _, c := range commands {
		names = append(names, c.verb)
	}

	return strings.Join(names, ", ")
}

// stepList renders the ids a workflow declares, bounded.
//
// [MaxScopeNames] is the bound rather than a number of this function's own,
// because it is the same question that constant already answers — how many
// names one line may list before it stops being scannable — asked about a
// different list. Sorted, since a map's order would make one refusal read
// differently on every run.
func stepList(names []string) string {
	sorted := make([]string, len(names))
	copy(sorted, names)
	slices.Sort(sorted)

	if len(sorted) > MaxScopeNames {
		return quote(sorted[:MaxScopeNames]) + fmt.Sprintf(" and %d more", len(sorted)-MaxScopeNames)
	}

	return quote(sorted)
}

// quote renders names as a comma-separated list of quoted words.
func quote(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}

	return strings.Join(quoted, ", ")
}
