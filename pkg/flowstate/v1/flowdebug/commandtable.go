package flowdebug

import (
	"fmt"
	"slices"
	"strings"
)

// CommandTableMarkdown renders the vocabulary as the Markdown table
// docs/DEBUGGING.md carries between its `commands:` markers: each verb with its
// short forms and argument, the fronts that answer it, and what it does.
//
// Generated from the same table the prompt, the [Driver], the completer and
// [CheckScript] read, so the document cannot teach a verb a front does not
// answer or leave one out; a test fails on drift and `-update` rewrites it.
func CommandTableMarkdown() string {
	var b strings.Builder
	b.WriteString("| Command | Where | What it does |\n| --- | --- | --- |\n")
	for _, c := range commands {
		fmt.Fprintf(&b, "| %s | %s | %s |\n", markdownCell(c.markdownSpelling()), c.frontNames(), markdownCell(c.help))
	}

	return strings.TrimRight(b.String(), "\n")
}

// markdownSpelling is a command as the table names it: the verb and argument in
// one code span, then each short form in its own.
func (c command) markdownSpelling() string {
	head := c.verb
	if c.argument != "" {
		head += " " + c.argument
	}
	out := "`" + head + "`"
	for _, alias := range c.aliases {
		out += ", `" + alias + "`"
	}

	return out
}

// frontNames lists the fronts that answer the verb, in the order the table's
// readers meet them.
func (c command) frontNames() string {
	var names []string
	for _, f := range []front{frontPrompt, frontDriver, frontAutopsy} {
		if c.onFront(f) {
			names = append(names, f.String())
		}
	}
	if c.fronts == frontsAll {
		return "every front"
	}

	return strings.Join(names, ", ")
}

// markdownCell keeps a sentence inside one table cell: a pipe would end it.
func markdownCell(text string) string {
	return strings.ReplaceAll(text, "|", `\|`)
}

// DriverCommandList is the verbs a structured front accepts, spelled with their
// argument grammar and comma-separated, for a tool description that lists them:
// `step, next, until <step>, pause, ...`. Derived from the table so the sentence
// an agent reads cannot name a verb the driver refuses or skip one it takes.
func DriverCommandList() string {
	var spellings []string
	for _, c := range commandsOn(frontDriver) {
		if c.verb == "help" {
			continue
		}
		spelling := c.verb
		if argument := c.argumentOn(frontDriver); argument != "" {
			spelling += " " + argument
		}
		spellings = append(spellings, spelling)
	}

	return strings.Join(spellings, ", ")
}

// Verb is one verb a structured front answers, spelled as the command table
// spells it for that front.
//
// It exists so a surface that binds keys to verbs (a full-screen debugger) can
// derive what it offers from the table rather than keep a second list: a verb
// the driver front does not answer is not in [DriverVerbs], so nothing can bind
// a key to it, and a verb added to the table is there to be bound or to be
// deliberately left to the console.
type Verb struct {
	// Name is the canonical spelling, the one [Driver.Do] resolves aliases to.
	Name string

	// Aliases are the short forms, in the order help shows them.
	Aliases []string

	// Argument is the grammar written after the verb on this front, or empty.
	Argument string

	// Help is the sentence beside the verb on this front.
	Help string

	// Moves reports that the verb resumes the run or steps it back.
	Moves bool
}

// DriverVerbs lists the verbs a [Driver] answers, in the table's order.
func DriverVerbs() []Verb {
	table := commandsOn(frontDriver)
	verbs := make([]Verb, 0, len(table))
	for _, c := range table {
		verbs = append(verbs, Verb{
			Name:     c.verb,
			Aliases:  slices.Clone(c.aliases),
			Argument: c.argumentOn(frontDriver),
			Help:     c.helpOn(frontDriver),
			Moves:    c.effect == effectMoves,
		})
	}

	return verbs
}
