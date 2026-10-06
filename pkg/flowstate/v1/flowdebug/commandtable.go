package flowdebug

import (
	"fmt"
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
