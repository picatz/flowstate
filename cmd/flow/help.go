package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Help and the error report are the two surfaces this CLI draws for somebody who
// has not got what they wanted yet, and they are drawn here rather than by fang.
//
// The reason is a query nothing could stop. fang resolves its palette through
// `mustColorscheme`, which asks the terminal for its background colour before
// building the styles every one of its options receives — so no option avoids it,
// including the ones that supply a palette outright. That put a second asker in the
// binary, on the two paths a person reaches when something has already gone wrong,
// answering a question the ui package had rules for and fang did not.
//
// Owning the call sites is what makes those rules reach the help and the error
// report: `NO_COLOR=1 flow --help` against a pty answering nothing went from 4.05s
// to 0.02s, and `FLOWSTATE_BACKGROUND=dark` from 4.02s to 0.02s. A terminal that
// answers nothing and does want colour still waits, because that wait is lipgloss's
// one query and this does not remove it — see [ui.BackgroundEnv].
//
// What is drawn is deliberately not a reimplementation of fang's layout. The rules
// are this project's, and two of them fang does not follow: nothing is padded to a
// width a pipe cannot see, and every heading survives losing its colour.

// helpSections are the parts of a help page, in the order somebody reads them.
//
// Named rather than inlined because the order is the design: what this command is,
// how to say it, what saying it looks like, and only then the full enumeration. A
// reader who stops after the examples should already be able to use the command.
// Takes the surface rather than a writer, which is the whole of the fix for a
// mistake made twice: `flow tasks` and this renderer both wrote past the surface to
// cmd.OutOrStdout(), sending 24-bit colour to terminals that carry 256. A writer
// parameter is an invitation to pass the wrong one, and a test cannot catch it —
// calling this with the right writer proves nothing about what the caller passes,
// and the caller only misbehaves against a real terminal.
//
// So there is no writer to pass. The surface knows which stream it is and which
// writer degrades it.
func renderHelp(surface *ui.UI, c *cobra.Command) {
	w := surface.Out
	theme := surface.Theme
	width := surface.Caps.Width

	var b strings.Builder

	if summary := cmp.Or(c.Long, c.Short); summary != "" {
		fmt.Fprintln(&b, wrap(summary, width))
		fmt.Fprintln(&b)
	}

	section(&b, theme, "usage")
	fmt.Fprintln(&b, indent(theme.Strong.Render(useLine(c))))

	if examples := exampleLines(c); len(examples) > 0 {
		fmt.Fprintln(&b)
		section(&b, theme, "examples")
		for _, line := range examples {
			if line == "" {
				fmt.Fprintln(&b)

				continue
			}

			fmt.Fprintln(&b, indent(styleExample(theme, line, width)))
		}
	}

	groups := commandGroups(c)
	flags := flagEntries(c)

	// One name column for the whole page rather than one per section. Each list is
	// the same kind of thing — a name and what it does — and aligning them
	// separately puts three description columns at three different depths, which
	// the eye reads as three unrelated tables rather than one page.
	names := columnWidth(groups, flags)

	for _, group := range groups {
		fmt.Fprintln(&b)
		section(&b, theme, group.title)
		writeColumns(&b, theme, group.entries, names, width)
	}

	if len(flags) > 0 {
		fmt.Fprintln(&b)
		section(&b, theme, "flags")
		writeColumns(&b, theme, flags, names, width)
	}

	fmt.Fprint(w, b.String())
}

// columnWidth is the width of the name column, across every list on the page.
func columnWidth(groups []group, flags []column) int {
	var widest int

	measure := func(entries []column) {
		for _, e := range entries {
			widest = max(widest, lipgloss.Width(e.name))
		}
	}

	for _, g := range groups {
		measure(g.entries)
	}
	measure(flags)

	return widest
}

// section writes a heading.
//
// Upper-cased rather than coloured, on the package's own rule: a heading that is
// only a colour is not a heading in a log file, through a pipe, or for a reader who
// cannot see the colour. The case carries it and the colour makes it faster to find.
func section(b *strings.Builder, theme ui.Theme, title string) {
	fmt.Fprintln(b, theme.Accent.Render(strings.ToUpper(title)))
}

// indent puts a body line under its heading.
//
// Two spaces, and applied to a line that is already styled: a style is escape
// sequences around the text, so indenting after styling keeps the prefix outside
// them and the alignment measurable.
func indent(line string) string {
	return "  " + line
}

// useLine is what somebody types, built rather than taken from cobra's UseLine.
//
// The order is the reason. UseLine appends `[flags]` immediately after the command
// path, so a command with subcommands reads `flow [flags] [command]` — which is a
// line nobody types and, read literally, says the flags come first. What somebody
// actually types is the command, then its arguments, then its flags.
func useLine(c *cobra.Command) string {
	parts := []string{c.CommandPath()}

	if c.HasAvailableSubCommands() {
		parts = append(parts, "[command]")
	}

	if _, args, found := strings.Cut(c.Use, " "); found {
		parts = append(parts, args)
	}

	if c.HasAvailableFlags() && !c.DisableFlagsInUseLine {
		parts = append(parts, "[flags]")
	}

	return strings.Join(parts, " ")
}

// exampleLines splits a command's Example block into lines.
//
// The blank lines between examples are kept, because they are what separates one
// example from the next — a comment and the command it explains are a pair, and a
// list of eight lines with no gaps reads as one example with four comments in it.
// Runs of blanks collapse to one, since how a Go string literal is spaced is not a
// decision about the help page.
func exampleLines(c *cobra.Command) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(c.Example), "\n") {
		line = strings.TrimSpace(line)
		if line == "" && (len(lines) == 0 || lines[len(lines)-1] == "") {
			continue
		}

		lines = append(lines, line)
	}

	return lines
}

// styleExample renders one line of an example block.
//
// A comment is the reason and the command is the thing to copy, so they are styled
// oppositely: the comment recedes, the command is the one the eye should land on.
func styleExample(theme ui.Theme, line string, width int) string {
	line = ui.Trim(line, width-2)

	if strings.HasPrefix(line, "#") {
		return theme.Muted.Render(line)
	}

	return theme.Strong.Render(line)
}

// column is one row of a two-column list: a name and what it does.
type column struct {
	// sort is what the row files under, where that is not what it renders as.
	sort string

	name string
	text string
}

// group is a titled list of commands.
type group struct {
	title   string
	entries []column
}

// commandGroups lists a command's subcommands under their group headings.
//
// cobra's own groups are used where a command declares one, and everything else
// falls under a general heading *last* rather than first. That ordering is the
// point: `completion` and `help` are the two commands nobody came here for, and
// putting the ungrouped ones first buries the workflow commands below a fold.
func commandGroups(c *cobra.Command) []group {
	byGroup := map[string][]column{}
	for _, sub := range c.Commands() {
		if !sub.IsAvailableCommand() || sub.IsAdditionalHelpTopicCommand() {
			continue
		}
		byGroup[sub.GroupID] = append(byGroup[sub.GroupID], column{
			name: subcommandUse(sub),
			text: sub.Short,
		})
	}

	var groups []group
	for _, declared := range c.Groups() {
		if entries := byGroup[declared.ID]; len(entries) > 0 {
			groups = append(groups, group{title: declared.Title, entries: entries})
		}
	}

	if entries := byGroup[""]; len(entries) > 0 {
		title := "commands"
		if len(groups) > 0 {
			title = "other commands"
		}
		groups = append(groups, group{title: title, entries: entries})
	}

	return groups
}

// subcommandUse is a subcommand's name and its arguments, in the order they are
// typed and without the parent path.
func subcommandUse(c *cobra.Command) string {
	name, args, _ := strings.Cut(c.Use, " ")

	parts := []string{name}
	if c.HasAvailableSubCommands() {
		parts = append(parts, "[command]")
	}
	if args != "" {
		parts = append(parts, args)
	}

	return strings.Join(parts, " ")
}

// flagEntries lists a command's flags, its own and everything inherited.
func flagEntries(c *cobra.Command) []column {
	seen := map[string]bool{}

	var entries []column
	add := func(f *pflag.Flag) {
		if f.Hidden || seen[f.Name] {
			return
		}
		seen[f.Name] = true

		entries = append(entries, column{sort: f.Name, name: flagName(f), text: flagUsage(f)})
	}

	c.LocalFlags().VisitAll(add)
	c.InheritedFlags().VisitAll(add)

	// By the long name, not by how the row is rendered. A shorthand puts `-v,`
	// in front of `--verbose`, so sorting the rendered string files every flag
	// that has one under its letter — which is why `--version` sorted above
	// `-v, --verbose` and the two `v` flags ended up apart.
	slices.SortFunc(entries, func(a, b column) int { return cmp.Compare(a.sort, b.sort) })

	return entries
}

// flagName is how a flag is written on a command line, short form first.
//
// A flag with no shorthand is padded by the width of one, so that every `--` on the
// page starts in the same column. Without it the long names sit four characters
// apart depending on whether somebody gave that flag a letter, which is not a
// distinction a reader is scanning for.
func flagName(f *pflag.Flag) string {
	name := "    --" + f.Name
	if f.Shorthand != "" {
		name = "-" + f.Shorthand + ", --" + f.Name
	}

	if kind := f.Value.Type(); kind != "bool" {
		name += " " + placeholder(kind)
	}

	return name
}

// placeholder names what a flag takes, in the vocabulary an author already has.
func placeholder(kind string) string {
	switch kind {
	case "stringSlice", "stringArray":
		return "<string,...>"
	default:
		return "<" + kind + ">"
	}
}

// flagUsage is a flag's description with its default, where the default is worth
// stating.
//
// An empty string and a false are not: "(default \"\")" tells a reader nothing they
// did not already assume, and it is the majority of the flags on any command.
func flagUsage(f *pflag.Flag) string {
	usage := f.Usage

	// cobra writes these two itself, in a shape that describes the mechanism
	// rather than the effect — and `help for flow` on a page headed FLAGS is
	// three words of which one is new. Rewritten rather than left, because they
	// are the first two rows a newcomer reads.
	switch f.Name {
	case "help":
		usage = "Show this help"
	case "version":
		usage = "Show the version and exit"
	}

	if f.DefValue != "" && f.DefValue != "false" && f.DefValue != "0" && f.DefValue != "[]" {
		usage += " (default " + f.DefValue + ")"
	}

	return sentence(usage)
}

// sentence starts a description with a capital.
//
// Applied at the rendering rather than fixed at each of the several dozen call
// sites, because it is a decision about the page: a column where some rows start
// upper and some lower reads as two people having written it, and which one a given
// flag got is an accident of who added it. The words themselves stay whoever's they
// were.
func sentence(text string) string {
	if text == "" {
		return text
	}

	runes := []rune(text)
	runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]

	return string(runes)
}

// writeColumns lays out a two-column list.
//
// The names are aligned to the widest of them and the descriptions wrap under
// themselves rather than running off the edge, because a description cut at the
// right margin loses the half that says what the flag is for. Where the names are
// so wide that nothing useful is left, the description goes on its own line instead
// — a narrow terminal is somebody's real terminal, not an edge case.
func writeColumns(b *strings.Builder, theme ui.Theme, entries []column, widest, width int) {
	const gutter = 2

	textWidth := width - 2 - widest - gutter
	if textWidth < minDescription {
		for _, e := range entries {
			fmt.Fprintln(b, indent(theme.Strong.Render(e.name)))
			if e.text != "" {
				fmt.Fprintln(b, indent(indent(theme.Muted.Render(ui.Trim(e.text, width-4)))))
			}
		}

		return
	}

	for _, e := range entries {
		pad := strings.Repeat(" ", widest-lipgloss.Width(e.name)+gutter)
		wrapped := strings.Split(wrap(e.text, textWidth), "\n")

		fmt.Fprintln(b, indent(theme.Strong.Render(e.name)+pad+theme.Muted.Render(wrapped[0])))
		for _, line := range wrapped[1:] {
			fmt.Fprintln(b, indent(strings.Repeat(" ", widest+gutter)+theme.Muted.Render(line)))
		}
	}
}

// minDescription is the narrowest a description column may be before it is worth
// giving the description its own line instead.
const minDescription = 20

// wrap breaks text to a width at word boundaries, without padding it out to one and
// without splitting a word that does not fit.
//
// Two things the obvious implementations do that are wrong here. lipgloss's Width
// pads every short line out to the full width, which looks identical on a terminal
// and puts trailing whitespace on every line of `flow --help | cat`. And both it and
// ansi.Wordwrap break at a hyphen, so a token longer than the line is cut wherever
// the margin lands:
//
//	/home/kent/very/long/path/to/a/workflow-
//	file.yaml:6:15: step "web" …
//
// A path, a URL and a workflow id are the things somebody most wants to copy out of
// an error, and that is the one place a break costs something. So a long token
// overflows the measure instead — the line is wider than it should be, which a
// terminal reflows, and the token is still one token.
// Breaking on whitespace and nothing else is the whole of it, and it is why neither
// lipgloss's Width nor ansi.Wordwrap is used: both treat a hyphen as a breakpoint, so
// both cut that path at `aae3-`. A word wider than the measure overflows instead. The
// terminal soft-wraps it, which looks the same and leaves it one token to select.
func wrap(text string, width int) string {
	if width < 1 {
		return text
	}

	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = wrapLine(line, width)
	}

	return strings.Join(lines, "\n")
}

// wrapLine wraps one line, breaking only between words.
func wrapLine(line string, width int) string {
	words := strings.Fields(line)
	if len(words) == 0 {
		return ""
	}

	var b strings.Builder

	b.WriteString(words[0])
	column := lipgloss.Width(words[0])

	for _, word := range words[1:] {
		switch w := lipgloss.Width(word); {
		case column+1+w <= width:
			b.WriteString(" ")
			b.WriteString(word)
			column += 1 + w

		default:
			b.WriteString("\n")
			b.WriteString(word)
			column = w
		}
	}

	return b.String()
}
