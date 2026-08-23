package main

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/taskexample"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// `flow tasks` is an index and a detail view, not one page.
//
// It printed every task with its full input and output tables, then the CEL
// function catalog by library, then the duration constructors, then a note about
// `now` inside a wait, then the `vars:`/`steps:` grammar, all in one breath, with no
// way to ask about one task (#379). Each piece was worth having and together they
// were a page nobody can hold, growing in the one dimension the command could not
// subset: the catalog, which plugins already extend.
//
// So it is the same index-then-detail shape the help page already uses, and it is
// rendered by the help page's own two-column writer rather than a second one that
// looks nearly like it (docs/CLI_DESIGN.md section 5, which refuses per-command
// bespoke styling). `flow tasks` names each task and what it is for; `flow tasks
// <name>` is the whole story of one; `flow tasks --expressions` is the authoring
// vocabulary that was living here because nowhere else printed it.
//
// Every one of those reads [v1.DefaultRegistry] and the descriptors it holds.
// There is deliberately no second table: a summary, a type, a bound, and the
// example step are all derived, so a task added to the registry is a task this
// command describes with nothing here touched.

// expressionsFlag asks for the authoring vocabulary rather than the catalog.
//
// A flag rather than the `flow tasks cel` the issue sketched, and the reason is
// that the argument position belongs to task names. A plugin may register a task
// called anything the registry accepts, so a reserved word there is a word that
// stops meaning what it says the day somebody's plugin claims it, and the failure
// would be silent, printing the CEL reference to someone who asked about their
// task.
const expressionsFlag = "expressions"

// runTasks implements the tasks command: the index, one task, or the expression
// reference.
//
// Given --plugin-dir it launches those plugins first, so that what this prints is
// the catalog a worker configured the same way would run rather than this
// binary's built-ins (#724). Everything below is unchanged by it: the listing,
// the detail page and the JSON document are all read off [v1.DefaultRegistry],
// which is exactly the registry the host registers into, so a plugin's task is
// described here by the same code that describes `http` — down to the example
// step, built from descriptors the plugin shipped.
//
// A plugin that fails to launch fails this command rather than being quietly
// left out of the listing, which is the same decision [runValidate] makes for
// the same reason: a catalog missing what somebody asked to see, printed as
// though it were the whole of it, is a wrong answer rather than a smaller one.
func runTasks(cmd *cobra.Command, args []string) error {
	surface := newSurface(cmd)

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	catalog, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		// See [runValidate]: a refusal made before anything launched is a wrong
		// command line, not a plugin that would not start.
		if isUsageError(err) {
			return err
		}

		return fmt.Errorf("--plugin-dir names what this listing is supposed to include, "+
			"and one of those plugins would not start: %w", err)
	}
	defer closePlugins()

	// Or the same listing from a document, with nothing launched (#710). A
	// catalog that will not load fails this command for the reason a plugin
	// that will not start does: a listing missing what somebody asked to see,
	// printed as though it were the whole of it, is a wrong answer rather than
	// a smaller one.
	if fromFile, err := loadPluginCatalog(cmd); err != nil {
		return fmt.Errorf("--%s names what this listing is supposed to include, and it "+
			"could not be read: %w", pluginCatalogFlag, err)
	} else if fromFile != nil {
		catalog = fromFile
	}

	expressions, _ := cmd.Flags().GetBool(expressionsFlag)
	if expressions && len(args) > 0 {
		return newUsageError(fmt.Errorf(
			"--%s prints the vocabulary every expression reaches, so it takes no task name; "+
				"drop one of the two", expressionsFlag))
	}

	// The catalog is one document and stays one document, whichever text surface
	// the same invocation would have drawn. A consumer wants
	// `.tasks[] | select(.name=="http")` rather than a stream to reassemble, and
	// the expression reference is fields of that same message, so `--expressions
	// -o json` answers with the whole of it rather than a second, smaller
	// document that would be a second schema.
	//
	// Naming one task narrows it, because that is what the argument means: the
	// answer to "describe http" is a task, and handing back the catalog would make
	// every consumer do the selecting the argument already did.
	if format.Machine() {
		if len(args) == 0 {
			return writeJSON(surface, format, v1.Catalog())
		}

		def, err := lookupTaskArg(args[0])
		if err != nil {
			return err
		}

		return writeJSON(surface, format, v1.DescribeTask(def))
	}

	switch {
	case expressions:
		return writeExpressionReference(surface)
	case len(args) == 0:
		return writeTaskIndex(surface, catalog)
	default:
		def, err := lookupTaskArg(args[0])
		if err != nil {
			return err
		}

		return writeTask(surface, def, catalog)
	}
}

// pluginNameOf is the plugin a task name belongs to, and whether it belongs to
// one at all.
//
// The dot is the whole of the rule, and it is the language's rule rather than
// this command's: `example.greet` is the `greet` task of the plugin discovered as
// `flowstate-plugin-example`, no built-in name carries a dot, and that is what
// keeps installing a plugin from ever changing what an existing `http:` step
// does (see the flowfile package's unknownTaskMessage, which reads a task name
// the same way to decide whether a missing task is a spelling question or an
// installation one).
//
// Asked of the *name* rather than of the catalog because the catalog is nil
// whenever nothing was launched, and a task can be in this registry with no
// catalog beside it: `flow task run --plugin-dir` and the LSP both register into
// it, and a test registers into it directly.
func pluginNameOf(task string) (string, bool) {
	name, _, dotted := strings.Cut(task, ".")
	if !dotted || name == "" {
		return "", false
	}

	return name, true
}

// writePluginProvenance names the plugins whose tasks are in this listing, and
// where each came from.
//
// Provenance rather than decoration. Every other line this command prints is
// about what a task does, and none of it answers the question a reviewer asks
// first about a step naming `example.greet`: whose code is that, and which file
// on this machine is it. The per-task marker says a task came from a plugin; this
// says which build of which plugin, from which path, which is the fact that
// changes when somebody drops a different binary into the directory.
//
// Nothing at all when no plugins were launched, which is the overwhelmingly
// common invocation: a footer explaining the absence of plugins to somebody who
// did not ask about plugins is noise on every other run of this command.
func writePluginProvenance(b *strings.Builder, theme ui.Theme, width int, catalog *v1.PluginCatalog) {
	if len(catalog.GetPlugins()) == 0 {
		return
	}

	fmt.Fprintln(b)
	section(b, theme, "provided by plugins")

	entries := make([]column, 0, len(catalog.GetPlugins()))
	for _, p := range catalog.GetPlugins() {
		names := make([]string, 0, len(p.GetTasks()))
		for _, task := range p.GetTasks() {
			names = append(names, task.GetName())
		}

		// The version and the path on the same line as the tasks, because the
		// three are one fact: this build of this plugin, from this file, added
		// these names to the catalog above.
		entries = append(entries, column{
			name: p.GetName(),
			text: strings.Join(names, ", ") + " — " + p.GetVersion() + ", " + p.GetPath(),
		})
	}

	writeColumns(b, theme, entries, columnWidth(nil, entries), width)
}

// lookupTaskArg resolves a task name, naming the nearest one when it does not.
//
// A misspelling is the common case and the registry already knows every name, so
// refusing without saying so would be withholding the answer, which is the standard
// `flowfile`'s diagnostics hold themselves to (CLAUDE.md, "diagnostics are a
// feature"). Marked as an invocation mistake, because it is one: nothing ran, the
// command line was wrong, and the exit code should say so rather than reading like
// a task that failed.
func lookupTaskArg(name string) (v1.TaskDef, error) {
	def, found := v1.LookupTask(name)
	if found {
		return def, nil
	}

	message := fmt.Sprintf("no task named %q in this build.", name)
	if suggestion, ok := nearest.Name(name, v1.TaskNames()); ok {
		message = fmt.Sprintf("no task named %q in this build; did you mean %q?", name, suggestion)
	}

	return v1.TaskDef{}, newUsageError(fmt.Errorf(
		"%s `flow tasks` lists every task this build can run, and `flow plugins` "+
			"lists what a plugin directory would add", message))
}

// writeTaskIndex lists every task a step may name, one line each.
//
// One line, and the line is a name and a purpose, the shape docs/CLI_DESIGN.md
// gives a list rather than a summary: many rows compared against each other, so
// nothing on any of them is a block the eye lands on. The markers are words for
// the same reason the charter's outcome roles are never colour alone; they read
// identically through a pipe, in a CI log, and to somebody who cannot see the
// colour.
//
// Laid out by the help page's own [writeColumns], which wraps a description under
// itself and gives it its own line where the terminal is too narrow to hold both.
// A narrow terminal is somebody's real terminal.
func writeTaskIndex(surface *ui.UI, catalog *v1.PluginCatalog) error {
	var (
		theme = surface.Theme
		width = surface.Caps.Width
		b     strings.Builder
	)

	defs := v1.DefaultRegistry().All()

	entries := make([]column, 0, len(defs))
	for _, def := range defs {
		text := sentence(def.Summary)
		if markers := taskMarkers(def); len(markers) > 0 {
			text = strings.TrimSuffix(text, " ") + " [" + strings.Join(markers, ", ") + "]"
		}

		entries = append(entries, column{name: def.Name, text: text})
	}

	section(&b, theme, "tasks")
	writeColumns(&b, theme, entries, columnWidth(nil, entries), width)

	// Which of the names above are not this binary's, said once and in full,
	// rather than left to be inferred from the dots in them.
	writePluginProvenance(&b, theme, width, catalog)

	// The footer is where the rest of the page went, so it names both halves. A
	// reference nobody can find is a reference that has been deleted, and the
	// expression vocabulary was only ever printed here.
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, theme.Muted.Render(wrap(
		"`flow tasks <name>` describes one task in full: every input, what bounds it, "+
			"and a step to copy. `flow tasks --expressions` is what every expression in a "+
			"Flowfile can say.", width)))

	_, err := io.WriteString(surface.Out, b.String())

	return err
}

// taskMarkers are the terse facts about a task worth carrying in a list.
//
// Read off the definition, every one of them, so a marker cannot claim something
// the engine does not do. They are the questions an author scanning for a task
// asks before opening it: does it hand anything back, may I put a credential in
// it, does it evaluate anything itself, and is it this binary's or a plugin's.
func taskMarkers(def v1.TaskDef) []string {
	var markers []string

	if len(v1.Outputs(def)) == 0 {
		markers = append(markers, "no outputs")
	}
	if len(def.AuthorityInputs) > 0 || len(def.NestedSecretInputs) > 0 {
		markers = append(markers, "takes secrets")
	}
	if len(def.DeferredInputs) > 0 {
		markers = append(markers, "defers inputs")
	}
	if !v1.IsBuiltinTask(def.Name) {
		// Provenance, which is the one thing a reviewer needs to know about a step
		// before anything else: this one leaves the engine's own code. Named
		// rather than only flagged, now that a listing can hold several plugins'
		// tasks at once (#724): "from a plugin" in a list where four of them are
		// tells a reader that the ones with dots have dots.
		if plugin, ok := pluginNameOf(def.Name); ok {
			markers = append(markers, "from the "+plugin+" plugin")
		} else {
			markers = append(markers, "from a plugin")
		}
	}

	return markers
}

// taskProvenance is the sentence naming where a task came from, or empty for a
// built-in.
//
// Empty for a built-in deliberately: "provided by this build of flow" on every
// `flow tasks http` is a line that says nothing, and the absence of the line is
// itself the answer once the line exists for the tasks that need it.
//
// The catalog narrows what can be said rather than deciding whether anything is.
// A plugin task can be in the registry with no catalog beside it — the LSP
// registers one, `flow task run` registers one, and a test registers one
// directly — and in that case the honest sentence names the plugin and stops,
// rather than omitting the provenance entirely because the version and path
// happen to be unavailable.
func taskProvenance(def v1.TaskDef, catalog *v1.PluginCatalog) string {
	name, ok := pluginNameOf(def.Name)
	if !ok || v1.IsBuiltinTask(def.Name) {
		return ""
	}

	for _, p := range catalog.GetPlugins() {
		if p.GetName() != name {
			continue
		}

		return fmt.Sprintf("Provided by the %s plugin, %s, launched from %s.",
			name, p.GetVersion(), p.GetPath())
	}

	return fmt.Sprintf("Provided by the %s plugin, not by this build of flow.", name)
}

// writeTask describes one task completely.
//
// The whole story, because the argument asking for it is somebody who already knows
// which task they want: every input with what may be written in it, what the task
// evaluates itself and why that matters, what it hands back, and a step to copy.
func writeTask(surface *ui.UI, def v1.TaskDef, catalog *v1.PluginCatalog) error {
	var (
		out   = surface.Out
		theme = surface.Theme
		width = surface.Caps.Width
	)

	inputs := v1.Inputs(def)
	outputs := v1.Outputs(def)

	if _, err := fmt.Fprintf(out, "%s\n  %s\n\n", theme.Accent.Render(def.Name),
		wrap(sentence(def.Summary), width-2)); err != nil {
		return err
	}

	// Whose code this is, before what it takes. The detail page is what somebody
	// reads while deciding to write this step, and for a task that is not this
	// binary's the first thing worth knowing is which plugin, which build, and
	// which file — the same three facts the index's provenance section carries,
	// narrowed to the one task that was asked about.
	if line := taskProvenance(def, catalog); line != "" {
		if _, err := fmt.Fprintf(out, "%s\n\n", theme.Muted.Render(wrap(line, width))); err != nil {
			return err
		}
	}

	if err := writeFields(out, theme, width, []fieldGroup{
		{label: "inputs", fields: inputs},
		{label: "outputs", fields: outputs},
	}); err != nil {
		return fmt.Errorf("writing the task: %w", err)
	}

	if slices.ContainsFunc(inputs, func(f v1.InputField) bool { return f.Required }) {
		if _, err := fmt.Fprintf(out, "\n%s\n",
			theme.Muted.Render("* marks an input the task cannot run without.")); err != nil {
			return err
		}
	}

	// The annotation that used to be a trailing clause in the type column, given
	// the room to be a sentence (#379). What it says is load-bearing (it decides
	// what an author may legally write in those inputs), and a listing that
	// delivers it as five muted words beside a type has told nobody anything.
	if len(def.DeferredInputs) > 0 {
		names := slices.Sorted(slices.Values(def.DeferredInputs))
		if _, err := fmt.Fprintf(out, "\n%s\n%s\n",
			theme.Accent.Render("Evaluated by the task, not by the engine: "+strings.Join(names, ", ")),
			wrap("The engine resolves an expression before it schedules the step, which is why an "+
				"ordinary input cannot name something that does not exist yet. These the task "+
				"evaluates itself, in its own scope, after it has run far enough to have one: it "+
				"is how `http`'s `outputs` may name `status_code`.", width)); err != nil {
			return err
		}
	}

	example, err := taskexample.Build(def)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(out, "\n%s\n%s\n",
		theme.Accent.Render("A step that uses it:"), example); err != nil {
		return err
	}

	return nil
}

// writeExpressionReference prints what every expression in a Flowfile can say.
//
// Moved out of the task listing rather than deleted, because it is the authoring
// companion to `flow validate`'s vocabulary and this was the only place in the
// terminal that printed it. What it is not is part of a task list: none of it is
// about a task, and it grew to be most of what `flow tasks` printed.
//
// The function names are laid out by the same wrapping writer the rest of the CLI
// uses, which is the fix for the symptom that named this issue: the `math` line was
// joined by hand and cut wherever the terminal's margin fell, splitting `math.sign`
// into `math.si` and `gn`. [wrap] breaks between words and never inside one.
func writeExpressionReference(surface *ui.UI) error {
	var (
		theme = surface.Theme
		width = surface.Caps.Width
		b     strings.Builder
	)

	// "every expression", not "the cel task". These used to be opt-in per `cel`
	// step, which made a listing accurate for one step kind and misleading for the
	// rest of the file: an author reading it to find out what an `if:` could say
	// got the wrong answer.
	//
	// Named, rather than only counted: a profile is a *membership*, and one nobody
	// can enumerate is one nobody can write against. A macro is marked because the
	// difference reaches an author. It is expanded when the file compiles, so it
	// is frozen into the compiled workflow, where a function is looked up by
	// whichever worker evaluates the run.
	if functions := v1.ProfileFunctions(v1.CurrentProfile); len(functions) > 0 {
		section(&b, theme, "functions every expression reaches")

		entries := make([]column, 0, len(v1.ExtensionLibraries()))
		for _, lib := range v1.ExtensionLibraries() {
			names := make([]string, 0, len(functions))
			for _, fn := range functions {
				if fn.Library != lib {
					continue
				}
				if fn.Macro {
					names = append(names, fn.Name+" (macro)")

					continue
				}
				names = append(names, fn.Name)
			}
			if len(names) == 0 {
				continue
			}

			entries = append(entries, column{name: lib, text: strings.Join(names, ", ")})
		}

		writeColumns(&b, theme, entries, columnWidth(nil, entries), width)

		// Said once rather than inlined per entry. A macro's name is not its call
		// form (cel-go reports `greatest` for `math.greatest(1, 2)`), so a reader
		// needs an example, and ninety names with two long expressions spliced into
		// them is harder to scan than a list of names plus one line.
		//
		// The examples come from the catalog rather than being written here, so this
		// line cannot describe a spelling the schema does not carry. Two of them,
		// because the two shapes are the whole point: one goes on a namespace and one
		// on a value, and showing only either would imply macros are all alike.
		if written := macroExamplesFor(functions, "greatest", "sortBy"); written != "" {
			fmt.Fprintln(&b)
			fmt.Fprintln(&b, theme.Muted.Render(wrap("A macro goes on something ("+written+
				") and is expanded when the file compiles.", width)))
		}
	}

	fmt.Fprintln(&b)
	section(&b, theme, "durations")
	fmt.Fprintln(&b, indentBlock(wrap(strings.Join(v1.DurationUnits(), ", "), width-2)))

	fmt.Fprintln(&b)
	section(&b, theme, "waiting")
	fmt.Fprintln(&b, indentBlock(wrap(fmt.Sprintf(
		"Inside a wait (sleep, wait_until, a signal's timeout), %s is the moment the wait is "+
			"evaluated, so a deadline is ${%s + days(3)} and a remaining bound is ${deadline - %s}.",
		v1.NowIdentifier, v1.NowIdentifier, v1.NowIdentifier), width-2)))

	// Where a value comes from, which a task listing otherwise leaves somebody to
	// guess at. A task is an *effect*; one of the two built-in ones produces
	// nothing, so a reader could reasonably conclude that computing something is
	// almost impossible here. It is an expression, named under `vars:`.
	fmt.Fprintln(&b)
	section(&b, theme, "where values come from")
	fmt.Fprintln(&b, indentBlock(wrap(fmt.Sprintf(
		"Values come from expressions rather than from tasks. Name one with %s: at the top of a "+
			"file, read everywhere as ${%s.<name>}; on a step, read inside it as a bare "+
			"${<name>}. A step's outputs are what it learned from outside, read as "+
			"${%s.<id>.<output>}.", v1.VarsRoot, v1.VarsRoot, v1.StepsRoot), width-2)))

	_, err := io.WriteString(surface.Out, b.String())

	return err
}

// indentBlock puts every line of a wrapped paragraph under its heading.
//
// [indent] is one line, which is what its callers on the help page have. A
// paragraph wrapped to the terminal is several, and indenting only the first puts
// the rest of the sentence back at the margin, where it reads as a new section
// rather than as the continuation it is.
func indentBlock(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = indent(line)
	}

	return strings.Join(lines, "\n")
}

// macroExamplesFor renders the catalog's example calls for the named macros.
//
// Read out of the catalog rather than written into the sentence, so the one line
// explaining how a macro is written cannot name a spelling the schema does not
// carry: the two would drift the first time an example changed, and this line is
// the only place a reader of the terminal listing learns the call form at all.
//
// Silently skips a name with no example, and returns empty if none of them have
// one, which the caller treats as "say nothing". A sentence promising examples and
// then listing none is worse than its absence.
func macroExamplesFor(functions []v1.LibraryFunction, names ...string) string {
	var out []string
	for _, name := range names {
		for _, fn := range functions {
			if fn.Name == name && fn.Example != "" {
				out = append(out, fn.Example)

				break
			}
		}
	}

	return strings.Join(out, ", ")
}

// writeFields prints one task's inputs or outputs, aligned under a label.
//
// A block per task rather than a row per task, which is what this was. `http` has
// eleven inputs; on one line they run past any terminal and take the table's
// alignment with them, so the shape that fits four tasks today would be unusable
// the moment somebody registers a fifth with a real schema. A block is the same
// width whatever the task holds.
//
// The writer is passed in so that a task's inputs and outputs share one, and
// therefore share a column layout. Two tabwriters would align each block against
// itself and against nothing else, which reads as a mistake even when every
// number in it is right.
//
// Required inputs are marked with `*` rather than only sorted first, because a
// mark survives being piped, logged, and read by somebody who cannot see colour.
// Errors are returned rather than discarded, which the tabwriter this replaced did
// by way of Flush. A full disk or a pipe that has gone away makes every write fail,
// and a listing that stopped halfway while reporting success is worse than one that
// says it could not finish.
//
// The width is the terminal's, and what it decides is where the description wraps:
// `method` is `3 to 6 characters, matching ^(?i)(GET|POST|PUT|PATCH|DELETE)$`, which
// is wider than a narrow terminal holds, and a bound cut off at the margin is a
// bound nobody can act on. Wrapped under its own column so the eye stays in it.
func writeFields(w io.Writer, theme ui.Theme, width int, groups []fieldGroup) error {
	// Laid out here rather than by tabwriter, which measures the bytes it is given.
	// A styled cell is mostly escape sequences, so tabwriter counted them as width
	// and every column after the first shifted, so the terminal rendering came out
	// visibly ragged while the piped one, being unstyled, looked fine. Widths are
	// measured with lipgloss, which counts displayed columns, the same way the help
	// page's own two-column lists are laid out.
	const gutter = 2

	var labels, names int
	for _, group := range groups {
		labels = max(labels, lipgloss.Width(group.label))
		for _, field := range group.fields {
			names = max(names, lipgloss.Width(fieldName(field)))
		}
	}

	// Where the description starts, which is also what a wrapped line is indented
	// to: a continuation that started at the margin would read as another field.
	offset := 2 + labels + gutter + names + gutter
	textWidth := max(width-offset, minDescription)

	for _, group := range groups {
		// A group with nothing in it says so, rather than being left out. `log` has
		// no outputs on purpose (a log line is an effect on a reader, not a value a
		// later step reads), and a listing that simply omits the heading leaves an
		// author to wonder whether the task has outputs nobody wrote down.
		if len(group.fields) == 0 {
			if _, err := fmt.Fprintf(w, "  %s%s%s\n",
				theme.Header.Render(group.label), pad(gutter),
				theme.Muted.Render("none")); err != nil {
				return err
			}

			continue
		}

		for i, field := range group.fields {
			// The label sits beside the first row and the rest align under it, so
			// the eye reads down a column rather than hunting for where one list
			// ends.
			// Only the first row of a group carries the label, and an empty one is
			// left unstyled: rendering "" through a style emits escape sequences
			// around nothing, which a terminal ignores and a reader of the raw
			// bytes has to skip past.
			label, styledLabel := "", ""
			if i == 0 {
				label = group.label
				styledLabel = theme.Header.Render(label)
			}

			name := fieldName(field)

			// The type and everything else the schema says about the field, as one
			// description. The bounds were nowhere at all before this: `method` is
			// three to six characters matching a pattern, and the only way to find
			// that out was to read the proto or to run a step that failed.
			describe := append([]string{field.Type}, field.Constraints...)
			if field.Deferred {
				// Marked on the row as well as explained below it, because the row
				// is where somebody deciding what to write in this input is looking.
				describe = append(describe, "evaluated by the task")
			}

			described := strings.Join(describe, ", ")

			lines := strings.Split(wrap(described, textWidth), "\n")

			if _, err := fmt.Fprintf(w, "  %s%s%s%s%s\n",
				styledLabel, pad(labels-lipgloss.Width(label)+gutter),
				theme.Strong.Render(name), pad(names-lipgloss.Width(name)+gutter),
				theme.Muted.Render(lines[0])); err != nil {
				return err
			}

			for _, line := range lines[1:] {
				if _, err := fmt.Fprintf(w, "%s%s\n", pad(offset), theme.Muted.Render(line)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// fieldGroup is one labelled list of a task's fields.
type fieldGroup struct {
	label  string
	fields []v1.InputField
}

// fieldName is how a field is written, with the marker a required one carries.
func fieldName(field v1.InputField) string {
	if field.Required {
		return field.Name + "*"
	}

	return field.Name
}

// pad returns n spaces, never fewer than none.
func pad(n int) string {
	if n < 0 {
		return ""
	}

	return strings.Repeat(" ", n)
}
