package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// debugSession builds the session `flow test --debug` runs one case under, or
// refuses the combination it was asked for.
//
// Every refusal here is a shape where "interactive" stops being true of the
// run, and each says which two things disagree rather than resolving one in
// the other's favour — the same rule [scheduleBudget] states for the seed
// flags: a flag that silently does nothing is the same failure as a check that
// silently does not run.
func debugSession(
	cmd *cobra.Command,
	surface *ui.UI,
	machine bool,
	budget dst.Budget,
	files []string,
	selectCase func(string) bool,
) (*flowdebug.Session, func(), error) {
	// Returned beside every refusal below as well as beside a session, so a
	// caller can defer it unconditionally: a cleanup a caller has to remember
	// only on the success path is one that eventually gets forgotten, and the
	// thing forgotten here is somebody's terminal left in raw mode.
	nothingToRestore := func() {}

	switch {
	case machine:
		// A prompt and a document cannot share one stdout: the first
		// `debug>` written into a JSON stream is a document nothing can
		// parse.
		return nil, nothingToRestore, errors.New("--debug reads commands and prints a prompt on the terminal, and " +
			"--output json writes a document to the same stream; run one or the other")

	case budget.Pinned != nil || budget.Schedules > 0:
		// A seeded exploration runs each case many times under different
		// schedules. Stepping through "the" run of a case that is about to
		// be run ten thousand times is a question with no answer.
		return nil, nothingToRestore, errors.New("--debug steps through one run, and seeded exploration runs each " +
			"case many times under different schedules; drop --seeds/--seed, or drop --debug")

	case len(files) != 1:
		return nil, nothingToRestore, fmt.Errorf("--debug drives one console, and %d test files matched; "+
			"name the one file to debug", len(files))
	}

	// Exactly one case, established by reading the file rather than by
	// hoping: `--run` takes a regular expression, and a pattern matching
	// three cases under a debugger would step through three runs with no
	// way to tell which one is speaking. Loading the file twice — here and
	// again inside the run — is a cost worth paying once, interactively, to
	// give a diagnostic that names the number.
	file, err := flowtest.Load(files[0])
	if err != nil {
		return nil, nothingToRestore, err
	}
	matched := make([]string, 0, len(file.Tests))
	var only flowtest.Test
	for _, test := range file.Tests {
		if selectCase == nil || selectCase(test.Name) {
			matched = append(matched, test.Name)
			only = test
		}
	}
	if len(matched) != 1 {
		return nil, nothingToRestore, fmt.Errorf("--debug steps through one case, and %d of this file's cases were "+
			"selected: %s. Name one with --run", len(matched), quotedList(matched))
	}

	console, out, restore := debugConsoleFor(cmd.InOrStdin(), surface.Out, surface.Theme)

	session, err := flowdebug.New(flowdebug.Options{
		In:      cmd.InOrStdin(),
		Console: consoleOrNil(console),
		Out:     out,
		// The session's tones through the one theme the transcript already
		// renders with, so a paused run and a failed run's account read as
		// one product. A non-terminal stream resolves every style to a
		// no-op, so machine-ish captures see the same bytes as before.
		Emit: debugEmitter(out, surface.Theme),
		// The step ids `break` and `until` complete over, read from the
		// workflow this case runs — which is a second file read on an
		// interactive path, and worth it for the same reason the test file is
		// read twice above: a breakpoint is for a step the run has not reached
		// yet, so a prompt that could only offer the ones already seen would be
		// no help at exactly the moment somebody wants one.
		//
		// A workflow that does not parse contributes nothing and says nothing:
		// the run is about to report that properly, with positions, and a
		// second complaint from the completer would be the same news told worse.
		Steps: workflowStepIDs(flowtest.WorkflowPath(files[0], &only)),
	})
	if err != nil {
		restore()

		return nil, nothingToRestore, err
	}
	if console != nil {
		console.SetCompleter(session.Complete)
	}

	fmt.Fprintf(out, "%s\n", surface.Theme.Accent.Render(
		fmt.Sprintf("debugging %q — `help` lists the commands", matched[0])))

	return session, restore, nil
}

// debugConsoleFor attaches a terminal line editor where stdin is a terminal,
// and answers with the plain writer where it is not.
//
// The writer comes back with the console because the two are one decision: a
// console owns the line, so everything the session prints while one is attached
// has to go through it. Splitting them would make it possible to attach a
// console and keep writing around it, which is a screen nobody can read.
func debugConsoleFor(in io.Reader, out io.Writer, theme ui.Theme) (console *debugConsole, writer io.Writer, restore func()) {
	console, restore, ok := attachDebugConsole(in, out, theme)
	if !ok {
		return nil, out, func() {}
	}

	return console, console, restore
}

// consoleOrNil hands a console to [flowdebug.Options] as the interface it
// implements, and a genuinely nil interface when there is none.
//
// Written out for the reason [debuggerOrNil] is: a nil *debugConsole stored in
// a [flowdebug.Console] field is not a nil interface, and the session would
// prompt through it — calling a method on nothing at the first breakpoint of
// every run that is not at a terminal, which is every scripted one.
func consoleOrNil(console *debugConsole) flowdebug.Console {
	if console == nil {
		return nil
	}

	return console
}

// workflowStepIDs is [stepIDs] for a workflow this command has not parsed yet.
//
// Errors are deliberately silent — see the call site.
func workflowStepIDs(path string) []string {
	workflow, _, err := flowfile.ParseFile(path)
	if err != nil {
		return nil
	}

	return stepIDs(workflow)
}

// stepIDs are the ids of every step a workflow declares, for completing
// `break` and `until`.
//
// Nested steps included, which is the point of walking rather than ranging over
// `steps:`: a breakpoint on a step inside a `for_each` body is the one somebody
// sets most, because a loop is where a run stops being easy to read.
func stepIDs(workflow *v1.Workflow) []string {
	var ids []string
	v1.WalkWorkflow(workflow, v1.Walk{Node: func(node *v1.Node) {
		if id := node.GetId(); id != "" {
			ids = append(ids, id)
		}
	}})

	return ids
}

// debugEmitter maps the session's tone vocabulary onto the theme:
//
//   - a break line is the product's own voice (Accent), the heading a reader
//     scans for;
//   - the prompt recedes (Muted) so the run's account stands out;
//   - warnings and dangers take the same two styles the transcript's
//     [flowtest.ToneWarning] and [flowtest.ToneDanger] take in
//     printTranscript, because a tolerated failure must look identical
//     whether an author meets it live at a breakpoint or afterward in a
//     failing case's account;
//   - the account itself stays plain, matching the transcript.
//
// Styling is applied to the fragment minus its trailing newline, so the
// escape sequences never wrap a line break.
func debugEmitter(out io.Writer, theme ui.Theme) func(string, flowdebug.Tone) {
	return func(text string, tone flowdebug.Tone) {
		trimmed, hadNewline := strings.CutSuffix(text, "\n")
		switch tone {
		case flowdebug.ToneBreak:
			trimmed = theme.Accent.Render(trimmed)
		case flowdebug.TonePrompt:
			trimmed = theme.Muted.Render(trimmed)
		case flowdebug.ToneWarning:
			trimmed = theme.Warning.Render(trimmed)
		case flowdebug.ToneDanger:
			trimmed = theme.Danger.Render(trimmed)
		}
		if hadNewline {
			fmt.Fprintln(out, trimmed)
			return
		}
		fmt.Fprint(out, trimmed)
	}
}

// debuggerOrNil hands a session to [flowtest.RunOptions] as the interface it
// implements, and a genuinely nil interface when there is no session.
//
// Written out rather than assigning the pointer directly because a nil
// *flowdebug.Session stored in a v1.Debugger field is not a nil interface: it
// is a non-nil interface holding a nil pointer, which passes every `!= nil`
// check the engine makes and then calls a method on nothing. The bug this
// avoids is a run that is not being debugged panicking at its first step.
func debuggerOrNil(session *flowdebug.Session) v1.Debugger {
	if session == nil {
		return nil
	}

	return session
}

// maxDiagnosticNameRunes bounds one name rendered into a diagnostic.
//
// The count bound below is not enough on its own: five names is five names of
// whatever length the file gave them, and a submitted document reaches this
// through `flowstate_debug`'s refusals, where an error is an answer with a
// byte budget (Codex, #1109). Long enough that no real case name is touched.
const maxDiagnosticNameRunes = 120

// capForDiagnostic shortens one name for a message, saying that it did.
func capForDiagnostic(name string) string {
	runes := []rune(name)
	if len(runes) <= maxDiagnosticNameRunes {
		return name
	}

	return string(runes[:maxDiagnosticNameRunes]) + fmt.Sprintf("… (%d more)", len(runes)-maxDiagnosticNameRunes)
}

// quotedList renders names for a diagnostic, bounded so a pattern that matched
// a hundred cases names a readable few and says how many more, and so that one
// very long name cannot be most of a message.
func quotedList(names []string) string {
	const show = 5

	quoted := ""
	for i, name := range names {
		if i == show {
			return fmt.Sprintf("%s and %d more", quoted, len(names)-show)
		}
		if i > 0 {
			quoted += ", "
		}
		quoted += fmt.Sprintf("%q", capForDiagnostic(name))
	}
	if quoted == "" {
		return "none"
	}

	return quoted
}
