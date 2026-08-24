package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
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
) (*flowdebug.Session, error) {
	switch {
	case machine:
		// A prompt and a document cannot share one stdout: the first
		// `debug>` written into a JSON stream is a document nothing can
		// parse.
		return nil, errors.New("--debug reads commands and prints a prompt on the terminal, and " +
			"--output json writes a document to the same stream; run one or the other")

	case budget.Pinned != nil || budget.Schedules > 0:
		// A seeded exploration runs each case many times under different
		// schedules. Stepping through "the" run of a case that is about to
		// be run ten thousand times is a question with no answer.
		return nil, errors.New("--debug steps through one run, and seeded exploration runs each " +
			"case many times under different schedules; drop --seeds/--seed, or drop --debug")

	case len(files) != 1:
		return nil, fmt.Errorf("--debug drives one console, and %d test files matched; "+
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
		return nil, err
	}
	matched := make([]string, 0, len(file.Tests))
	for _, test := range file.Tests {
		if selectCase == nil || selectCase(test.Name) {
			matched = append(matched, test.Name)
		}
	}
	if len(matched) != 1 {
		return nil, fmt.Errorf("--debug steps through one case, and %d of this file's cases were "+
			"selected: %s. Name one with --run", len(matched), quotedList(matched))
	}

	session, err := flowdebug.New(flowdebug.Options{
		In:  cmd.InOrStdin(),
		Out: surface.Out,
	})
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(surface.Out, "debugging %q — `help` lists the commands\n", matched[0])

	return session, nil
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

// quotedList renders names for a diagnostic, bounded so a pattern that matched
// a hundred cases names a readable few and says how many more.
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
		quoted += fmt.Sprintf("%q", name)
	}
	if quoted == "" {
		return "none"
	}

	return quoted
}
