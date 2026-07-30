package main

import (
	"context"
	"fmt"
	"strings"

	mango "github.com/muesli/mango-cobra"
	"github.com/muesli/roff"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// execute runs the CLI and draws the two surfaces that belong to a person who has
// not got what they wanted: the help, and the report of an error.
//
// This is the work fang did, minus a terminal query its options could not reach.
// See help.go for why that mattered and what is deliberately different about the
// layout.
//
// The parts of fang worth keeping are kept and they are all small: a hidden `man`
// command over the same mango/roff pair it used, and a version string set here so
// nothing overwrites what the build stamps in. Shell completions need nothing —
// they are cobra's own and are on by default.
func execute(ctx context.Context, root *cobra.Command) error {
	// Both silenced because both are drawn below. Cobra's own error line and usage
	// block would otherwise print alongside, in a different voice, before the
	// report — and `validate` and `fix` already set SilenceUsage for a reason that
	// is really the whole CLI's.
	root.SilenceUsage = true
	root.SilenceErrors = true
	root.Version = version

	root.SetHelpFunc(func(c *cobra.Command, _ []string) {
		renderHelp(newSurface(c), c)
	})

	root.AddCommand(manCommand())

	if err := root.ExecuteContext(ctx); err != nil {
		surface := newSurface(root)
		renderError(surface, err)

		return err
	}

	return nil
}

// manCommand generates a manual page, the way fang did.
//
// Hidden, because it is a build step rather than something to run: a packager pipes
// it into a file at install time and nobody types it twice.
func manCommand() *cobra.Command {
	return &cobra.Command{
		Use:                   "man",
		Short:                 "Generate the manual page",
		Args:                  cobra.NoArgs,
		Hidden:                true,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			page, err := mango.NewManPage(1, cmd.Root())
			if err != nil {
				return fmt.Errorf("build manual page: %w", err)
			}

			_, err = fmt.Fprint(cmd.OutOrStdout(), page.Build(roff.NewDocument()))

			return err
		},
	}
}

// renderError reports why a command did not do what was asked.
//
// Three things it must do, and each of them is a way a report goes wrong. It has to
// be findable, since it arrives after however much output the command already
// produced. It has to survive losing its colour, because it is the line most likely
// to be read out of a CI log. And it must not be the only place the reason exists —
// the error's own text is printed verbatim, never summarised.
//
// Verbatim includes the first letter. An earlier version capitalized it, on the
// reasoning that Go errors are written lower case because they are usually wrapped
// and this is the end of that chain — which is true of prose and wrong of everything
// else an error starts with. `step "web": …` became `Step "web": …`, and a file
// position became `Workflow.yaml:3:1`, which is not a file anybody can search for.
// No heuristic separates the two: the guard tried, and the test that caught it was
// the one asserting the text survives. An author who wants a capital writes one.
// Takes the surface for the same reason [renderHelp] does: there is no writer to
// pass, so there is no wrong one to pass. This path was correct throughout while the
// help path a few lines above was not, which is the argument — nothing about the
// code made the right answer obvious.
func renderError(surface *ui.UI, err error) {
	w := surface.Err
	theme := surface.ErrTheme
	width := surface.ErrCaps.Width

	if err == nil {
		return
	}

	var b strings.Builder

	fmt.Fprintln(&b, theme.Pill(ui.ToneDanger, "error"))
	fmt.Fprintln(&b, wrap(err.Error(), width))

	// A wrong flag or an unknown command is a mistake about the command line
	// itself, and the one case where what to do next is knowable. Everything else
	// failed for its own reasons and a suggestion would be invented.
	if isUsageError(err) {
		fmt.Fprintln(&b)
		fmt.Fprintln(&b, theme.Muted.Render("Try `flow --help` for the commands and flags."))
	}

	fmt.Fprint(w, b.String())
}

// isUsageError reports whether cobra refused the command line rather than the
// command failing.
//
// Matched on the text because cobra does not give these a type — there is nothing
// to match on with errors.As, and an errors.As against an interface every error
// satisfies would look like a check and be none. That is fragile in the direction
// costing least: a miss loses one line of advice, and there is no wrong advice it
// can produce, since every other error prints exactly this report without it.
func isUsageError(err error) bool {
	text := err.Error()
	for _, prefix := range []string{
		"unknown command",
		"unknown flag",
		"unknown shorthand flag",
		"flag needs an argument",
		"invalid argument",
		"accepts ",
		"requires ",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}

	return false
}

// exitCode is the status a failed command leaves behind.
//
// One, always. A command line's exit status is read by `&&` and by CI, and a
// vocabulary of codes is only useful if every caller knows it — which nobody does
// for a tool this size, so the distinctions would be paid for and never read.
const exitCode = 1
