package main

import (
	"context"
	"errors"
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

	// Cobra's own did-you-mean machinery (findSuggestions, wired into the
	// `unknown command` error text itself) is unranked and uncapped: see
	// suggest.go's comment on maxSuggestions for what that looked like for
	// `flow lst`. Turned off here so [commandSuggestionError] is the only
	// source of a suggestion, drawn as this CLI's own styled element rather
	// than arriving pre-baked into cobra's error text.
	root.DisableSuggestions = true

	// Inherited by every subcommand: cobra's FlagErrorFunc walks up to the
	// nearest ancestor that set one, so this is the one place an unknown
	// flag gets a suggestion rather than pflag's bare "unknown flag: --adress"
	// with nothing pointing at "--address".
	root.SetFlagErrorFunc(flagErrorFunc)

	root.SetHelpFunc(func(c *cobra.Command, _ []string) {
		renderHelp(newSurface(c), c)
	})

	root.AddCommand(manCommand())

	// Registered here rather than in newRootCommand for the reason `man` is: both
	// are build steps rather than capabilities. That also keeps them out of the
	// command tree the README's pin tests and the generated CLI reference walk,
	// which is the right answer — `flow docs generate` documents the product and
	// is not part of it. See docsgen.go.
	root.AddCommand(newDocsCommand())

	if _, err := root.ExecuteContextC(ctx); err != nil {
		// ExecuteContextC rather than ExecuteContext, so an "unknown command"
		// error can be looked up against root's own subcommand list for a
		// suggestion: see commandSuggestionError. The returned command is
		// otherwise unused: SilenceErrors above is what already stops cobra
		// printing its own report from inside Execute, which is the "whatever
		// double-print problem" this file's own doc comment names; nothing
		// about reading the second return value reopens that.
		if isUsageError(err) && strings.HasPrefix(err.Error(), "unknown command ") {
			err = commandSuggestionError(root, err)
		}

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
		// A ranked candidate, when [flagErrorFunc] or [commandSuggestionError]
		// found one close enough, drawn as its own muted line rather than
		// inside err.Error(), so the error text above stays exactly what
		// cobra or pflag produced. See suggest.go's [suggestedError] doc for
		// why the two are kept apart.
		if line := didYouMean(err); line != "" {
			fmt.Fprintln(&b)
			fmt.Fprintln(&b, theme.Muted.Render(line))
		}

		fmt.Fprintln(&b)
		// The hint is this repo's own prose and goes through the same markup pass
		// the help page does. The error above it deliberately does not: an error's
		// text can carry a line out of somebody's Flowfile, and a backtick in that
		// is theirs rather than markup of ours to eat.
		fmt.Fprintln(&b, wrapProse(theme, theme.Muted, "Try `flow --help` for the commands and flags.", width))
	}

	// An error that knows what to run next says so in the one element this CLI
	// has for that, whatever failed. See [commandBlock].
	if blocks := nextCommandsFor(err); len(blocks) > 0 {
		writeNextCommands(&b, theme, blocks)
	}

	fmt.Fprint(w, b.String())
}

// commandBlock is a group of runnable commands under one optional lead-in.
//
// The shape `flow init`'s NEXT block already prints, moved somewhere both the
// happy path and the sad path can reach: a first block of commands, then any
// number of alternatives each introduced by a muted line saying what makes it a
// different choice rather than a longer one. #327 asked for one way of suggesting
// next commands, and an error that has a remedy is exactly as much a suggestion
// as a scaffold that has just been written.
type commandBlock struct {
	// lead is the muted line above the commands, or empty for the first block,
	// which needs no introduction beyond the heading.
	lead string

	// commands are typed verbatim, in the order somebody would type them.
	commands []string
}

// commandSuggester is an error that knows what to run next.
//
// An interface rather than a check against one concrete type, so the next error
// with a runnable remedy renders identically without editing [renderError],
// which is the whole of what keeps this from becoming a second way of saying the
// same thing.
type commandSuggester interface {
	nextCommands() []commandBlock
}

// nextCommandsFor finds the suggestion anywhere in an error's chain.
//
// Anywhere, because a refusal is routinely wrapped on its way out. `flow watch`
// gives up with the last poll failure inside a sentence of its own, and a remedy
// that disappeared the moment somebody added context would be a remedy nobody
// could rely on.
func nextCommandsFor(err error) []commandBlock {
	var suggester commandSuggester
	if !errors.As(err, &suggester) {
		return nil
	}

	return suggester.nextCommands()
}

// writeNextCommands draws the blocks under the heading, two-space indented per the
// design language's rule for a line subordinate to the one above it.
func writeNextCommands(b *strings.Builder, theme ui.Theme, blocks []commandBlock) {
	fmt.Fprintln(b)
	fmt.Fprintln(b, theme.Accent.Render("NEXT"))

	for i, block := range blocks {
		if block.lead != "" {
			if i > 0 {
				fmt.Fprintln(b)
			}
			fmt.Fprintf(b, "  %s\n", theme.Muted.Render(block.lead))
		}
		for _, command := range block.commands {
			fmt.Fprintf(b, "  %s\n", theme.Strong.Render(command))
		}
	}
}

// usageError marks an error this repo constructs as itself an invocation
// mistake — the command line was wrong and nothing ran — rather than something a
// command discovered about the file or workflow it was given.
//
// The division of labour with the prefix list below is exact and deliberate.
// Cobra's own refusals (an unknown flag, the wrong number of arguments) get no
// type from cobra, so they are matched on the wording it is known to use today —
// fragile in the one direction that costs least, since a miss there only drops a
// line of advice and an exit code, and every prefix is pinned by
// TestCobraUsageErrorsMatchIsUsageError so a cobra upgrade that reworded one is
// caught rather than silently accepted. Nothing this repo constructs may lean on
// that list: a command validating its own flags controls its own wording, so it
// has no excuse to be one string edit away from silently losing its
// classification. It marks itself instead.
//
// Marking, not replacing: Unwrap and Error both forward to the wrapped error
// unchanged, so wrapping never alters the message a person or a script reads —
// only what [isUsageError] answers about it.
//
// The boundary that decides which errors get marked: a rejection of the
// invocation — a flag combination that cannot both be honoured, a value a flag
// does not accept — is marked. A rejection of the *file* a command was pointed
// at — a Flowfile that does not parse, one `flow validate` finds a diagnostic
// in — is never marked, because the command line asking for that check was
// correct; what it found was the answer.
type usageError struct {
	err error
}

// newUsageError marks err as an invocation mistake, or returns nil unchanged —
// mirroring fmt.Errorf and every other error constructor here, so a caller can
// write `return newUsageError(fmt.Errorf(...))` without a separate nil check.
func newUsageError(err error) error {
	if err == nil {
		return nil
	}
	return &usageError{err: err}
}

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

// isUsageError reports whether the command line itself was wrong — cobra
// refused it, or a command's own flag validation did — rather than the command
// running and finding something to refuse.
//
// Typed errors first: anything this repo constructs that means "the invocation
// was wrong" says so with [newUsageError], checked with errors.As so a wrap
// anywhere in the chain is still found. Cobra's own errors carry no type, so
// they fall through to the wording match below — see [usageError]'s doc for why
// that split is exact rather than incidental.
func isUsageError(err error) bool {
	var marked *usageError
	if errors.As(err, &marked) {
		return true
	}

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

// The exit codes a failed command leaves behind, exactly the three docs/CLI.md
// promises: 0 for success (never assigned here, since this file is only reached on
// failure), 1 for a command that ran and found something to refuse, and 2 for a
// command line that was wrong before anything ran.
//
// The distinction earns its keep at exactly one call site — a script or a CI job
// that wants to tell "the workflow failed" from "I typed the flag wrong" apart —
// and costs nothing anywhere else, because [isUsageError] already existed to put
// the one piece of knowable advice in the report. [exitCodeFor] is the same
// classification read a second time rather than a second one computed, which is
// what keeps this from becoming a value that can disagree with the report above it.
const (
	exitCodeFailure = 1
	exitCodeUsage   = 2
)

// exitCodeFor is the status main leaves the process in for a command that returned
// err.
//
// Kept beside [isUsageError] rather than folded into [renderError], because the
// report and the status are two different consumers of one classification — a
// person reads the advice, a script reads the number — and a change to one must
// not silently change the other without a test noticing both.
func exitCodeFor(err error) int {
	if isUsageError(err) {
		return exitCodeUsage
	}
	return exitCodeFailure
}
