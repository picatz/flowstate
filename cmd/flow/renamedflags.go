package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// The flags `flow server` and `flow worker` used to spell without a noun, and
// what to say instead (picatz/flowstate#580).
//
// These are refusals, not aliases. `--address` on these two commands meant
// Temporal's frontend while `--address` on the fifteen verbs that talk to a
// Flowstate server meant the Flowstate server — one spelling with two meanings,
// decided by which command declared it. The failure that ambiguity produces is
// silent: an operator on a platform that hands out an `--address`-shaped port
// variable spells `flow server --address 0.0.0.0:8080`, the server dials that
// as Temporal, binds its own default port, and nothing says why. Accepting the
// old spelling with a warning preserves exactly that failure, which is the
// thing the rename exists to kill, so a pinned command line fails instead —
// before the command does anything, with both replacements named, and with the
// exit status a wrong command line already carries.
//
// Nor is it a deprecation: this repository has no deprecation lifecycle to hang
// one on (picatz/flowstate#722), and "removed in a future release" has no
// referent in a repository with no releases. Registering the name and refusing
// it is what a removal can say that `unknown flag: --address` cannot, and it
// costs one hidden flag rather than a policy nothing else follows.
var renamedTemporalFlags = []renamedFlag{
	{
		old:  "address",
		new:  "temporal-address",
		what: "Temporal's frontend address",
	},
	{
		old:  "namespace",
		new:  "temporal-namespace",
		what: "Temporal's namespace, which is not a Flowstate tenant",
	},
	{
		old:  "profile",
		new:  "temporal-profile",
		what: "a Temporal configuration profile",
	},
}

// renamedFlag is one spelling that is gone and the one that replaced it.
type renamedFlag struct {
	old  string
	new  string
	what string // what the old spelling named, as the object of "named ...".
}

// refusedFlagUsage marks a flag that exists only to be refused.
//
// It is a prefix on the usage string rather than a second table because pflag
// has nowhere else to record it: [TestNoFlagNameCarriesTwoMeanings] walks the
// same cobra tree the docs generator does and has only the flag to read.
const refusedFlagUsage = "removed: "

// addRenamedTemporalFlags registers the old spellings on a command, hidden, and
// arranges for any of them that is actually passed to fail before the command
// runs.
//
// Hidden, so `--help` and docs/reference/cli.md show one spelling per meaning:
// a removed flag documented beside its replacement is the ambiguity again, in
// prose. Registered at all, so cobra parses it and this refusal is what the
// operator reads rather than `unknown flag: --address`, which names neither
// meaning and suggests nothing (the near-miss suggester in cmd/flow/suggest.go
// is bounded at two edits; `address` is ten from `temporal-address`).
func addRenamedTemporalFlags(cmd *cobra.Command) {
	for _, renamed := range renamedTemporalFlags {
		cmd.Flags().String(renamed.old, "", refusedFlagUsage+"say --"+renamed.new+" instead")

		if err := cmd.Flags().MarkHidden(renamed.old); err != nil {
			panic(fmt.Sprintf("marking --%s hidden on %q: %v", renamed.old, cmd.Name(), err))
		}
	}

	// PreRunE rather than RunE's first lines, so that every command sharing
	// this list refuses identically, and so `--help` still renders: cobra runs
	// PreRunE only on the way to RunE.
	if cmd.PreRunE != nil {
		panic(fmt.Sprintf("%q already has a PreRunE; addRenamedTemporalFlags would replace it", cmd.Name()))
	}
	cmd.PreRunE = refuseRenamedTemporalFlags
}

// refuseRenamedTemporalFlags reports the first old spelling this invocation
// passed, as an error naming what it used to mean and every flag that answers
// that question now.
func refuseRenamedTemporalFlags(cmd *cobra.Command, _ []string) error {
	for _, renamed := range renamedTemporalFlags {
		if !cmd.Flags().Changed(renamed.old) {
			continue
		}

		var b strings.Builder
		fmt.Fprintf(&b, "--%s was removed from `%s`: it named %s (picatz/flowstate#580)",
			renamed.old, cmd.CommandPath(), renamed.what)
		for _, remedy := range remediesFor(cmd, renamed) {
			b.WriteString("\n  " + remedy)
		}
		// An invocation mistake: nothing ran, and the command line is what
		// was wrong. Marked rather than left to the wording match, so `flow
		// server --address ...` exits 2 the way `--adress` already does.
		return newUsageError(errors.New(b.String()))
	}

	return nil
}

// remediesFor is what to say instead, on this command.
//
// Read off the command rather than tabulated, because which of them a command
// can offer is a property of the flags it declares: `flow server` binds a
// socket and so has a --listen to be confused with, `flow worker` binds nothing
// and executes one tenant's runs, so each is told about the flag it actually
// has. A table would be the same facts written twice, and the half that drifts
// is always the copy.
func remediesFor(cmd *cobra.Command, renamed renamedFlag) []string {
	remedies := []string{fmt.Sprintf("--%s names %s", renamed.new, renamed.what)}

	switch renamed.old {
	case "address":
		if cmd.Flags().Lookup("listen") != nil {
			remedies = append(remedies, "--listen names the socket this server binds")
		}
	case "namespace":
		if cmd.Flags().Lookup("tenant") != nil {
			remedies = append(remedies, "--tenant names the Flowstate tenant whose runs this worker executes")
		}
	}

	return remedies
}
