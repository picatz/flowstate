package main

import (
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The nouns picatz/flowstate#580 settled, and the property that makes each one
// checkable rather than merely stated.
//
// The rule the issue arrived at is one sentence: **a flag names the noun it
// belongs to.** Temporal's settings carry --temporal-, a socket this process
// binds is --listen, a place this process dials is --address, and a Flowstate
// tenant is --tenant. What made that worth a mechanism is that the version of
// the mistake this repository shipped was invisible to every existing test:
// --address was declared on seventeen commands and meant Temporal's frontend on
// two of them, which no test of any one command can see — the defect is a
// property of the *set*.
//
// So each entry pairs the meaning with something a walk of the cobra tree can
// decide about any command declaring that name. A prose-only table would be the
// same drift the issue is about, one layer up: a sentence nothing reads.
var flagMeanings = []flagMeaning{
	{
		name:    "address",
		means:   "the Flowstate server a client dials",
		holdsOn: declaresToo("token-file"),
		because: "--address names a server to dial, and every command that dials one takes " +
			"the credential flags beside it (addServerFlags, cmd/flow/client.go). A command " +
			"declaring --address without them is naming something else — which is what " +
			"`flow server --address` did, naming Temporal's frontend",
	},
	{
		name:    "listen",
		means:   "a socket this process binds",
		holdsOn: declaresNot("address"),
		because: "a command that binds a socket must not also spell something --address: " +
			"that is the collision #580 is about, and the failure it produces is silent — " +
			"the process comes up on its default port and nothing says why",
	},
	{
		name:    "temporal-address",
		means:   "the Temporal frontend this process dials",
		holdsOn: declaresNot("address"),
		because: "the prefix exists to keep the two apart; a command carrying both spellings " +
			"has re-introduced the ambiguity rather than resolved it",
	},
	{
		name:    "temporal-namespace",
		means:   "Temporal's namespace",
		holdsOn: declaresNot("namespace"),
		because: "`namespace` unprefixed named five different things across this CLI (#580, and " +
			"#568 one layer down in the CEL vocabulary); Temporal's is spelled with Temporal's " +
			"prefix, and a Flowstate tenant is --tenant",
	},
	{
		name:    "temporal-profile",
		means:   "a Temporal configuration profile",
		holdsOn: declaresNot("profile"),
		because: "same rule: Temporal's settings carry Temporal's prefix",
	},
}

// flagMeaning is one flag name, the single thing it names, and the property that
// has to hold wherever it is declared.
type flagMeaning struct {
	name    string
	means   string
	holdsOn func(cmd *cobra.Command) bool
	because string
}

// declaresToo is satisfied by a command that also declares other.
func declaresToo(other string) func(*cobra.Command) bool {
	return func(cmd *cobra.Command) bool { return cmd.Flags().Lookup(other) != nil }
}

// declaresNot is satisfied by a command that does not declare other — or that
// declares it only to refuse it, which is what a removed spelling is.
func declaresNot(other string) func(*cobra.Command) bool {
	return func(cmd *cobra.Command) bool {
		flag := cmd.Flags().Lookup(other)
		return flag == nil || strings.HasPrefix(flag.Usage, refusedFlagUsage)
	}
}

// TestNoFlagNameCarriesTwoMeanings walks the command tree and checks every
// declaration of a named flag against the one thing that name means.
//
// The failure message is the table entry, so the fix is a rename or a
// deliberate edit here — not a shrug.
func TestNoFlagNameCarriesTwoMeanings(t *testing.T) {
	for _, meaning := range flagMeanings {
		t.Run(meaning.name, func(t *testing.T) {
			declaredOn := commandsDeclaring(t, meaning.name)
			require.NotEmpty(t, declaredOn,
				"nothing declares --%s any more; delete its entry from flagMeanings rather "+
					"than leaving a rule about a flag that does not exist", meaning.name)

			for _, cmd := range declaredOn {
				assert.True(t, meaning.holdsOn(cmd),
					"`%s` declares --%s, which names %s: %s",
					cmd.CommandPath(), meaning.name, meaning.means, meaning.because)
			}
		})
	}
}

// TestTemporalSettingsAreSpelledWithTemporalsPrefix is the positive and negative
// halves of the rename, on the two commands that dial Temporal.
//
// The negative direction is the one that matters, and it is the direction
// CLAUDE.md asks for: not "--temporal-address exists", which a tree that kept
// both spellings would satisfy, but "the unprefixed spelling is gone from the
// surface a reader judges".
func TestTemporalSettingsAreSpelledWithTemporalsPrefix(t *testing.T) {
	for _, path := range []string{"server", "worker"} {
		t.Run(path, func(t *testing.T) {
			cmd := findCommand(t, path)

			for _, renamed := range renamedTemporalFlags {
				replacement := cmd.Flags().Lookup(renamed.new)
				require.NotNil(t, replacement,
					"`flow %s` has no --%s; Temporal's settings carry Temporal's prefix (#580)",
					path, renamed.new)
				assert.False(t, replacement.Hidden, "--%s is the spelling operators are meant to find", renamed.new)

				old := cmd.Flags().Lookup(renamed.old)
				require.NotNil(t, old,
					"--%s is gone entirely from `flow %s`, so a command line written against it "+
						"gets cobra's `unknown flag`, which names neither the old meaning nor the "+
						"new spelling; keep it registered and refusing (cmd/flow/renamedflags.go)",
					renamed.old, path)
				assert.True(t, old.Hidden,
					"--%s is refused, so it must not appear in `flow %s --help` or in "+
						"docs/reference/cli.md beside the spelling that replaced it",
					renamed.old, path)
			}
		})
	}
}

// TestRemovedFlagSpellingsRefuseAndSayWhatToSayInstead is the diagnostic, which
// is the whole reason the old names are still registered.
//
// A warning would not do: continuing to accept `flow server --address` while
// meaning Temporal is exactly the silent misconfiguration the rename exists to
// prevent, so the refusal is an error, at parse time, naming every flag that
// answers the question now.
func TestRemovedFlagSpellingsRefuseAndSayWhatToSayInstead(t *testing.T) {
	cases := []struct {
		args    []string
		mention []string
	}{
		{
			args:    []string{"server", "--address", "0.0.0.0:8080"},
			mention: []string{"--address was removed", "--temporal-address", "--listen"},
		},
		{
			args:    []string{"worker", "--address", "temporal:7233"},
			mention: []string{"--address was removed", "--temporal-address"},
		},
		{
			args:    []string{"worker", "--namespace", "production"},
			mention: []string{"--namespace was removed", "--temporal-namespace", "--tenant"},
		},
		{
			args:    []string{"server", "--profile", "prod"},
			mention: []string{"--profile was removed", "--temporal-profile"},
		},
	}

	for _, tc := range cases {
		t.Run(strings.Join(tc.args[:2], " "), func(t *testing.T) {
			result := runFlow(t, tc.args...)

			require.Error(t, result.Err, "the removed spelling was accepted")
			for _, want := range tc.mention {
				assert.Contains(t, result.Err.Error(), want)
			}
			assert.Equal(t, exitCodeUsage, exitCodeFor(result.Err),
				"a command line naming a flag that no longer exists is an invocation mistake, "+
					"which docs/CLI.md promises exits 2")
		})
	}

	// `flow worker` binds no socket, so the remedy that belongs to `flow
	// server` must not be offered there — a suggestion naming a flag the
	// command does not have is a false diagnostic, which CLAUDE.md holds is
	// worse than a missing one.
	assert.NotContains(t, runFlow(t, "worker", "--address", "temporal:7233").Err.Error(), "--listen")
}

// TestNoVisibleFlagIsARemovedSpelling holds the convention the two tests above
// depend on: a flag registered only to be refused is hidden everywhere, so
// `--help` and docs/reference/cli.md show one spelling per meaning.
func TestNoVisibleFlagIsARemovedSpelling(t *testing.T) {
	eachCommand(newRootCommand(), func(cmd *cobra.Command) {
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			if strings.HasPrefix(f.Usage, refusedFlagUsage) {
				assert.True(t, f.Hidden,
					"`%s` documents --%s, which exists only to be refused; a removed spelling "+
						"printed beside the one that replaced it is the ambiguity again, in prose",
					cmd.CommandPath(), f.Name)
			}
		})
	})
}

// commandsDeclaring collects every command in the tree that declares a flag of
// its own by that name, refusals excepted.
func commandsDeclaring(t *testing.T, name string) []*cobra.Command {
	t.Helper()

	var declaredOn []*cobra.Command
	eachCommand(newRootCommand(), func(cmd *cobra.Command) {
		flag := cmd.LocalFlags().Lookup(name)
		if flag == nil || strings.HasPrefix(flag.Usage, refusedFlagUsage) {
			return
		}
		declaredOn = append(declaredOn, cmd)
	})

	sort.Slice(declaredOn, func(i, j int) bool {
		return declaredOn[i].CommandPath() < declaredOn[j].CommandPath()
	})

	return declaredOn
}
