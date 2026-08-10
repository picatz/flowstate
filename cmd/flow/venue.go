package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Where a run runs, said out loud before it runs.
//
// #371 decided the model this file implements, and the two halves of it that
// matter here are that a venue is *configured* and that it is *announced*. The
// first is already true of the grammar: `flow run` means the server venue and
// `flow run local` means this process, and neither ever becomes the other. A
// dial that fails is reported as a server that did not answer (see
// [noServerError]) and never as a reason to execute the workload here, because a
// network blip must not turn a production deploy into a laptop run.
//
// The second is what was missing. The venue was a fact a reader had to infer
// from which words they typed, which flag defaulted from which variable, and
// which shell exported what. A run started against a staging deployment and a
// run started in this process looked identical for the first several lines, and
// the difference only showed up afterwards, in whether anything durable existed.
// One line at the start removes the whole class: there is never a run whose
// venue was a surprise.
//
// # Why the tenant is not named
//
// The decision's sentence is "running on <address> as <tenant>", and the tenant
// is the one word this program cannot honestly write. A caller's namespace is
// derived by the *server*, from the trust policy it loads: an issuer may pin one
// (auth.TrustedIssuer.Namespace), or take it from a claim
// (auth.TrustedIssuer.NamespaceClaim), and the deployment's tenancy mapping then
// places it. A client that read a claim out of its own token and printed it as
// the tenant would be announcing a deployment's decision that it has not been
// told, which is the same mistake CLAUDE.md's diagnostics rule names: report
// what is a property of what you have, and stay silent about what a deployment
// decides.
//
// So the sentence names the identity this command will *present*, which is
// configuration and therefore knowable: nothing, a file, or the variable. That
// answers the question the announcement exists to answer ("is this the shell
// with the production credential in it?") without inventing the half only the
// server knows.

// venue is where a run is about to run, in the shape the announcement needs.
//
// A value rather than two print statements, so that the two drivers cannot drift
// into two spellings of one fact. It is deliberately not a schema type: it never
// crosses a boundary, and there is nothing to serialize until #374's envelope
// grows a field for it.
type venue struct {
	// address is the server this run is being submitted to, exactly as
	// `--address` or FLOWSTATE_ADDRESS spelled it. Empty for the local venue,
	// which contacts nothing.
	address string

	// identity describes the credential this command will present, in the
	// words of the configuration that supplied it. Empty for the local venue,
	// which authenticates to nobody.
	identity string
}

// localVenue is this process.
func localVenue() venue { return venue{} }

// serverVenue is the deployment a run is being submitted to.
//
// getenv is a parameter so the identity half can be exercised from a table
// without a process-wide t.Setenv per case, the same shape [devRefusals] uses
// for the same reason.
func serverVenue(server serverFlags, getenv func(string) string) venue {
	return venue{address: server.address, identity: presentedIdentity(server.tokenFile, getenv)}
}

// presentedIdentity names the credential a request will carry.
//
// The order is [readToken]'s own, because a description that disagreed with what
// is actually sent would be worse than no description: the file wins, then the
// variable, then nothing. The token itself is never read here. Naming a path
// costs no I/O and cannot leak a credential into a terminal, a CI log, or a
// screen recording, and whether the file is readable is the request's report to
// make rather than the announcement's.
func presentedIdentity(tokenFile string, getenv func(string) string) string {
	if tokenFile != "" {
		return "the identity in " + tokenFile
	}

	if getenv("FLOWSTATE_TOKEN") != "" {
		return "the identity in FLOWSTATE_TOKEN"
	}

	return "an anonymous caller"
}

// announce writes the venue line to the account stream.
//
// Three decisions worth stating, because each is a way this line could have
// gone wrong.
//
// It goes to stderr, where the run's own commentary already goes, so the
// document on stdout stays the single JSON value a pipe can read. That is the
// same stream discipline `flow run local`'s status pill and `flow run`'s
// "started <id>" line already follow.
//
// It is printed whatever the output format asked for, unlike those two. They are
// suppressed for a machine format because the document carries the same fact
// inside it, and this one does not yet: #374's envelope grows a venue field when
// it lands. Until then, suppressing it for `-o json` would mean the invocations
// most likely to be running unattended, in a pipeline, against whichever address
// the environment happens to hold, are the only ones with nothing saying where
// their work went.
//
// It is styled the way the CLI states a fact rather than the way it raises an
// alarm: no pill, since a pill is for the one value on a line worth finding at a
// glance and this line is one value long, and the venue itself carries the
// emphasis so a reader scanning a scrollback sees the address rather than the
// verb.
func (v venue) announce(surface *ui.UI) {
	theme := surface.ErrTheme

	if v.address == "" {
		fmt.Fprintf(surface.Err, "%s %s\n", theme.Muted.Render("running"), theme.Strong.Render("locally"))

		return
	}

	fmt.Fprintf(surface.Err, "%s %s %s %s\n",
		theme.Muted.Render("running on"), theme.Strong.Render(v.address),
		theme.Muted.Render("as"), theme.Strong.Render(v.identity))
}

// announceVenue is what the two run verbs call.
//
// A helper taking the command, so neither verb has to remember to build a
// surface early or to read the server flags twice, and so adding a third caller
// is one line rather than a decision about ordering.
func announceVenue(cmd *cobra.Command, v venue) {
	v.announce(newSurface(cmd))
}
