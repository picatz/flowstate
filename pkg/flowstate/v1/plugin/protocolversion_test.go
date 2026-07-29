package plugin

import (
	"strconv"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// A protocol version names a set of routes, and this is what holds it to that.
//
// The plugin protocol moved from the `flowstate.v1` package to `flowstate.plugin.v1`,
// which rewrote every Connect route it serves. The version number did not move with
// it at first, and that combination is the quiet one: an old plugin offers version 1,
// a new host offers version 1, negotiation agrees, the handshake succeeds — and then
// the first `Describe` goes to a route that plugin does not serve. What an operator
// sees is a plugin that cannot be described, which reads as a broken plugin rather
// than a version mismatch, at the far end of a launch from the thing that was wrong.
//
// Nothing here would have caught it, because both halves moved together: the host
// and the SDK are one repo, so every test built both sides from the same routes and
// agreed with itself. The only witness is a binary compiled before the move, which
// no test in this repo has. So the invariant is checked against the *generated*
// service names instead — the closest thing to that binary's view that a test can
// reach, and one that changes exactly when the routes do.

// TestProtocolVersionNamesItsRoutes fails if the served routes move without the
// version moving with them.
//
// It reads the service names out of the generated Connect package rather than
// repeating them, so the only way to satisfy it after a package move is to bump the
// version — which is the decision the move requires someone to make.
func TestProtocolVersionNamesItsRoutes(t *testing.T) {
	t.Parallel()

	// The package the current version serves from, spelled once. A move changes
	// the generated names below and this expectation has to be updated by hand,
	// which is the moment to ask whether the version should change too.
	const servedPackage = "flowstate.plugin.v1"

	for _, name := range []string{
		pluginv1connect.PluginServiceName,
		pluginv1connect.SecretServiceName,
		pluginv1connect.TaskServiceName,
	} {
		if !strings.HasPrefix(name, servedPackage+".") {
			t.Errorf("service %q is not under %q\n"+
				"  the plugin protocol's routes moved, which makes this a different wire\n"+
				"  bump the protocol version and retire the old one rather than changing what the current number addresses",
				name, servedPackage)
		}
	}

	// Version 2 is what that package is worth. Asserted so the constant cannot be
	// renumbered back to something already spent.
	if got, want := protocol.HostVersions(), []int{protocol.Version2}; len(got) != len(want) || got[0] != want[0] {
		t.Errorf("HostVersions() = %v, want %v", got, want)
	}
}

// TestRetiredProtocolVersionIsNotOffered is the negative direction.
//
// Retiring a version means the host stops offering it, and that is the whole of the
// fix: a plugin built against version 1 then finds nothing in common and refuses at
// startup, naming both sides, instead of negotiating successfully into routes that
// are not there. A test asserting only that version 2 is offered would also pass on
// a host that offered both.
func TestRetiredProtocolVersionIsNotOffered(t *testing.T) {
	t.Parallel()

	for _, v := range protocol.HostVersions() {
		if v == protocol.Version1 {
			t.Errorf("the host offers protocol version %d, which is retired\n"+
				"  its routes were under `flowstate.v1.` and nothing serves them now\n"+
				"  offering it lets a plugin negotiate a version this build cannot answer",
				protocol.Version1)
		}
	}

	// And the retired number stays reserved rather than being reused, for the same
	// reason `Task.description`'s field number is: a number that meant something
	// else must never come back meaning something new.
	if protocol.Version1 == protocol.Version2 {
		t.Errorf("Version1 and Version2 are both %d; a retired version number must not be reused",
			protocol.Version1)
	}
}

// TestNegotiationRefusesARetiredPluginClearly checks what an old plugin actually
// gets, rather than trusting that a missing version produces a good failure.
//
// This is the exchange the bug produced backwards: the point of retiring version 1
// is that the refusal happens here, at negotiation, where both numbers are in hand
// and the message can say so — not later, on a request to a route nobody answers.
func TestNegotiationRefusesARetiredPluginClearly(t *testing.T) {
	t.Parallel()

	// What a plugin built before the move speaks.
	old := []int{protocol.Version1}

	if _, ok := protocol.Negotiate(old, protocol.HostVersions()); ok {
		t.Fatal("a plugin speaking only the retired version negotiated successfully; " +
			"it would then be sent requests to routes it does not serve")
	}

	// A current plugin still negotiates, or the check above would be satisfied by a
	// host that had stopped speaking anything at all.
	got, ok := protocol.Negotiate(protocol.HostVersions(), protocol.HostVersions())
	if !ok {
		t.Fatal("a current plugin failed to negotiate with the host")
	}
	if got != protocol.Version2 {
		t.Errorf("negotiated version = %d, want %d", got, protocol.Version2)
	}

	// The refusal an operator reads names both sides. Checked because the value of
	// failing here rather than on `Describe` is entirely in what it says.
	rendered := protocol.FormatVersions(protocol.HostVersions())
	if !strings.Contains(rendered, strconv.Itoa(protocol.Version2)) {
		t.Errorf("FormatVersions(%v) = %q, which does not name the version the host speaks",
			protocol.HostVersions(), rendered)
	}
}
