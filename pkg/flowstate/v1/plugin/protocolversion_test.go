package plugin

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

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

	// Version 6 is what that package is worth today. Asserted so the constant
	// cannot be renumbered back to something already spent.
	if got, want := protocol.HostVersions(), []int{protocol.Version6}; len(got) != len(want) || got[0] != want[0] {
		t.Errorf("HostVersions() = %v, want %v", got, want)
	}
}

// TestRetiredProtocolVersionIsNotOffered is the negative direction.
//
// Retiring a version means the host stops offering it, and that is the whole of the
// fix: a plugin built against a retired version then finds nothing in common and
// refuses at startup, naming both sides, instead of negotiating successfully into
// routes that are not there — or, for version 2, into a manifest neither side can
// reconstruct. A test asserting only that the current version is offered would also
// pass on a host that offered the retired ones alongside it.
func TestRetiredProtocolVersionIsNotOffered(t *testing.T) {
	t.Parallel()

	retired := map[int]string{
		protocol.Version1: "its routes were under `flowstate.v1.` and nothing serves them now",
		protocol.Version2: "its descriptor exchange assumes flowstate/v1/flowstate.proto, which the twelve-file split replaced",
		protocol.Version3: "it reads the per-launch token from FLOWSTATE_PLUGIN_TOKEN, which the host no longer sets",
		protocol.Version4: "it predates FLOWSTATE_EGRESS_POLICY_B64, so a plugin speaking it reaches the network under no policy at all",
		protocol.Version5: "its netpolicy refuses the deployment_default key the grant now carries, so the whole policy document fails to parse in the plugin",
	}

	for _, v := range protocol.HostVersions() {
		if why, dead := retired[v]; dead {
			t.Errorf("the host offers protocol version %d, which is retired\n"+
				"  %s\n"+
				"  offering it lets a plugin negotiate a version this build cannot answer",
				v, why)
		}
	}

	// And a retired number stays reserved rather than being reused, for the same
	// reason `Task.description`'s field number is: a number that meant something
	// else must never come back meaning something new.
	for _, pair := range [][2]int{
		{protocol.Version1, protocol.Version2},
		{protocol.Version1, protocol.Version3},
		{protocol.Version1, protocol.Version4},
		{protocol.Version1, protocol.Version5},
		{protocol.Version2, protocol.Version3},
		{protocol.Version2, protocol.Version4},
		{protocol.Version2, protocol.Version5},
		{protocol.Version3, protocol.Version4},
		{protocol.Version3, protocol.Version5},
		{protocol.Version4, protocol.Version5},
		{protocol.Version1, protocol.Version6},
		{protocol.Version2, protocol.Version6},
		{protocol.Version3, protocol.Version6},
		{protocol.Version4, protocol.Version6},
		{protocol.Version5, protocol.Version6},
	} {
		if pair[0] == pair[1] {
			t.Errorf("two protocol versions are both %d; a retired version number must not be reused", pair[0])
		}
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

	// What a plugin built before each retirement speaks.
	for _, old := range []int{protocol.Version1, protocol.Version2, protocol.Version3, protocol.Version4, protocol.Version5} {
		if _, ok := protocol.Negotiate([]int{old}, protocol.HostVersions()); ok {
			t.Fatalf("a plugin speaking only retired version %d negotiated successfully; "+
				"it would then be sent requests it cannot answer", old)
		}
	}

	// A current plugin still negotiates, or the check above would be satisfied by a
	// host that had stopped speaking anything at all.
	got, ok := protocol.Negotiate(protocol.HostVersions(), protocol.HostVersions())
	if !ok {
		t.Fatal("a current plugin failed to negotiate with the host")
	}
	if got != protocol.Version6 {
		t.Errorf("negotiated version = %d, want %d", got, protocol.Version6)
	}

	// The refusal an operator reads names both sides. Checked because the value of
	// failing here rather than on `Describe` is entirely in what it says.
	rendered := protocol.FormatVersions(protocol.HostVersions())
	if !strings.Contains(rendered, strconv.Itoa(protocol.Version6)) {
		t.Errorf("FormatVersions(%v) = %q, which does not name the version the host speaks",
			protocol.HostVersions(), rendered)
	}
}

// TestTheSchemaSplitIsRefusedAtTheHandshakeInBothDirections is the reason
// version 3 exists, and it is a *bidirectional* claim, which is what the first
// analysis of the split got wrong.
//
// Splitting flowstate/v1/flowstate.proto into twelve files made a pre-split and
// a post-split build mutually unusable, because each omits from its shipped
// descriptors the files it believes the other already has, and after the split
// they disagree about what those are. Measured, before this bump: the same
// descriptor bytes are accepted by one engine and refused by the other, in both
// directions, with an error naming an import path.
//
// Nothing about that is expressible as a missing route, which is why leaving the
// number at 2 was tempting. It is also why leaving it would have been the worst
// available failure: negotiation agrees, the plugin loads, and the first task
// manifest fails to reconstruct — an operator told everything is fine, then
// handed a descriptor error deep in a launch.
//
// So the claim under test is that neither combination gets that far. Both are
// checked because the reverse direction is the one no diagnostic of ours can
// reach: it fails inside a *host that already shipped*, and the only thing that
// makes it legible there is a version number that host already knows how to
// refuse.
func TestTheSchemaSplitIsRefusedAtTheHandshakeInBothDirections(t *testing.T) {
	t.Parallel()

	preSplit := []int{protocol.Version2}
	postSplit := []int{protocol.Version3}

	// Old plugin, new host: a post-split host offers only 3, and a version 2
	// plugin finds nothing in common and exits before it ever prints a
	// handshake line.
	if _, ok := protocol.Negotiate(postSplit, preSplit); ok {
		t.Error("a pre-split plugin negotiated with a post-split host\n" +
			"  it would load, then fail reconstructing its first task manifest\n" +
			"  with an error about flowstate/v1/flowstate.proto rather than about versions")
	}

	// New plugin, old host: the mirror, and the one that matters most. A host
	// built before this change offers only 2; a plugin built after it speaks only
	// 3, so the plugin refuses at startup naming both numbers. That refusal is
	// reachable precisely because the old host's code already knows how to fail
	// this way — no change we ship today can run inside it.
	if _, ok := protocol.Negotiate(preSplit, postSplit); ok {
		t.Error("a post-split plugin negotiated with a pre-split host\n" +
			"  it would load, then fail reconstructing its first task manifest\n" +
			"  with an error about flowstate/v1/value.proto rather than about versions")
	}

	// And matched builds still work, or the two checks above are satisfied by a
	// protocol that refuses everything.
	if _, ok := protocol.Negotiate(postSplit, postSplit); !ok {
		t.Fatal("two post-split builds failed to negotiate with each other")
	}
}

// TestTheTokenDescriptorIsRefusedAtTheHandshakeInBothDirections is the reason
// version 4 exists, and it is the same bidirectional claim version 3's test
// makes about the schema split.
//
// Moving the per-launch secret out of the environment onto an inherited
// descriptor changes only the launch contract — no route, no message, nothing a
// running plugin serves. That is precisely why leaving the number at 3 would
// have been tempting, and why it would have been wrong: a version 3 plugin looks
// for FLOWSTATE_PLUGIN_TOKEN, which a version 4 host does not set, and a
// version 4 plugin looks for FLOWSTATE_PLUGIN_TOKEN_FD, which a version 3 host
// does not pass. Each build refuses over a variable name, which reads as a
// misconfigured deployment rather than two builds that cannot work together —
// and an implementation less careful than this SDK serves with an empty token
// and rejects every request the host makes.
//
// Both directions are checked because the reverse is the one no diagnostic of
// ours can reach: it fails inside a host that already shipped, and the only
// thing that makes it legible there is a version number that host already knows
// how to refuse.
func TestTheTokenDescriptorIsRefusedAtTheHandshakeInBothDirections(t *testing.T) {
	t.Parallel()

	inEnvironment := []int{protocol.Version3}
	onDescriptor := []int{protocol.Version4}

	// Both sides are named rather than read from HostVersions, because the
	// transition under test is 3 to 4 and the current version has moved past it.
	// A test that tracked HostVersions would keep passing while quietly checking
	// a different bump than the one it documents.

	// Old plugin, new host: the host offers only 4, so a version 3 plugin exits
	// at negotiation rather than looking for a variable that is not there.
	if _, ok := protocol.Negotiate(onDescriptor, inEnvironment); ok {
		t.Error("a plugin expecting the token in the environment negotiated with a host that does not put it there\n" +
			"  it would refuse over a missing FLOWSTATE_PLUGIN_TOKEN, or serve with no token at all,\n" +
			"  rather than over two version numbers")
	}

	// New plugin, old host: the mirror, and the one that matters. A host built
	// before this change offers only 3; a plugin built after it speaks only 4,
	// so the plugin refuses at startup naming both numbers, using the old host's
	// already-shipped negotiation.
	if _, ok := protocol.Negotiate(inEnvironment, onDescriptor); ok {
		t.Error("a plugin expecting the token on a descriptor negotiated with a host that does not pass one\n" +
			"  it would refuse over a missing FLOWSTATE_PLUGIN_TOKEN_FD rather than over two version numbers")
	}

	// And matched builds still negotiate, or both checks above are satisfied by
	// a protocol that refuses everything.
	if _, ok := protocol.Negotiate(onDescriptor, onDescriptor); !ok {
		t.Fatal("two builds delivering the token on a descriptor failed to negotiate with each other")
	}
}

// TestAPluginSpeakingThePreviousVersionIsRefusedAtTheHandshake is what makes the
// launch environment safe to extend.
//
// Version 6 carries what version 5 did not: a grant that is always present under
// `flow` and may say `deployment_default` (#1332). It is not on the wire, so it
// is invisible to a route-shaped compatibility argument — and the failure is the
// quiet one, arriving as somebody else's configuration error: a version 5 binary
// launched by a version 6 host reads a policy document whose new key its own
// strict `netpolicy.ParseConfig` refuses, so it fails in its own startup with a
// parse error while the handshake reported both sides compatible. Closed the
// same way every launch-environment change before it was, by the version
// refusing to negotiate.
//
// The fixture announces [protocol.Version5] rather than an invented number, and
// that matters here more than usual: version 5 is not hypothetical. It shipped
// on main in #1390, before the marker existed, so binaries speaking it are the
// ones a staggered upgrade actually produces. The refusal has to name both
// sides: "version mismatch" with no numbers leaves an operator unable to tell
// which half is old.
func TestAPluginSpeakingThePreviousVersionIsRefusedAtTheHandshake(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "previous-version"))

	// Generous, for the reason TestOpenRefusesBadPlugins records: the refusal
	// under test is one the plugin gives, and a busy machine must not be able to
	// turn it into a timeout.
	cfg.HandshakeTimeout = time.Minute
	cfg.DescribeTimeout = time.Minute

	// A policy is configured, so a host that launched this plugin anyway would
	// be launching it *with* a grant it cannot read — which is the shape of the
	// bug: it would run, and it would run ungoverned.
	cfg.EgressPolicy = []byte("egress:\n  schemes: [https]\n")

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	openErr := host.Open(t.Context())
	if openErr == nil {
		t.Fatal("a plugin speaking the previous protocol version was launched, not refused")
	}
	if !errors.Is(openErr, ErrHandshake) {
		t.Errorf("Open error = %v, want one wrapping ErrHandshake", openErr)
	}

	for _, want := range []string{
		strconv.Itoa(protocol.Version5),
		strconv.Itoa(protocol.Version6),
	} {
		if !strings.Contains(openErr.Error(), want) {
			t.Errorf("Open error = %q, want it to name version %s; a refusal naming one side does not say which build is old",
				openErr.Error(), want)
		}
	}

	// Refused, not admitted with a warning. A plugin the host kept would be one
	// running under a launch environment it does not understand.
	if got := len(host.Plugins()); got != 0 {
		t.Errorf("host holds %d plugins after refusing a version mismatch, want 0", got)
	}
	if got := len(host.TaskDefs()); got != 0 {
		t.Errorf("host offers %d task defs from a refused plugin, want 0", got)
	}
}

// TestTheEgressGrantIsRefusedAtTheHandshakeInBothDirections is the reason
// version 5 exists, in the same bidirectional shape versions 3 and 4 have above.
//
// The grant is a launch-environment change and nothing on the wire, so leaving
// the number at 4 was tempting for exactly the reason it was wrong: version 4
// had already shipped, in #1389, before FLOWSTATE_EGRESS_POLICY_B64 existed. One
// number would then have named two launch contracts — a host that sets the grant
// and a host that does not — and negotiation cannot tell an operator which one
// they have.
//
// What that costs is worse than the earlier bumps, because the missing half is
// an authorization control rather than a credential: a version 4 plugin under a
// version 5 host does not fail, it succeeds, reaching whatever the network
// allows while its operator believes a policy is in force. Nothing later in the
// launch notices. Only the version can.
//
// The reverse direction is the one no diagnostic in this build can reach: it
// fails inside a host that already shipped, and the only thing that makes it
// legible there is a version number that host already knows how to refuse.
func TestTheEgressGrantIsRefusedAtTheHandshakeInBothDirections(t *testing.T) {
	t.Parallel()

	// Both sides are named rather than read from HostVersions, because the
	// transition under test is 4 to 5 and the current version has moved past it
	// (version 6 carries the deployment-default marker). A test that tracked
	// HostVersions would keep passing while quietly checking a later bump than
	// the one it documents.
	ungranted := []int{protocol.Version4}
	granted := []int{protocol.Version5}

	// Old plugin, new host. Without this the plugin serves, and every task it
	// runs reaches the network under no policy on a worker that configured one.
	if _, ok := protocol.Negotiate(granted, ungranted); ok {
		t.Error("a plugin predating the egress grant negotiated with a host that sends one\n" +
			"  it would serve, ungoverned, on a deployment whose operator configured a policy\n" +
			"  rather than refusing over two version numbers")
	}

	// New plugin, old host: the mirror. A host built before the grant offers
	// only 4; a plugin built after it speaks only 5, so the plugin refuses at
	// startup naming both numbers, using the old host's already-shipped
	// negotiation.
	if _, ok := protocol.Negotiate(ungranted, granted); ok {
		t.Error("a plugin expecting the egress grant negotiated with a host that does not send one\n" +
			"  it would refuse over a missing FLOWSTATE_EGRESS_POLICY_B64 rather than over two version numbers")
	}

	// And matched builds still negotiate, or both checks above are satisfied by
	// a protocol that refuses everything.
	if _, ok := protocol.Negotiate(granted, granted); !ok {
		t.Fatal("two builds carrying the egress grant failed to negotiate with each other")
	}
}

// TestTheDeploymentDefaultMarkerIsRefusedAtTheHandshakeInBothDirections is the
// reason version 6 exists, in the same bidirectional shape versions 3, 4 and 5
// have above.
//
// Marking the grant looks additive, and that is the trap. `deployment_default`
// is a key in the policy document, and netpolicy's parser is strict on purpose —
// an unknown key is an error so a misspelled rule cannot silently drop a
// restriction — so a version 5 plugin does not ignore the new key, it refuses
// the whole document. Under a worker upgraded ahead of its plugins, an existing
// version 5 `sql` or `slack` binary fails in its own startup with a policy parse
// error, while the handshake told the operator both sides were compatible.
//
// The alternative encoding is what makes the number rather than a different
// spelling the answer: a marker beside the document would leave that same
// version 5 plugin parsing the deployment default as though an operator had
// written it, so `sql` would open a database connection on a worker that
// authorized no destination — a posture widening on binaries this repository
// cannot see. A refusal at the handshake is the failure to prefer.
//
// The reverse direction is the one no diagnostic in this build can reach: it
// fails inside a host that already shipped, and the only thing that makes it
// legible there is a version number that host already knows how to refuse.
func TestTheDeploymentDefaultMarkerIsRefusedAtTheHandshakeInBothDirections(t *testing.T) {
	t.Parallel()

	unmarked := []int{protocol.Version5}
	marked := []int{protocol.Version6}

	// Old plugin, new host. Without this the plugin loads and then refuses its
	// own launch over a policy document it cannot parse, which reads as a broken
	// policy file rather than as two builds that cannot work together.
	if _, ok := protocol.Negotiate(marked, unmarked); ok {
		t.Error("a plugin predating the deployment-default marker negotiated with a host that sends one\n" +
			"  it would fail parsing the grant, reporting an operator's policy as malformed\n" +
			"  rather than refusing over two version numbers")
	}

	// New plugin, old host: the mirror. A host built before the marker offers
	// only 5; a plugin built after it speaks only 6, so the plugin refuses at
	// startup naming both numbers, using the old host's already-shipped
	// negotiation.
	if _, ok := protocol.Negotiate(unmarked, marked); ok {
		t.Error("a plugin expecting a marked grant negotiated with a host that never sends one\n" +
			"  it would read the deployment default as an operator's policy rather than refusing over two version numbers")
	}

	// And matched builds still negotiate, or both checks above are satisfied by
	// a protocol that refuses everything.
	if _, ok := protocol.Negotiate(marked, marked); !ok {
		t.Fatal("two builds carrying the marked grant failed to negotiate with each other")
	}
}
