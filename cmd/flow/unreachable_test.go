package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/watch"
)

// The first sad path anybody meets is the one where no server is running, and for
// a long time it was the least helpful thing this CLI said: one sentence written
// down four times, offering only an address to point somewhere else, and missing
// entirely from `flow run`, the command most likely to be typed first.
//
// What follows holds the fix in place from both directions. The sentence has to be
// the same sentence whichever verb dialled, and the remedies have to be the ones
// that apply to that verb and that address rather than a list of everything the
// CLI knows how to say.

// refusedAddress returns a loopback address with nothing listening on it.
//
// A port bound and immediately released rather than a number picked out of the
// air: an arbitrary port might be somebody else's server on a shared machine, and
// a test that dials it would be asserting about their process. Loopback also
// refuses instantly, so every dial below fails in microseconds rather than waiting
// out a timeout.
func refusedAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "could not reserve a port to then close")

	address := listener.Addr().String()
	require.NoError(t, listener.Close(), "could not close the reserved port")

	return address
}

// unavailable is a refusal carrying the code a dial failure arrives as.
func unavailable() error {
	return connect.NewError(connect.CodeUnavailable,
		errors.New("dial tcp [::1]:9233: connect: connection refused"))
}

// TestOneSentenceForAServerThatDidNotAnswer is the one-constant rule applied to
// error prose.
//
// Every path that translates a CodeUnavailable is asked the same question with the
// same address, and every answer has to be the same string. Written against the
// translating functions rather than a grep for the sentence, because the property
// is that the callers agree, and a fifth caller inventing its own wording fails
// here the moment it is added to the list.
func TestOneSentenceForAServerThatDidNotAnswer(t *testing.T) {
	t.Parallel()

	server := serverFlags{address: "localhost:9233"}

	answers := map[string]string{
		"refusedRun":          refusedRun("reading", "flowstate-workflow-3f7c", server, unavailable()).Error(),
		"refusedList":         refusedList(server, unavailable()).Error(),
		"refusedSchedule":     refusedSchedule("describing", "nightly", server, unavailable()).Error(),
		"refusedScheduleList": refusedScheduleList(server, unavailable()).Error(),
		"refusedStart":        refusedStart("../../examples/hello-world/workflow.yaml", "hello-world", nil, server, unavailable()).Error(),
	}

	want := answers["refusedList"]
	for name, got := range answers {
		assert.Equal(t, want, got,
			"%s spells the unreachable-server sentence its own way; there is one of it", name)
	}

	assert.Contains(t, want, "localhost:9233",
		"the sentence does not name the address that was dialled")
	assert.Contains(t, want, "--address",
		"the sentence does not name the flag that points somewhere else")
	assert.Contains(t, want, "FLOWSTATE_ADDRESS",
		"the sentence does not name the variable that points somewhere else")
	assert.Contains(t, want, "connection refused",
		"the dial failure underneath was summarised away")
}

// TestEveryDialingVerbReportsTheSameUnreachableSentence drives the real commands.
//
// The check above proves the translating functions agree; this proves the verbs
// actually call them. `flow run` is in the list because it used to report a bare
// `starting hello-world: unavailable: …` with no address, no variable and no way
// out, which is the whole of #370's third problem.
func TestEveryDialingVerbReportsTheSameUnreachableSentence(t *testing.T) {
	t.Parallel()

	address := refusedAddress(t)

	for _, verb := range []struct {
		name string
		args []string
	}{
		{"list", []string{"list"}},
		{"get", []string{"get", "flowstate-workflow-3f7c"}},
		{"cancel", []string{"cancel", "flowstate-workflow-3f7c"}},
		{"terminate", []string{"terminate", "flowstate-workflow-3f7c"}},
		{"signal", []string{"signal", "flowstate-workflow-3f7c", "approve"}},
		{"schedule list", []string{"schedule", "list"}},
		{"schedule describe", []string{"schedule", "describe", "nightly"}},
		{"schedule delete", []string{"schedule", "delete", "nightly"}},
		{"run", []string{"run", "../../examples/hello-world/workflow.yaml"}},
	} {
		t.Run(verb.name, func(t *testing.T) {
			err := runVerbAgainst(t, address, verb.args)

			report := reportOf(t, err)
			assert.Contains(t, report, "no Flowstate server answered at "+address,
				"%v did not report the unreachable server in the CLI's one spelling", verb.args)

			assert.Equal(t, exitCodeFailure, exitCodeFor(err),
				"a server that did not answer was classified as a mistake about the command line")
		})
	}
}

// TestOnlyRunShapedVerbsOfferLocalRehearsal is the direction #370 asked for by
// name, and it is the one worth writing.
//
// `flow run local` is an answer to "I never wanted a server", which is a sensible
// thing to tell somebody who was starting a workflow and nothing to tell somebody
// who asked what their runs are doing. A listing has no local twin, and offering
// one would send a reader to a command that cannot answer their question. Both
// halves are asserted, because only the second one can fail quietly.
func TestOnlyRunShapedVerbsOfferLocalRehearsal(t *testing.T) {
	t.Parallel()

	address := refusedAddress(t)

	t.Run("run offers it", func(t *testing.T) {
		report := reportOf(t, runVerbAgainst(t, address,
			[]string{"run", "../../examples/hello-world/workflow.yaml"}))

		assert.Contains(t, report, "flow run local ../../examples/hello-world/workflow.yaml",
			"`flow run` did not offer to rehearse the file it was given without a server")
	})

	for _, verb := range [][]string{
		{"list"},
		{"get", "flowstate-workflow-3f7c"},
		{"cancel", "flowstate-workflow-3f7c"},
		{"terminate", "flowstate-workflow-3f7c"},
		{"signal", "flowstate-workflow-3f7c", "approve"},
		{"schedule", "list"},
		{"schedule", "describe", "nightly"},
	} {
		t.Run(strings.Join(verb, " ")+" does not", func(t *testing.T) {
			report := reportOf(t, runVerbAgainst(t, address, verb))

			assert.NotContains(t, report, "flow run local",
				"%v offered a local rehearsal, which cannot answer what it was asked", verb)
		})
	}
}

// TestTheWayOutLeadsWithTheDevStack pins the order the remedies are offered in.
//
// `flow server dev` first because since #377 it is one command rather than three
// terminals, and because a refusal from this machine overwhelmingly means no
// server exists yet rather than that the caller meant a different one. The second
// line is the verb they already wanted, so the block is the whole path from here
// to a durable run rather than an instruction to go and read about one.
func TestTheWayOutLeadsWithTheDevStack(t *testing.T) {
	t.Parallel()

	report := reportOf(t, refusedStart("pipeline/workflow.yaml", "pipeline", nil,
		serverFlags{address: "localhost:9233"}, unavailable()))

	assert.Contains(t, report, "NEXT",
		"the remedies are not drawn as this CLI's one way of suggesting next commands")

	dev := strings.Index(report, "flow server dev")
	durable := strings.Index(report, "flow run pipeline/workflow.yaml")
	local := strings.Index(report, "flow run local pipeline/workflow.yaml")

	require.Positive(t, dev, "the dev stack was not offered")
	require.Positive(t, durable, "the run that follows the dev stack was not offered")
	require.Positive(t, local, "the local rehearsal was not offered")

	assert.Less(t, dev, durable, "the two-command durable path is out of order")
	assert.Less(t, durable, local,
		"the local rehearsal came before the durable path; rehearsing is the alternative, not the lead")
}

// TestARemoteAddressIsOfferedNoLocalRemedies is the negative direction of the
// same rule.
//
// Both remedies are about this machine. A staging deployment that is down is not
// fixed by starting a dev stack here, and saying so would be answering a question
// nobody asked. The address hint in the sentence is the lead there, which is
// where the sentence already puts it.
func TestARemoteAddressIsOfferedNoLocalRemedies(t *testing.T) {
	t.Parallel()

	for _, address := range []string{
		"flowstate.internal:9233",
		"https://flowstate.example.com",
		"10.0.0.7:9233",
	} {
		t.Run(address, func(t *testing.T) {
			report := reportOf(t, unreachableServer(serverFlags{address: address},
				"pipeline/workflow.yaml", unavailable()))

			assert.Contains(t, report, "no Flowstate server answered at "+address,
				"a remote refusal lost the sentence")
			assert.NotContains(t, report, "NEXT",
				"a deployment somewhere else was answered with commands about this machine")
			assert.NotContains(t, report, "flow server dev",
				"a deployment somewhere else was told to start a dev stack here")
		})
	}
}

// TestTheWayOutSurvivesBeingWrapped covers how the refusal actually arrives.
//
// A watch that gave up reports the last poll failure inside a sentence of its own,
// through [transientError] and one more wrap. A remedy that only rendered at the
// top of a chain would be a remedy nobody watching a run ever saw.
func TestTheWayOutSurvivesBeingWrapped(t *testing.T) {
	t.Parallel()

	refusal := unreachableServer(serverFlags{address: "localhost:9233"}, "", unavailable())

	gaveUp := fmt.Errorf("gave up watching %q after 30s of the server being unable to answer: %w",
		"flowstate-workflow-3f7c", watch.NewTransientError(refusal))

	assert.NotEmpty(t, nextCommandsFor(gaveUp),
		"the way out was lost behind the sentence that wrapped it")
	assert.Contains(t, reportOf(t, gaveUp), "flow server dev",
		"a watch that gave up because nothing answered offered no way out")
}

// runVerbAgainst executes one verb against an address nothing is listening on and
// returns the refusal.
func runVerbAgainst(t *testing.T, address string, args []string) error {
	t.Helper()

	err := runFlow(t, append(append([]string{}, args...), "--address", address)...).Err
	require.Error(t, err, "%v somehow succeeded against an address with nothing on it", args)

	return err
}

// TestASchemedLoopbackAddressStillGetsTheWayOut is the review finding: the
// supported spelling `--address http://localhost:9233` must not read as remote
// just because the scheme defeats a bare host-port parse.
func TestASchemedLoopbackAddressStillGetsTheWayOut(t *testing.T) {
	for _, address := range []string{
		"http://localhost:9233",
		"https://127.0.0.1:9233",
		"http://[::1]:9233",
	} {
		assert.True(t, isLoopbackAddress(address), "%s is this machine, spelled with a scheme", address)
	}
	for _, address := range []string{
		"http://flowstate.internal:9233",
		"https://10.0.0.4:9233",
	} {
		assert.False(t, isLoopbackAddress(address), "%s is not this machine, scheme or no scheme", address)
	}
}

// TestSuggestedCommandsSurviveAShell pins the quoting: a path with whitespace
// pastes back as one argument, and a leading dash cannot read as a flag.
func TestSuggestedCommandsSurviveAShell(t *testing.T) {
	assert.Equal(t, "examples/hello/workflow.yaml", shellArgument("examples/hello/workflow.yaml"),
		"an ordinary path is not decorated")
	assert.Equal(t, "'my flows/deploy it.yaml'", shellArgument("my flows/deploy it.yaml"))
	assert.Equal(t, "./-tricky.yaml", shellArgument("-tricky.yaml"))
	assert.Equal(t, `'it'\''s.yaml'`, shellArgument("it's.yaml"))
}

// TestTheSuggestedRunCarriesItsInputs is the third finding: a workflow with
// required inputs refuses the flagless spelling, so the recovery command must
// be the invocation that failed, inputs and all.
func TestTheSuggestedRunCarriesItsInputs(t *testing.T) {
	rendered := reportOf(t, unreachableServerWithArguments(
		serverFlags{address: "localhost:9233"},
		"deploy.yaml",
		[]string{"--input-file=inputs.json", "--input=region=eu-west-1"},
		unavailable(),
	))
	assert.Contains(t, rendered, "flow run deploy.yaml --input-file=inputs.json --input=region=eu-west-1")
	assert.Contains(t, rendered, "flow run local deploy.yaml --input-file=inputs.json --input=region=eu-west-1")
}
