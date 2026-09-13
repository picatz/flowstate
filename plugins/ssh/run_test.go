package main

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestMain installs an operator egress policy that permits the ssh scheme on
// loopback.
//
// It is deliberately not the deployment default: this plugin refuses that, and
// the refusal has its own test. What is installed here is what an operator who
// meant to allow remote execution would write, so the tests below are about the
// grants and the protocol rather than about the policy.
func TestMain(m *testing.M) {
	document := "egress:\n  schemes: [ssh]\n  allow_loopback: true\n"
	if err := os.Setenv(sdk.EgressPolicyEnv, base64.StdEncoding.EncodeToString([]byte(document))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

// grantsFor builds the operator authority for a test: one host, one command,
// wired to the fake server.
func grantsFor(t *testing.T, host *fakeHost, command commandGrant) *grants {
	t.Helper()

	parsed := &grants{
		Hosts: map[string]hostGrant{"target": {
			Address:      host.address(),
			User:         "runbook",
			IdentityFile: host.clientKeyPath,
			HostKeys:     []string{host.hostKeyLine},
			Commands:     []string{"probe"},
		}},
		Commands: map[string]commandGrant{"probe": command},
	}
	if err := parsed.check(); err != nil {
		t.Fatalf("the test's own grants do not validate: %v", err)
	}
	return parsed
}

// simpleCommand is a command grant with no parameters.
func simpleCommand() commandGrant {
	return commandGrant{Argv: []string{"/bin/echo", "hello"}, Timeout: Duration(10 * time.Second)}
}

// TestAGrantedCommandRunsAndItsOutputIsCaptured is the ordinary path, end to
// end, against a real SSH server.
func TestAGrantedCommandRunsAndItsOutputIsCaptured(t *testing.T) {
	host := newFakeHost(t)
	host.stdout = "service restarted\n"
	host.stderr = "a warning\n"
	authority := grantsFor(t, host, simpleCommand())

	out, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.exitCode != 0 || out.stdout != "service restarted\n" || out.stderr != "a warning\n" {
		t.Errorf("result = %+v", out)
	}
	if out.truncated {
		t.Error("truncated is true for output that fit")
	}
	if got := host.commandLine(); got != "'/bin/echo' 'hello'" {
		t.Errorf("the host received %q", got)
	}
}

// TestNoPtyOrForwardingIsEverRequested is a claim about the session this plugin
// opens, and the only place it is observable is the requests the far side
// received.
func TestNoPtyOrForwardingIsEverRequested(t *testing.T) {
	host := newFakeHost(t)
	authority := grantsFor(t, host, simpleCommand())

	if _, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'"); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, unwanted := range []string{"pty-req", "x11-req", "shell", "subsystem", "env"} {
		if host.sawRequest(unwanted) {
			t.Errorf("the session sent a %q request", unwanted)
		}
	}
	if !host.sawRequest("exec") {
		t.Error("the session sent no exec request, so nothing above was tested")
	}
}

// TestAHostPresentingAnUnpinnedKeyIsRefusedBeforeAnythingRuns is the
// host-key contract: no trust-on-first-use, no known_hosts, and the refusal
// happens in the handshake - so the command never reaches the far side.
func TestAHostPresentingAnUnpinnedKeyIsRefusedBeforeAnythingRuns(t *testing.T) {
	host := newFakeHost(t)
	authority := grantsFor(t, host, simpleCommand())

	// A different key, of the same type: the grant pins bytes, not an
	// algorithm.
	_, _, otherLine := generateKey(t)
	grant := authority.Hosts["target"]
	grant.HostKeys = []string{otherLine}

	_, err := run(t.Context(), grant, authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err == nil {
		t.Fatal("a host presenting an unpinned key was accepted")
	}
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied", err)
	}
	if host.ran() != 0 {
		t.Errorf("%d commands ran on a host that failed verification", host.ran())
	}
}

// TestAMalformedPinnedKeyIsRefusedRatherThanIgnored: a grant whose pin cannot
// be parsed must not fall back to accepting whatever the host presents.
func TestAMalformedPinnedKeyIsRefusedRatherThanIgnored(t *testing.T) {
	host := newFakeHost(t)
	authority := grantsFor(t, host, simpleCommand())
	grant := authority.Hosts["target"]
	grant.HostKeys = []string{"not-a-key"}

	if _, err := run(t.Context(), grant, authority.Commands["probe"], "'/bin/echo' 'hello'"); err == nil {
		t.Fatal("a grant with an unparseable pinned key connected anyway")
	}
	if host.ran() != 0 {
		t.Errorf("%d commands ran under a grant whose pin could not be parsed", host.ran())
	}
}

// TestANonZeroExitFailsTheStepAndStillCarriesTheOutput: a command that failed
// should fail the step, and a runbook debugging it needs what the command said.
func TestANonZeroExitFailsTheStepAndStillCarriesTheOutput(t *testing.T) {
	host := newFakeHost(t)
	host.exitCode = 3
	host.stderr = "unit not found\n"
	authority := grantsFor(t, host, simpleCommand())

	out, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err == nil {
		t.Fatal("a non-zero exit was reported as success")
	}
	if out == nil {
		t.Fatal("the failure carries no output, so a runbook cannot see why")
	}
	if out.exitCode != 3 || !strings.Contains(out.stderr, "unit not found") {
		t.Errorf("result = %+v", out)
	}
	if sdk.IsUnavailable(err) {
		t.Error("a command that ran and failed was classified as retryable")
	}
}

// TestAGrantMayCountOtherExitCodesAsSuccess: whether a non-zero status is a
// failure is a fact about the command, which the operator who granted it knows
// and a workflow author does not.
func TestAGrantMayCountOtherExitCodesAsSuccess(t *testing.T) {
	host := newFakeHost(t)
	host.exitCode = 1
	command := simpleCommand()
	command.SuccessExitCodes = []int32{0, 1}
	authority := grantsFor(t, host, command)

	out, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.exitCode != 1 {
		t.Errorf("exit_code = %d", out.exitCode)
	}
}

// TestOutputOverTheGrantsLimitIsMarkedTruncated: a cut-off stream must never be
// readable as a complete one.
func TestOutputOverTheGrantsLimitIsMarkedTruncated(t *testing.T) {
	host := newFakeHost(t)
	host.stdout = strings.Repeat("x", 4096)
	command := simpleCommand()
	command.MaxOutputBytes = 128
	authority := grantsFor(t, host, command)

	out, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out.stdout) != 128 {
		t.Errorf("stdout is %d bytes, want the grant's limit of 128", len(out.stdout))
	}
	if !out.truncated {
		t.Error("truncated is false for a stream that was cut off")
	}
}

// TestACommandThatNeverFinishesIsAnUnknownOutcome is the honest core of remote
// execution: the timeout stops waiting, it does not stop the command, so the
// result is not a failure that can be retried.
func TestACommandThatNeverFinishesIsAnUnknownOutcome(t *testing.T) {
	host := newFakeHost(t)
	host.hang = true
	command := simpleCommand()
	command.Timeout = Duration(250 * time.Millisecond)
	authority := grantsFor(t, host, command)

	_, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err == nil {
		t.Fatal("a command that never reported a status was reported as success")
	}
	if !sdk.IsOutcomeUnknown(err) {
		t.Errorf("error is %v, want the unknown-outcome classification: the command may still be running", err)
	}
	if sdk.IsUnavailable(err) {
		t.Error("a command that may still be running was classified as retryable")
	}
}

// TestAnUnreachableHostIsRetryable is the other side of that line: nothing was
// sent, so nothing ran.
func TestAnUnreachableHostIsRetryable(t *testing.T) {
	host := newFakeHost(t)
	authority := grantsFor(t, host, simpleCommand())
	grant := authority.Hosts["target"]

	// Close the listener so the port answers nothing.
	_ = host.listener.Close()

	_, err := run(t.Context(), grant, authority.Commands["probe"], "'/bin/echo' 'hello'")
	if err == nil {
		t.Fatal("a connection to a closed port succeeded")
	}
	if !sdk.IsUnavailable(err) {
		t.Errorf("error is %v, want unavailable: nothing was sent, so nothing ran", err)
	}
}

// TestADeniedDestinationIsNeverDialed proves the second operator statement is
// real: even with a host grant naming the address, an egress policy that does
// not permit it stops the call.
func TestADeniedDestinationIsNeverDialed(t *testing.T) {
	host := newFakeHost(t)
	authority := grantsFor(t, host, simpleCommand())

	previous := egressPolicy
	t.Cleanup(func() { egressPolicy = previous })

	denying, err := netpolicy.New(netpolicy.WithSchemes("ssh"))
	if err != nil {
		t.Fatalf("building a deny-by-default policy: %v", err)
	}
	egressPolicy = denying

	if _, err := run(t.Context(), authority.Hosts["target"], authority.Commands["probe"], "'/bin/echo' 'hello'"); err == nil {
		t.Fatal("a host the egress policy does not permit was dialed")
	} else if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied", err)
	}
	if host.ran() != 0 {
		t.Errorf("%d commands ran on a host the egress policy denies", host.ran())
	}
}
