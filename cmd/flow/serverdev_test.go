package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// What `flow server dev` has to earn is narrower than what it starts, and it is
// three claims.
//
// It refuses to compose into something that is not a dev stack. It says every
// posture it takes on the operator's behalf, in the words the flags it replaces
// use. And the two commands it promises are actually two commands: a person with
// only this binary reaches a durable run without a Temporal, a server or a worker
// of their own, and gets their machine back afterwards with nothing of it left
// running.
//
// The first two are cheap and are tested as pure functions. The third needs the
// real stack, so it is one bounded integration test at the bottom of this file.

// TestServerDevRefusesToComposeIntoSomethingElse walks the refusals.
//
// Both directions, per CLAUDE.md's rule about isolation tests that only prove a
// party reaches its own resource: every case that must be refused is here beside
// a case that must be allowed, because a check that refuses everything passes a
// table of refusals and is useless.
func TestServerDevRefusesToComposeIntoSomethingElse(t *testing.T) {
	env := func(pairs ...string) devEnv {
		values := map[string]string{}
		for i := 0; i+1 < len(pairs); i += 2 {
			values[pairs[i]] = pairs[i+1]
		}

		return func(name string) string { return values[name] }
	}

	for _, tc := range []struct {
		name    string
		flags   devFlags
		getenv  devEnv
		refused string
	}{
		{
			name:   "loopback by name",
			flags:  devFlags{listen: "localhost:9233"},
			getenv: env(),
		},
		{
			name:   "loopback by address",
			flags:  devFlags{listen: "127.0.0.1:9233", listenGiven: true},
			getenv: env(),
		},
		{
			name:   "loopback, v6",
			flags:  devFlags{listen: "[::1]:9233", listenGiven: true},
			getenv: env(),
		},
		{
			name:    "every interface, which is the one a permissive check misses",
			flags:   devFlags{listen: ":9233", listenGiven: true},
			getenv:  env(),
			refused: "reaches past this machine",
		},
		{
			name:    "the wildcard spelled out",
			flags:   devFlags{listen: "0.0.0.0:9233", listenGiven: true},
			getenv:  env(),
			refused: "reaches past this machine",
		},
		{
			name:    "a routable address",
			flags:   devFlags{listen: "10.0.0.4:9233", listenGiven: true},
			getenv:  env(),
			refused: "reaches past this machine",
		},
		{
			name:    "FLOWSTATE_ADDRESS, which is where it arrives without a flag",
			flags:   devFlags{listen: "10.0.0.4:9233"},
			getenv:  env("FLOWSTATE_ADDRESS", "10.0.0.4:9233"),
			refused: "FLOWSTATE_ADDRESS=10.0.0.4:9233",
		},
		{
			// The deployment's own authentication configuration, inherited from
			// a shell rather than typed here. A stack that started anyway would
			// serve everyone while its operator believes that file decides who
			// gets in.
			name:    "authentication configured in the environment",
			flags:   devFlags{listen: "localhost:9233"},
			getenv:  env("FLOWSTATE_AUTH_POLICY", "/etc/flowstate/trust.yaml"),
			refused: "FLOWSTATE_AUTH_POLICY=/etc/flowstate/trust.yaml",
		},
		{
			// The same path, typed on this command line, is somebody choosing
			// it for this dev stack's worker-side secret resolution, with the
			// issuers unused, which the flag's help and the banner both say.
			name: "the same policy, chosen on this command line",
			flags: devFlags{
				listen:          "localhost:9233",
				authPolicy:      "/etc/flowstate/trust.yaml",
				authPolicyGiven: true,
			},
			getenv: env("FLOWSTATE_AUTH_POLICY", "/etc/flowstate/trust.yaml"),
		},
		{
			name:    "somebody else's Temporal, which this would silently not use",
			flags:   devFlags{listen: "localhost:9233"},
			getenv:  env("TEMPORAL_ADDRESS", "temporal.internal:7233"),
			refused: "TEMPORAL_ADDRESS=temporal.internal:7233",
		},
		{
			name:    "somebody else's Temporal, named by profile",
			flags:   devFlags{listen: "localhost:9233"},
			getenv:  env("TEMPORAL_PROFILE", "staging"),
			refused: "TEMPORAL_PROFILE=staging",
		},
		{
			name:    "somebody else's Temporal, named by an explicit config file",
			flags:   devFlags{listen: "localhost:9233"},
			getenv:  env("TEMPORAL_CONFIG_FILE", "/etc/temporal/staging.toml"),
			refused: "TEMPORAL_CONFIG_FILE=/etc/temporal/staging.toml",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := devRefusals(tc.flags, tc.getenv)

			if tc.refused == "" {
				assert.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.refused)

			// A fail-closed refusal with no way forward is the dead end
			// invariant 8 forbids, so every one of them names the command line
			// that does what the operator evidently meant.
			assert.Contains(t, err.Error(), "flow server",
				"a refusal has to leave the reader somewhere to go")
		})
	}
}

// TestDevBannerSaysWhatTheReplacedFlagsSay pins the two posture sentences against
// the commands they stand in for.
//
// The banner's whole claim is that it states these postures *as the flags it
// replaces would*, and the flags' own sentences live in main.go. Two descriptions
// of one posture is the "written down twice" shape CLAUDE.md warns about, and the
// drift is invisible from either side: the banner would keep reading fine while
// describing a warning nothing prints any more. So the source is read.
func TestDevBannerSaysWhatTheReplacedFlagsSay(t *testing.T) {
	source, err := os.ReadFile("main.go")
	require.NoError(t, err)

	assert.Contains(t, string(source), devPostureAnonymous,
		"the banner's anonymous-auth sentence is no longer the one `flow server --insecure-no-auth` logs")
	assert.Contains(t, string(source), devPostureUnversioned,
		"the banner's unversioned sentence is no longer the one `flow worker --"+allowUnversionedFlag+"` logs")
}

// TestDevBannerStatesEveryInsecurePosture is the unit half of the mutation the
// integration test below covers end to end: a banner that stopped naming a
// posture the process is still taking.
func TestDevBannerStatesEveryInsecurePosture(t *testing.T) {
	var out, errOut strings.Builder
	surface := ui.Plain(&out, &errOut)

	writeDevBanner(surface, devStack{
		flowstate: "127.0.0.1:9233",
		temporal:  "127.0.0.1:40001",
		uiURL:     "http://localhost:8233",
		otlp:      "http://localhost:4317",
	})

	banner := errOut.String()

	assert.Empty(t, out.String(), "the banner is an account, not an answer, and belongs on stderr")

	for _, said := range []string{
		"http://127.0.0.1:9233",
		"127.0.0.1:40001",
		"http://localhost:8233",
		"in memory; nothing here survives this process",
		"http://localhost:4317",
		"--insecure-no-auth",
		devPostureAnonymous,
		"--" + allowUnversionedFlag,
		devPostureUnversioned,
		"FLOWSTATE_ALLOW_LOOPBACK_EGRESS unset",
		"flow run <file>",
	} {
		assert.Contains(t, banner, said)
	}
}

// TestDevBannerReportsPersistenceAndEgressOptIns covers the two lines that read
// differently when the operator has changed something: sqlite instead of memory,
// and loopback egress opted into rather than denied. Both are postures the banner
// exists to state, and a banner that only ever prints the defaults would pass the
// test above while saying nothing true about this session.
func TestDevBannerReportsPersistenceAndEgressOptIns(t *testing.T) {
	var out, errOut strings.Builder
	surface := ui.Plain(&out, &errOut)

	writeDevBanner(surface, devStack{
		flowstate:      "127.0.0.1:9233",
		temporal:       "127.0.0.1:40001",
		database:       "/tmp/flowstate.db",
		loopbackEgress: true,
		egressPolicy:   "egress.yaml",
		taskPolicy:     "tasks.yaml",
		authPolicy:     "policy.yaml",
	})

	banner := errOut.String()

	assert.Contains(t, banner, "sqlite at /tmp/flowstate.db; runs survive a restart")
	assert.Contains(t, banner, "FLOWSTATE_ALLOW_LOOPBACK_EGRESS=true")
	assert.Contains(t, banner, "the http task may reach this machine")
	assert.Contains(t, banner, "--egress-policy egress.yaml")
	assert.Contains(t, banner, "--task-policy tasks.yaml")
	assert.Contains(t, banner, "--auth-policy policy.yaml")
	assert.Contains(t, banner, "its issuers are unused")
	assert.NotContains(t, banner, "temporal ui",
		"no UI was configured, so the banner must not name one")
}

// TestDevStackJSONCarriesThePostureWithTheEndpoints checks the document a script
// reads. The caller who starts this programmatically is exactly the caller who
// will never see the banner, so "this endpoint accepts anonymous callers" has to
// travel with the address rather than be inferred from the command's name.
func TestDevStackJSONCarriesThePostureWithTheEndpoints(t *testing.T) {
	var out, errOut strings.Builder
	surface := ui.Plain(&out, &errOut)

	require.NoError(t, writeDevStackJSON(surface, devStack{
		flowstate: "127.0.0.1:9233",
		temporal:  "127.0.0.1:40001",
		uiURL:     "http://localhost:8233",
		database:  "runs.db",
	}))

	var stack devStackJSON
	require.NoError(t, json.Unmarshal([]byte(out.String()), &stack))

	assert.Equal(t, "127.0.0.1:9233", stack.FlowstateAddress)
	assert.Equal(t, "http://127.0.0.1:9233", stack.FlowstateURL)
	assert.Equal(t, "127.0.0.1:40001", stack.TemporalAddress)
	assert.Equal(t, devTemporalNamespace, stack.TemporalNamespace)
	assert.Equal(t, "http://localhost:8233", stack.TemporalUIURL)
	assert.Equal(t, "sqlite", stack.Persistence)
	assert.True(t, stack.AnonymousAuth)
	assert.True(t, stack.Unversioned)
	assert.False(t, stack.LoopbackEgress)
}

// TestDevUIPortEncodesNoUIRatherThanAFreeOne guards a one-character mistake with
// a silent consequence. The SDK reads an empty UI port as "pick a free one", and
// a free port DevServer never reports is a UI nobody can open, so 0 has to mean
// no UI at all, which is what the flag's help promises.
func TestDevUIPortEncodesNoUIRatherThanAFreeOne(t *testing.T) {
	assert.Equal(t, "", devUIPort(0))
	assert.Equal(t, "", devUIURL(0))
	assert.Equal(t, "8233", devUIPort(devDefaultUIPort))
	assert.Equal(t, "http://localhost:8233", devUIURL(devDefaultUIPort))
}

// syncWriter is a buffer two goroutines may touch: the command writes to it while
// the test reads it, which is a data race with -race and a flake without it.
type syncWriter struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.b.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.b.String()
}

// TestServerDevReachesADurableRunInTwoCommands is the house gate #377 exists for:
// somebody holding only this binary reaches a durable run without starting a
// Temporal, a server or a worker of their own.
//
// Written as the two commands rather than as calls into the internals, because
// the claim is about a command line. The scaffold is written first (that is
// `flow init`, the command before the two) and then:
//
//	flow server dev
//	flow run <file>
//
// and the run has to complete, on Temporal, through the same [engine.Register]
// path production uses.
//
// The end of it is the other half of the promise. Ctrl-C is a person asking for
// their machine back, and this command holds a child process, so the test asserts
// that the Temporal frontend it was serving on stops answering. A teardown that
// stopped the server and left the child is invisible to every other assertion
// here: the run completed, the command returned, and a Temporal server is still
// holding a port.
func TestServerDevReachesADurableRunInTwoCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: needs a Temporal dev server; CI runs the full suite")
	}

	// The refusals read the process environment, and a machine that exports any
	// of these (a developer's shell pointed at a staging cluster is the usual
	// one) would refuse the command for a reason that has nothing to do with
	// what is under test.
	for _, name := range []string{
		"FLOWSTATE_ADDRESS", "FLOWSTATE_AUTH_POLICY", "TEMPORAL_ADDRESS", "TEMPORAL_PROFILE",
		"TEMPORAL_CONFIG_FILE",
	} {
		t.Setenv(name, "")
	}

	dir := t.TempDir()
	workflow := filepath.Join(dir, scaffoldWorkflow)

	_, _, err := initOutput(t, dir)
	require.NoError(t, err, "scaffolding the workflow the two commands are about")

	// Port 0 on both: this suite runs beside other tests that hold Temporal
	// ports, and a dev command that can only ever bind 9233 and 8233 would make
	// the gate a test of what else is running. The UI is off for the same reason,
	// and because nothing here opens a browser.
	out, errOut := &syncWriter{}, &syncWriter{}

	root := newRootCommand()
	root.SetOut(out)
	root.SetErr(errOut)
	root.SetArgs([]string{"server", "dev", "--listen", "localhost:0", "--ui-port", "0", "-o", "json"})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	stopped := make(chan error, 1)
	go func() { stopped <- root.ExecuteContext(ctx) }()

	stack, err := awaitDevStack(t, out, stopped)
	if err != nil {
		// Loudly, and only for the one environment failure that is not a
		// finding about this code: the dev server is a `temporal` binary this
		// machine may have neither cached nor be able to fetch. Any other
		// startup error is exactly what this gate exists to catch (a worker
		// registration break, a bad SDK option, a listener failure), so it
		// fails rather than skipping; a skip that swallowed those would let CI
		// pass while the command cannot start at all.
		if !devServerUnavailable(err) {
			t.Fatalf("`flow server dev` failed to start, and not for want of a Temporal binary: %v", err)
		}
		t.Skipf("SKIPPING the two-command gate: this environment cannot start a Temporal dev server (%v). "+
			"That is an environment limitation, not a passing test: nothing below this line ran.", err)
	}

	require.Equal(t, "memory", stack.Persistence, "the default has to be the ephemeral one")
	require.True(t, stack.AnonymousAuth)
	require.True(t, stack.Unversioned)

	// The banner is a product surface, and a stack that took the postures
	// without stating them is the failure this asserts. Checked here rather than
	// only in the unit test above, because the unit test cannot see a command
	// that stopped calling the renderer.
	banner := errOut.String()
	assert.Contains(t, banner, devPostureAnonymous, "the running stack never stated its auth posture")
	assert.Contains(t, banner, devPostureUnversioned, "the running stack never stated its versioning posture")
	assert.Contains(t, banner, stack.FlowstateAddress, "the banner has to name the address it bound")

	// Command two. Not `run local`: this submits to the server the first command
	// started, and the step executes on the worker it started, durably.
	report, err := runFlow(t, "run", workflow, "--address", stack.FlowstateAddress)
	require.NoError(t, err, "the durable run: %s", report)
	assert.Contains(t, report, "COMPLETED")

	// Ctrl-C, and the whole stack with it.
	cancel()

	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(devShutdownTimeout + 30*time.Second):
		t.Fatal("`flow server dev` did not return after its context was canceled")
	}

	assertNothingAnswersAt(t, stack.TemporalAddress)
	assertNothingAnswersAt(t, stack.FlowstateAddress)
}

// awaitDevStack reads the resolved-endpoints document the command writes before
// it begins serving, or reports why it never came.
//
// Bounded twice over, because the two ways this waits are bounded by different
// things: the command may fail (which arrives on stopped) or it may simply take
// its start-up budget, which on a machine with no cached `temporal` binary
// includes a download.
func awaitDevStack(t *testing.T, out *syncWriter, stopped <-chan error) (devStackJSON, error) {
	t.Helper()

	deadline := time.After(devStartTimeout + 30*time.Second)

	for {
		if line := strings.TrimSpace(out.String()); line != "" {
			var stack devStackJSON
			if err := json.Unmarshal([]byte(line), &stack); err == nil {
				return stack, nil
			}
		}

		select {
		case err := <-stopped:
			if err == nil {
				err = errDevStackVanished
			}

			return devStackJSON{}, err
		case <-deadline:
			return devStackJSON{}, errDevStackTimedOut
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// The two ways awaiting the stack ends without a stack, named so the skip above
// reads as something other than a nil error.
var (
	errDevStackVanished = devStackError("`flow server dev` returned before it reported any endpoints")
	errDevStackTimedOut = devStackError("`flow server dev` reported no endpoints within its start-up budget")
)

type devStackError string

func (e devStackError) Error() string { return string(e) }

// assertNothingAnswersAt is the teardown assertion: the address stops accepting
// connections.
//
// Retried rather than probed once, because a listener closing and the kernel
// releasing the port are not the same instant, and a single probe would make this
// a test of timing. Bounded, so a port that is genuinely still held fails rather
// than hangs.
func assertNothingAnswersAt(t *testing.T, address string) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			return
		}
		_ = conn.Close()

		if time.Now().After(deadline) {
			t.Fatalf("something is still listening at %s after `flow server dev` returned: "+
				"the stack left a process behind", address)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// devServerUnavailable reports whether err is the one environment failure the
// two-command gate may skip on: this machine cannot provide a `temporal`
// binary, because the SDK could not download one and none was cached. Matched
// on the download and exec failure shapes the SDK and the OS produce, and
// nothing broader: an unrecognized startup error is a finding about this
// command, and skipping on it would let CI pass while `flow server dev`
// cannot start at all.
func devServerUnavailable(err error) bool {
	if err == nil {
		return false
	}

	text := err.Error()
	for _, marker := range []string{
		"unable to download",
		"failed to download",
		"download temporal",
		"no such host",
		"executable file not found",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}

	return false
}
