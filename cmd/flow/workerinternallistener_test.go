package main

import (
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The worker's half of the internal listener (#916).
//
// `flow server` had the listener and `flow worker` did not, which left the
// capacity runbook in docs/DEPLOYMENT.md prescribing a signal — "this
// process's own CPU/memory", the thing that decides between raising slots and
// paying for another replica's plugin fleet — that the process it prescribes
// it for could not emit. The flag, the handler and the refusals are shared
// with the server unchanged; what is worth testing here is the wiring: that
// the flag reaches a socket, that no flag reaches no socket, and that the
// socket goes away with the worker rather than outliving it.

// TestWorkerDeclaresTheInternalListenerFlagOffByDefault is the negative
// direction, asserted where it can be asserted cheaply: `flow worker` takes
// the flag, and an operator who never types it gets the empty address that
// [startInternalListener] reads as "bind nothing".
//
// The positive half of this — a configured address really is served — is
// TestWorkerServesHealthAndPprofOnTheInternalListener below, and neither
// substitutes for the other: a default of 127.0.0.1:9090 would satisfy every
// test that only ever passes the flag explicitly, while opening a pprof port
// on every worker in every deployment that never read this flag's help.
func TestWorkerDeclaresTheInternalListenerFlagOffByDefault(t *testing.T) {
	// Not t.Parallel(): t.Setenv forbids it. The variable is cleared rather
	// than assumed unset, because the default is composed when the flag is
	// registered and a developer with it exported would otherwise see a
	// different tree than CI does.
	t.Setenv("FLOWSTATE_INTERNAL_ADDRESS", "")

	worker := findCommand(t, "worker")

	flag := worker.Flags().Lookup("internal-listen")
	require.NotNil(t, flag, "`flow worker` has no --internal-listen, so a worker still cannot "+
		"be profiled or liveness-probed — which is what #916 reports")
	assert.False(t, flag.Hidden, "--internal-listen is the spelling an operator is meant to find")

	address, err := worker.Flags().GetString("internal-listen")
	require.NoError(t, err)
	assert.Empty(t, address,
		"--internal-listen must default to empty on the worker exactly as it does on the "+
			"server; a worker resolves secrets into its own heap, and pprof serves that heap")

	server, listener, err := startInternalListener(discardLogger(), address)
	require.NoError(t, err)
	require.Nil(t, server, "the worker's default must build no internal HTTP server")
	require.Nil(t, listener, "the worker's default must bind nothing, not even loopback")
}

// TestServeInternalListenerServesThenStops is the lifecycle `flow worker`
// depends on, without a Temporal dev server in the way: what is served while
// it runs, and that stopping it actually releases the socket rather than
// leaving a goroutine holding a port after the command returns.
func TestServeInternalListenerServesThenStops(t *testing.T) {
	t.Parallel()

	server, listener, err := startInternalListener(discardLogger(), "127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, server)

	address := listener.Addr().String()
	stop := serveInternalListener(discardLogger(), server, listener)

	base := "http://" + address

	resp, err := http.Get(base + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(base + "/debug/pprof/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	stop()

	// Nothing answers on that address any more. Asserted by dialing rather
	// than by asking the server object what it thinks it is doing: a
	// Shutdown that returned while a goroutine still held the listener would
	// satisfy the second and fail this.
	_, err = net.DialTimeout("tcp", address, 2*time.Second)
	require.Error(t, err, "the internal listener's socket must be released when the worker stops")
}

// TestServeInternalListenerWithNothingConfiguredIsANoOp pins the shape that
// lets runWorker call this unconditionally: the disabled case is a nil server
// and a stop function that is safe to call.
func TestServeInternalListenerWithNothingConfiguredIsANoOp(t *testing.T) {
	t.Parallel()

	server, listener, err := startInternalListener(discardLogger(), "")
	require.NoError(t, err)
	require.Nil(t, server)

	stop := serveInternalListener(discardLogger(), server, listener)
	require.NotNil(t, stop, "a disabled listener must still hand back something callable")
	stop()
}

// internalListenerAddressLogged reads the address the worker says it bound
// from its transcript, which is how a test can use port 0 — the operating
// system picks the port, and the log line is the only place the process says
// which one it got.
var internalListenerAddressLogged = regexp.MustCompile(`starting internal listener" address=(\S+)`)

// TestWorkerServesHealthAndPprofOnTheInternalListener is #916 end to end: a
// real `flow worker` process, the flag on its real command line, and the two
// endpoints answered over a real socket.
//
// Everything cheaper than this proves something adjacent. The handler's own
// tests prove /healthz and pprof are routed; the flag test above proves the
// flag exists and defaults off. Neither can see a runWorker that reads the
// flag and never binds, or one that binds before the worker polls — which is
// the ordering that makes the first 200 mean the plugins launched and
// Temporal was dialed rather than merely that a port is open.
func TestWorkerServesHealthAndPprofOnTheInternalListener(t *testing.T) {
	namespace := registerTestNamespace(t)

	worker := startFlowWorker(t, namespace, nil,
		"--task-queue", "internal-listener-test-"+namespace,
		// Port 0: the address is read back from the log below, so this test
		// cannot collide with a sibling test's port on a shared machine.
		"--internal-listen", "127.0.0.1:0",
		"--worker-stop-timeout", "10s",
	)

	var address string
	require.Eventually(t, func() bool {
		match := internalListenerAddressLogged.FindStringSubmatch(worker.snapshot())
		if match == nil {
			return false
		}
		address = match[1]

		return true
	}, 30*time.Second, 50*time.Millisecond,
		"the worker never logged an internal listener address:\n%s", worker.snapshot())

	base := "http://" + address

	resp, err := http.Get(base + "/healthz")
	require.NoError(t, err, "the worker's internal listener refused a liveness probe")
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"a worker's /healthz must answer 200 — that is the probe target the Kubernetes "+
			"recipe in docs/DEPLOYMENT.md now points at")

	resp, err = http.Get(base + "/debug/pprof/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"the worker must serve pprof's index — the capacity runbook's rung 3 (is this "+
			"process itself the constraint?) is what it exists for")

	// And it goes away with the worker. A listener that outlived the drain
	// would hold the port against the replacement process a restart starts,
	// which is the failure an operator sees rather than the one a log shows.
	require.NoError(t, worker.cmd.Process.Signal(syscall.SIGTERM))

	waited := make(chan error, 1)
	go func() { waited <- worker.cmd.Wait() }()

	select {
	case err := <-waited:
		require.NoError(t, err, "flow worker exited non-zero after SIGTERM:\n%s", worker.snapshot())
	case <-time.After(20 * time.Second):
		_ = worker.cmd.Process.Kill()
		t.Fatalf("flow worker did not exit within 20s of SIGTERM:\n%s", worker.snapshot())
	}

	_, err = net.DialTimeout("tcp", address, 2*time.Second)
	require.Error(t, err, "the worker exited still holding its internal listener's port")
}

// TestWorkerBindsNoInternalListenerWithoutTheFlag is the negative direction
// at the process level: a worker started the way every recipe in
// docs/DEPLOYMENT.md starts one — no --internal-listen, no
// FLOWSTATE_INTERNAL_ADDRESS — opens no socket at all.
//
// Asserted against a worker that is confirmed to be polling (startFlowWorker
// waits for that), so this is "it got all the way up and still bound
// nothing", not "it had not reached the binding yet".
func TestWorkerBindsNoInternalListenerWithoutTheFlag(t *testing.T) {
	namespace := registerTestNamespace(t)

	worker := startFlowWorker(t, namespace, []string{"FLOWSTATE_INTERNAL_ADDRESS="},
		"--task-queue", "no-internal-listener-test-"+namespace,
	)

	// The worker is already polling. Give the binding, which happens
	// immediately after that on the configured path, room to have appeared
	// before concluding it did not.
	time.Sleep(2 * time.Second)

	got := worker.snapshot()
	assert.NotContains(t, got, "starting internal listener",
		"a worker nobody configured a listener for bound one anyway:\n%s", got)
	assert.False(t, strings.Contains(got, "/debug/pprof"),
		"a worker nobody configured a listener for mentioned pprof:\n%s", got)
}
