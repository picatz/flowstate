package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	pluginv1connect "github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// TestReadEnvironmentRefusals checks what a plugin must refuse to serve without.
//
// Each of these is the plugin failing closed at startup with one clear message,
// rather than serving something it cannot serve and failing per request later.
func TestReadEnvironmentRefusals(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr error
		wantMsg string
	}{
		{
			name:    "run from a shell, with no cookie at all",
			env:     map[string]string{},
			wantErr: ErrNotLaunchedByHost,
		},
		{
			name:    "the wrong cookie",
			env:     map[string]string{protocol.MagicCookieEnv: "guessed"},
			wantErr: ErrNotLaunchedByHost,
		},
		{
			name: "a host that speaks no version this plugin does",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    "99",
			},
			wantErr: ErrProtocolVersion,
			wantMsg: fmt.Sprintf("this plugin speaks %d", protocol.Version4),
		},
		{
			name: "no socket to serve on",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    protocol.FormatVersions(protocol.HostVersions()),
			},
			wantMsg: protocol.SocketEnv + " is not set",
		},
		{
			name: "no descriptor to read the token from",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    protocol.FormatVersions(protocol.HostVersions()),
				protocol.SocketEnv:      "/tmp/s",
			},
			wantMsg: protocol.TokenFDEnv + " is not set",
		},
		{
			// The descriptor number is input from outside this process, so a
			// value that is not one is refused rather than parsed generously.
			name: "a token descriptor that is not a number",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    protocol.FormatVersions(protocol.HostVersions()),
				protocol.SocketEnv:      "/tmp/s",
				protocol.TokenFDEnv:     "the-token-itself",
			},
			wantMsg: protocol.TokenFDEnv + " does not name an inherited descriptor",
		},
		{
			// stdin, stdout and stderr are not inherited extra descriptors, and
			// reading a secret from whatever they happen to be is worse than
			// refusing: on stdout it would also consume the handshake channel.
			name: "a token descriptor naming stdout",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    protocol.FormatVersions(protocol.HostVersions()),
				protocol.SocketEnv:      "/tmp/s",
				protocol.TokenFDEnv:     "1",
			},
			wantMsg: protocol.TokenFDEnv + " does not name an inherited descriptor",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, name := range []string{
				protocol.MagicCookieEnv, protocol.VersionsEnv,
				protocol.SocketEnv, protocol.TokenEnv, protocol.TokenFDEnv,
				protocol.HostFDEnv,
			} {
				t.Setenv(name, "")
				os.Unsetenv(name)
			}
			for name, value := range test.env {
				t.Setenv(name, value)
			}

			_, err := readEnvironment()
			if err == nil {
				t.Fatal("readEnvironment succeeded, want a refusal")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error = %v, want one wrapping %v", err, test.wantErr)
			}
			if test.wantMsg != "" && !strings.Contains(err.Error(), test.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), test.wantMsg)
			}
		})
	}
}

// TestReadEnvironmentReadsTheTokenOffADescriptor checks the delivery that
// replaced an environment variable, and that nothing puts the value back.
//
// Unsetting a variable was never enough on Linux: /proc/<pid>/environ shows the
// block the kernel copied at execve(2), so a token delivered there is readable
// for the process's whole life. The plugin-side half of the fix is that this SDK
// reads the secret off a descriptor and never writes it into its own
// environment, which is what the second half of this test pins.
func TestReadEnvironmentReadsTheTokenOffADescriptor(t *testing.T) {
	const token = "THE-PER-LAUNCH-TOKEN"

	t.Setenv(protocol.MagicCookieEnv, protocol.MagicCookieValue)
	t.Setenv(protocol.VersionsEnv, protocol.FormatVersions(protocol.HostVersions()))
	t.Setenv(protocol.SocketEnv, "/tmp/s")
	t.Setenv(protocol.TokenFDEnv, strconv.Itoa(tokenDescriptor(t, token)))

	env, err := readEnvironment()
	if err != nil {
		t.Fatalf("readEnvironment: %v", err)
	}

	if got := env.token(); got != token {
		t.Errorf("token = %q, want %q", got, token)
	}

	if _, set := os.LookupEnv(protocol.TokenEnv); set {
		t.Errorf("%s is set; the retired variable must stay empty", protocol.TokenEnv)
	}
	for _, entry := range os.Environ() {
		if strings.Contains(entry, token) {
			t.Errorf("the token reached this process's environment as %q", entry)
		}
	}
}

// tokenDescriptor returns the number of a descriptor already holding one token
// line, the way the host hands one to a plugin.
//
// The pipe's *os.File is kept alive for the life of this test binary on purpose.
// readToken closes the descriptor it is given, and a collected *os.File would
// close that number a second time — by then possibly belonging to something else
// here. Nothing in this binary needs the wrapper again, so holding it is cheaper
// than the alternative.
func tokenDescriptor(t *testing.T, token string) int {
	t.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}

	if err := protocol.WriteToken(writer, token); err != nil {
		t.Fatalf("writing the token: %v", err)
	}

	// Closed before the reader ever looks: the token is far smaller than a
	// pipe's buffer, so the read finds one line and then EOF with nothing to
	// wait for — which is exactly what the host arranges before a plugin starts.
	if err := writer.Close(); err != nil {
		t.Fatalf("closing the token pipe: %v", err)
	}

	fd := int(reader.Fd())

	keptOpen.Lock()
	keptOpen.files = append(keptOpen.files, reader)
	keptOpen.Unlock()

	return fd
}

// keptOpen holds every pipe end handed to readToken. See [tokenDescriptor].
var keptOpen struct {
	sync.Mutex
	files []*os.File
}

// TestServeAuthenticatesTheHost checks that a plugin serves only the worker that
// launched it.
//
// The socket's directory is what keeps other users out; this is what keeps
// anything that reaches the socket anyway — another process of the same user, a
// bug in a directory mode — from acting as the host.
func TestServeAuthenticatesTheHost(t *testing.T) {
	const token = "the-per-launch-token"

	socket := startTestPlugin(t, token)

	tests := []struct {
		name      string
		presented string
		wantOK    bool
	}{
		{name: "the right token", presented: token, wantOK: true},
		{name: "no token", presented: "", wantOK: false},
		{name: "the wrong token", presented: "guessed", wantOK: false},
		{name: "a prefix of the token", presented: token[:5], wantOK: false},
		{name: "the token with something appended", presented: token + "x", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := pluginv1connect.NewPluginServiceClient(
				unixClient(socket), "http://plugin.invalid",
				connect.WithInterceptors(connect.UnaryInterceptorFunc(
					func(next connect.UnaryFunc) connect.UnaryFunc {
						return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
							if test.presented != "" {
								req.Header().Set(protocol.TokenHeader, test.presented)
							}
							return next(ctx, req)
						}
					})),
			)

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			resp, err := client.Describe(ctx, connect.NewRequest(&pluginv1.DescribeRequest{}))

			if !test.wantOK {
				if err == nil {
					t.Fatal("the plugin answered a request that did not authenticate")
				}
				if connect.CodeOf(err) != connect.CodePermissionDenied {
					t.Errorf("code = %v, want permission denied", connect.CodeOf(err))
				}
				return
			}

			if err != nil {
				t.Fatalf("Describe: %v", err)
			}
			if resp.Msg.GetManifest().GetName() != "testplug" {
				t.Errorf("manifest name = %q", resp.Msg.GetManifest().GetName())
			}
		})
	}
}

// TestServeAuthenticatesTheHostOnExecuteStream checks that ExecuteStream, a
// streaming RPC, is authenticated exactly as every unary one is.
//
// requireToken used to be installed as a connect.UnaryInterceptorFunc, whose
// WrapStreamingHandler is a documented no-op — so this route ran with no
// per-launch-token check at all: any process that could reach the socket
// could invoke a task, supplying whatever identity and namespace it liked in
// the request, with no token at all. This is the streaming twin of
// TestServeAuthenticatesTheHost, over TaskService.ExecuteStream instead of
// PluginService.Describe.
func TestServeAuthenticatesTheHostOnExecuteStream(t *testing.T) {
	const token = "the-per-launch-token"

	socket := startTestPlugin(t, token)

	tests := []struct {
		name      string
		presented string
		wantOK    bool
	}{
		{name: "the right token", presented: token, wantOK: true},
		{name: "no token", presented: "", wantOK: false},
		{name: "the wrong token", presented: "guessed", wantOK: false},
		{name: "a prefix of the token", presented: token[:5], wantOK: false},
		{name: "the token with something appended", presented: token + "x", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// No interceptor here: connect.UnaryInterceptorFunc's
			// WrapStreamingClient is a documented no-op, so an interceptor
			// built that way — the same shape the plugin package's own
			// client used before this fix — would silently never attach the
			// header to this call. Setting it directly on the request proves
			// the fix at the handler, independent of how a client happens to
			// be built.
			client := pluginv1connect.NewTaskServiceClient(unixClient(socket), "http://plugin.invalid")

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			req := connect.NewRequest(&pluginv1.ExecuteStreamRequest{
				Task: &flowstatev1.Task{Name: "testplug_noop"},
			})
			if test.presented != "" {
				req.Header().Set(protocol.TokenHeader, test.presented)
			}

			stream, err := client.ExecuteStream(ctx, req)

			if !test.wantOK {
				// A streaming call reports its handler's error on the first
				// Receive, not on the call that opens the stream: connect
				// builds the ServerStreamForClient before the handler side
				// has run at all.
				if err != nil {
					t.Fatalf("ExecuteStream: %v", err)
				}
				if stream.Receive() {
					t.Fatal("the plugin streamed a message for a request that did not authenticate")
				}
				if connect.CodeOf(stream.Err()) != connect.CodePermissionDenied {
					t.Errorf("code = %v, want permission denied", connect.CodeOf(stream.Err()))
				}
				return
			}

			if err != nil {
				t.Fatalf("ExecuteStream: %v", err)
			}
			defer stream.Close()

			var gotResponse bool
			for stream.Receive() {
				if stream.Msg().GetResponse() != nil {
					gotResponse = true
				}
			}
			if err := stream.Err(); err != nil {
				t.Fatalf("stream: %v", err)
			}
			if !gotResponse {
				t.Error("the stream ended without a terminal response")
			}
		})
	}
}

// TestServeAnnouncesOnceThenLeavesStdoutAlone checks the promise that makes the
// handshake reliable: one line on stdout, and nothing after it.
func TestServeAnnouncesOnceThenLeavesStdoutAlone(t *testing.T) {
	const token = "tok"

	var announced syncBuffer
	socket := startTestPluginCapturing(t, token, &announced)

	// The plugin is serving, so the handshake has been printed. Anything it
	// prints from now on goes to stderr instead.
	client := pluginv1connect.NewPluginServiceClient(
		unixClient(socket), "http://plugin.invalid",
		connect.WithInterceptors(connect.UnaryInterceptorFunc(
			func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
					req.Header().Set(protocol.TokenHeader, token)
					return next(ctx, req)
				}
			})),
	)

	if _, err := client.Health(t.Context(), connect.NewRequest(&pluginv1.HealthRequest{})); err != nil {
		t.Fatalf("Health: %v", err)
	}

	line := strings.TrimSpace(announced.String())
	if strings.Count(line, "\n") != 0 {
		t.Errorf("more than one line was written to stdout:\n%s", announced.String())
	}

	handshake, err := protocol.ParseHandshake(line)
	if err != nil {
		t.Fatalf("the announced line does not parse: %v (%q)", err, line)
	}
	if handshake.Address != socket {
		t.Errorf("announced address = %q, want %q", handshake.Address, socket)
	}
	if handshake.ProtocolVersion != protocol.Version4 {
		t.Errorf("announced protocol version = %d, want %d", handshake.ProtocolVersion, protocol.Version4)
	}
}

// TestServeSocketPermissions checks the mode the plugin sets on its socket,
// which is the second line behind the directory the host created.
func TestServeSocketPermissions(t *testing.T) {
	socket := startTestPlugin(t, "tok")

	info, err := os.Stat(socket)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket mode = %#o, want 0600", perm)
	}
}

// syncBuffer collects what the plugin writes to stdout.
//
// It is synchronized because the goroutine draining the pipe writes to it while
// the test reads it, which is exactly the shape the race detector exists to
// catch — and a test that races is a test that cannot be trusted about anything
// else it asserts.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startTestPlugin serves a small plugin on a socket and returns its path.
func startTestPlugin(t *testing.T, token string) string {
	t.Helper()
	return startTestPluginCapturing(t, token, &syncBuffer{})
}

// startTestPluginCapturing serves a plugin, capturing what it writes to stdout.
func startTestPluginCapturing(t *testing.T, token string, stdout *syncBuffer) string {
	t.Helper()

	return startTestPluginRunning(t, token, stdout,
		func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
			return &flowstatev1.Node_Outputs{}, nil
		})
}

// startTestPluginRunning is the same harness with the task's body supplied, for
// a test whose claim is about what the SDK did before that body could run.
func startTestPluginRunning(t *testing.T, token string, stdout *syncBuffer, fn TaskFunc) string {
	t.Helper()

	// A short directory: a Unix socket address holds about a hundred bytes, and
	// the temporary directory is most of that on macOS.
	dir, err := os.MkdirTemp("", "sdkt")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	socket := filepath.Join(dir, "s")

	t.Setenv(protocol.MagicCookieEnv, protocol.MagicCookieValue)
	t.Setenv(protocol.VersionsEnv, protocol.FormatVersions(protocol.HostVersions()))
	t.Setenv(protocol.SocketEnv, socket)
	t.Setenv(protocol.TokenFDEnv, strconv.Itoa(tokenDescriptor(t, token)))

	// Run writes the handshake to os.Stdout and then points os.Stdout at stderr,
	// so both are swapped for the duration and put back afterwards.
	realStdout, realStderr := os.Stdout, os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = writer

	captured := make(chan struct{})
	go func() {
		defer close(captured)
		io.Copy(stdout, reader)
	}()

	ctx, cancel := context.WithCancel(context.Background())

	served := make(chan error, 1)
	go func() {
		served <- Run(ctx, Plugin{
			Name:    "testplug",
			Version: "0.0.1",
			Tasks: []Task{{
				Name:   "testplug_noop",
				Input:  &flowstatev1.Task_Log_Inputs{},
				Output: &flowstatev1.Task_Log_Outputs{},
				Fn:     fn,
			}},
		})
	}()

	t.Cleanup(func() {
		cancel()

		select {
		case err := <-served:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("the plugin did not shut down")
		}

		os.Stdout, os.Stderr = realStdout, realStderr
		writer.Close()
		<-captured
		reader.Close()
	})

	// Wait for the handshake line, which is what a real host waits for.
	//
	// This used to wait for the socket to exist, and the comment claimed that was
	// the same thing. It is not, and the difference is the whole of what
	// TestServeSocketPermissions asserts: `listen` creates the socket and *then*
	// narrows it to 0600, so a test that resumes the moment the path stats is
	// racing the chmod it is about to measure. It lost, once, in a full `-race`
	// run — `socket mode = 0755, want 0600`, which is umask 022 applied to a fresh
	// socket, the mode before the chmod rather than after it.
	//
	// The handshake is announced after `listen` returns, so waiting for it is
	// waiting for everything `listen` does. A weaker readiness signal than the one
	// the real consumer uses will eventually observe a state the real consumer
	// never can.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(stdout.String(), "\n") {
			return socket
		}
		select {
		case err := <-served:
			t.Fatalf("the plugin stopped before it served: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("the plugin never started listening")
	return ""
}

// unixClient returns an HTTP client that reaches a Unix socket, the way the host
// does.
func unixClient(socket string) connect.HTTPClient {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socket)
			},
		},
	}
}
