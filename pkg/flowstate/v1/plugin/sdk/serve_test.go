package sdk

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
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
			wantMsg: "this plugin speaks 1",
		},
		{
			name: "no socket to serve on",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    "1",
			},
			wantMsg: protocol.SocketEnv + " is not set",
		},
		{
			name: "no token to authenticate the host by",
			env: map[string]string{
				protocol.MagicCookieEnv: protocol.MagicCookieValue,
				protocol.VersionsEnv:    "1",
				protocol.SocketEnv:      "/tmp/s",
			},
			wantMsg: protocol.TokenEnv + " is not set",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, name := range []string{
				protocol.MagicCookieEnv, protocol.VersionsEnv,
				protocol.SocketEnv, protocol.TokenEnv, protocol.HostFDEnv,
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

// TestReadEnvironmentClearsTheToken checks that the token does not stay in the
// environment, where anything that can read this process can read it.
func TestReadEnvironmentClearsTheToken(t *testing.T) {
	t.Setenv(protocol.MagicCookieEnv, protocol.MagicCookieValue)
	t.Setenv(protocol.VersionsEnv, "1")
	t.Setenv(protocol.SocketEnv, "/tmp/s")
	t.Setenv(protocol.TokenEnv, "the-token")

	env, err := readEnvironment()
	if err != nil {
		t.Fatalf("readEnvironment: %v", err)
	}

	if got := env.token(); got != "the-token" {
		t.Errorf("the token was not read")
	}
	if _, still := os.LookupEnv(protocol.TokenEnv); still {
		t.Error("the token is still in the environment")
	}
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
			client := flowstatev1connect.NewPluginServiceClient(
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

			resp, err := client.Describe(ctx, connect.NewRequest(&flowstatev1.DescribePluginRequest{}))

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

// TestServeAnnouncesOnceThenLeavesStdoutAlone checks the promise that makes the
// handshake reliable: one line on stdout, and nothing after it.
func TestServeAnnouncesOnceThenLeavesStdoutAlone(t *testing.T) {
	const token = "tok"

	var announced syncBuffer
	socket := startTestPluginCapturing(t, token, &announced)

	// The plugin is serving, so the handshake has been printed. Anything it
	// prints from now on goes to stderr instead.
	client := flowstatev1connect.NewPluginServiceClient(
		unixClient(socket), "http://plugin.invalid",
		connect.WithInterceptors(connect.UnaryInterceptorFunc(
			func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
					req.Header().Set(protocol.TokenHeader, token)
					return next(ctx, req)
				}
			})),
	)

	if _, err := client.Health(t.Context(), connect.NewRequest(&flowstatev1.HealthRequest{})); err != nil {
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
	if handshake.ProtocolVersion != protocol.Version1 {
		t.Errorf("announced protocol version = %d, want %d", handshake.ProtocolVersion, protocol.Version1)
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
	t.Setenv(protocol.TokenEnv, token)

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
				Input:  &flowstatev1.Task_Echo_Inputs{},
				Output: &flowstatev1.Task_Echo_Outputs{},
				Fn: func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
					return &flowstatev1.Node_Outputs{}, nil
				},
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

	// Wait for the socket, which is what the handshake line would tell a real
	// host.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socket); err == nil {
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
