package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// fakeHost is a real SSH server, in this process, speaking the real protocol.
//
// A fake at the client's own boundary would skip everything worth testing here:
// the host-key check happens in the handshake, the quoting is only observable
// as the command line the far side receives, and "no PTY is requested" is a
// claim about channel requests. So this is an ssh.ServerConn, and the
// assertions are about what it actually saw.
type fakeHost struct {
	listener net.Listener
	t        *testing.T

	// hostKey is what the server presents, and hostKeyLine its authorized_keys
	// spelling for a grant to pin.
	hostKey     ssh.Signer
	hostKeyLine string

	// clientKeyPath is the private key file a host grant points at.
	clientKeyPath string

	mu sync.Mutex

	// commandLines are the exec requests received, in order: the one place the
	// quoting this plugin does is observable.
	commandLines []string

	// requestTypes are every channel request type received, so a test can prove
	// no pty-req, no x11-req and no subsystem was asked for.
	requestTypes []string

	// stdout, stderr and exitCode are what the next command answers with.
	stdout   string
	stderr   string
	exitCode uint32

	// hang makes the server accept the exec request and never answer, for the
	// timeout case - where the command is running and the outcome is unknown.
	hang bool
}

// newFakeHost starts one.
func newFakeHost(t *testing.T) *fakeHost {
	t.Helper()

	hostKey, _, hostLine := generateKey(t)
	clientKey, clientPrivate, _ := generateKey(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	host := &fakeHost{
		listener:      listener,
		t:             t,
		hostKey:       hostKey,
		hostKeyLine:   hostLine,
		clientKeyPath: writeKeyFile(t, clientPrivate),
		stdout:        "ok\n",
	}

	config := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if string(key.Marshal()) != string(clientKey.PublicKey().Marshal()) {
				return nil, errUnauthorizedKey
			}
			return &ssh.Permissions{}, nil
		},
	}
	config.AddHostKey(hostKey)

	go host.serve(config)
	t.Cleanup(func() { _ = listener.Close() })

	return host
}

// address is where a host grant points.
func (f *fakeHost) address() string { return f.listener.Addr().String() }

// commandLine is the exec request the server received, for asserting on the
// quoting.
func (f *fakeHost) commandLine() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commandLines) == 0 {
		return ""
	}
	return f.commandLines[len(f.commandLines)-1]
}

// sawRequest reports whether a channel request of this type was ever received.
func (f *fakeHost) sawRequest(kind string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, seen := range f.requestTypes {
		if seen == kind {
			return true
		}
	}
	return false
}

// ran reports how many commands were executed, which is what proves a refusal
// happened before anything ran.
func (f *fakeHost) ran() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commandLines)
}

func (f *fakeHost) serve(config *ssh.ServerConfig) {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn, config)
	}
}

func (f *fakeHost) handle(conn net.Conn, config *ssh.ServerConfig) {
	defer conn.Close()

	serverConn, channels, requests, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return
	}
	defer serverConn.Close()
	go ssh.DiscardRequests(requests)

	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "only session channels")
			continue
		}

		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go f.handleSession(channel, channelRequests)
	}
}

func (f *fakeHost) handleSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()

	for request := range requests {
		f.mu.Lock()
		f.requestTypes = append(f.requestTypes, request.Type)
		f.mu.Unlock()

		if request.Type != "exec" {
			// Answered honestly rather than accepted: this plugin should never
			// send one of these, and a test asserts it did not.
			_ = request.Reply(false, nil)
			continue
		}

		var payload struct{ Command string }
		if err := ssh.Unmarshal(request.Payload, &payload); err != nil {
			_ = request.Reply(false, nil)
			continue
		}

		f.mu.Lock()
		f.commandLines = append(f.commandLines, payload.Command)
		stdout, stderr, code, hang := f.stdout, f.stderr, f.exitCode, f.hang
		f.mu.Unlock()

		_ = request.Reply(true, nil)

		if hang {
			// The command is "running": the client's timeout is what ends this,
			// and the far side never reports a status.
			time.Sleep(30 * time.Second)
			return
		}

		_, _ = channel.Write([]byte(stdout))
		_, _ = channel.Stderr().Write([]byte(stderr))
		_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{code}))
		return
	}
}

// errUnauthorizedKey is what the fake refuses an unknown client key with.
var errUnauthorizedKey = errors.New("unauthorized key")

// generateKey makes one ed25519 key, returning a signer, the key material a
// file needs, and the authorized_keys line a grant pins.
func generateKey(t *testing.T) (ssh.Signer, ed25519.PrivateKey, string) {
	t.Helper()

	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatalf("building a signer: %v", err)
	}

	public := signer.PublicKey()
	return signer, private, public.Type() + " " + base64.StdEncoding.EncodeToString(public.Marshal())
}

// writeKeyFile writes a private key where a host grant can name it.
func writeKeyFile(t *testing.T, private ed25519.PrivateKey) string {
	t.Helper()

	block, err := ssh.MarshalPrivateKey(private, "flowstate ssh plugin test")
	if err != nil {
		t.Fatalf("marshalling a private key: %v", err)
	}

	path := filepath.Join(t.TempDir(), "id_ed25519")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("writing a private key: %v", err)
	}
	return path
}
