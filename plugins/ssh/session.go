package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/ssh"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

const (
	// maxHostAddresses bounds how many addresses one host name may resolve to
	// before this plugin stops authorizing them. A name answering with fifty
	// addresses is a name worth looking at rather than dialing.
	maxHostAddresses = 8

	// maxIdentityBytes bounds a private key file this process reads.
	maxIdentityBytes = 64 << 10

	// maxPassphraseBytes bounds a key passphrase file.
	maxPassphraseBytes = 4096
)

// result is what one remote command did.
type result struct {
	exitCode  int32
	stdout    string
	stderr    string
	truncated bool
}

// run connects to a host grant and runs one already-built command line.
//
// The order is deliberate and is the whole of this function's safety: resolve,
// authorize every candidate address against the operator's egress policy, dial
// one that was authorized, verify the host key against the grant's pinned set,
// then - and only then - send the exec request.
func run(ctx context.Context, host hostGrant, command commandGrant, commandLine string) (*result, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("%v", egressRefusalReason())
	}

	signer, err := loadIdentity(host)
	if err != nil {
		return nil, err
	}

	hostKeys, err := parseHostKeys(host.HostKeys)
	if err != nil {
		return nil, err
	}

	address, err := authorizedAddress(ctx, host)
	if err != nil {
		return nil, err
	}

	connectTimeout := host.ConnectTimeout.duration(defaultConnectTimeout)
	dialCtx, cancelDial := context.WithTimeout(ctx, connectTimeout)
	defer cancelDial()

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(dialCtx, "tcp", address)
	if err != nil {
		// Nothing has been sent, so nothing has run: this is the one failure
		// class here that is definitely safe to retry.
		return nil, sdk.Unavailable("the host could not be reached: %v", err)
	}
	defer conn.Close()

	// The deadline covers the handshake as well as the dial; without it a host
	// that accepts a connection and never speaks holds this call open.
	_ = conn.SetDeadline(time.Now().Add(connectTimeout))

	config := &ssh.ClientConfig{
		User:              host.User,
		Auth:              []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback:   pinnedHostKeys(hostKeys),
		HostKeyAlgorithms: hostKeyAlgorithms(hostKeys),
		Timeout:           connectTimeout,
	}

	clientConn, channels, requests, err := ssh.NewClientConn(conn, address, config)
	if err != nil {
		return nil, classifyHandshake(err)
	}
	client := ssh.NewClient(clientConn, channels, requests)
	defer client.Close()

	// The handshake is done; the command's own bound starts here.
	_ = conn.SetDeadline(time.Time{})

	return exec(ctx, client, command, commandLine)
}

// exec opens one session, runs the command line, and reads bounded output.
//
// One session channel, one exec request. No PTY is requested, no agent is
// forwarded, no port or X11 forwarding is set up, and no subsystem or shell is
// started - not as defaults this plugin leaves alone, but because nothing here
// asks for them and no grant can.
func exec(ctx context.Context, client *ssh.Client, command commandGrant, commandLine string) (*result, error) {
	session, err := client.NewSession()
	if err != nil {
		return nil, sdk.Unavailable("the host refused a session: %v", err)
	}
	defer session.Close()

	limit := command.outputLimit()
	stdout := &boundedWriter{limit: limit}
	stderr := &boundedWriter{limit: limit}
	session.Stdout = stdout
	session.Stderr = stderr

	// No stdin. A command that waits for input would otherwise hold this call
	// until the timeout, and a runbook command that reads stdin is a command
	// whose grant is wrong.
	session.Stdin = strings.NewReader("")

	timeout := command.Timeout.duration(defaultCommandTimeout)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Everything from here on may have run on the far side. The exec request is
	// the line: a failure before it is a definite no-run, and a failure after it
	// is an unknown outcome this plugin will not retry.
	done := make(chan error, 1)
	go func() { done <- session.Run(commandLine) }()

	select {
	case err := <-done:
		return finish(command, stdout, stderr, err)
	case <-runCtx.Done():
		// Closing the session unblocks the goroutine above; the command itself
		// keeps running on the far side, which is exactly why this is an
		// unknown outcome rather than a failure.
		_ = session.Close()
		if errors.Is(context.Cause(runCtx), context.DeadlineExceeded) || runCtx.Err() == context.DeadlineExceeded {
			return nil, sdk.OutcomeUnknown(
				"the command did not finish within this grant's timeout of %s; it may still be running on the host, so it is not retried automatically",
				timeout)
		}
		return nil, sdk.OutcomeUnknown(
			"the call was cancelled after the command was started; it may still be running on the host, so it is not retried automatically")
	}
}

// finish turns a completed session into a result or a classified failure.
func finish(command commandGrant, stdout, stderr *boundedWriter, err error) (*result, error) {
	out := &result{
		stdout:    stdout.text(),
		stderr:    stderr.text(),
		truncated: stdout.overflowed || stderr.overflowed,
	}

	var exitErr *ssh.ExitError
	switch {
	case err == nil:
		out.exitCode = 0
	case errors.As(err, &exitErr):
		out.exitCode = int32(exitErr.ExitStatus())
	default:
		var missing *ssh.ExitMissingError
		if errors.As(err, &missing) {
			// The connection closed without a status: the command ran, and
			// whether it finished is unknown.
			return nil, sdk.OutcomeUnknown(
				"the host closed the connection without reporting an exit status; the command may have run to completion, so it is not retried automatically")
		}
		return nil, sdk.OutcomeUnknown(
			"the session failed after the command was started (%v); it may have run, so it is not retried automatically", err)
	}

	if !command.successful(out.exitCode) {
		// A failure carrying the evidence: the streams are on the error path as
		// well as the success path, because a runbook debugging a non-zero exit
		// needs what the command said.
		return out, sdk.Failed("the command exited %d, which this grant does not count as success: %s",
			out.exitCode, truncate(cmp.Or(out.stderr, out.stdout, "no output"), 512))
	}
	return out, nil
}

// authorizedAddress resolves the grant's host and returns one address the
// operator's egress policy permits.
//
// Resolution happens here rather than inside the dialer so the policy decides
// on the address that will actually be connected to: a name that resolves to a
// permitted address and an internal one must not be dialable by luck of
// ordering.
func authorizedAddress(ctx context.Context, host hostGrant) (string, error) {
	hostname, port, err := net.SplitHostPort(host.dialAddress())
	if err != nil {
		return "", sdk.Failed("host grant address %q is not host:port", truncate(host.Address, 128))
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return "", sdk.Failed("host grant address %q names no usable port", truncate(host.Address, 128))
	}

	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", hostname)
	if err != nil || len(addresses) == 0 {
		return "", sdk.Unavailable("the host grant's address could not be resolved")
	}
	if len(addresses) > maxHostAddresses {
		return "", sdk.PermissionDenied(
			"the host grant's address resolved to more than the %d addresses this plugin will authorize", maxHostAddresses)
	}

	// Every candidate is authorized, and the first one is what gets dialed. A
	// name answering with one permitted address and one denied address is a
	// name this plugin refuses outright rather than a coin flip.
	var chosen netip.Addr
	for _, address := range addresses {
		addrPort := netip.AddrPortFrom(address.Unmap(), uint16(portNumber))
		if err := egressPolicy.CheckConnection(ctx, "ssh", hostname, addrPort); err != nil {
			return "", classifyEgressCheck(err)
		}
		if !chosen.IsValid() {
			chosen = address.Unmap()
		}
	}

	return net.JoinHostPort(chosen.String(), port), nil
}

// loadIdentity reads the private key the operator's grant names.
func loadIdentity(host hostGrant) (ssh.Signer, error) {
	key, err := readBoundedFile(host.IdentityFile, maxIdentityBytes, "identity_file")
	if err != nil {
		return nil, err
	}

	if host.IdentityPassphraseFile == "" {
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			var passphraseMissing *ssh.PassphraseMissingError
			if errors.As(err, &passphraseMissing) {
				return nil, sdk.Failed(
					"the host grant's identity_file is encrypted and the grant names no identity_passphrase_file")
			}
			// The key's own bytes are never in the message.
			return nil, sdk.Failed("the host grant's identity_file could not be parsed as a private key")
		}
		return signer, nil
	}

	passphrase, err := readBoundedFile(host.IdentityPassphraseFile, maxPassphraseBytes, "identity_passphrase_file")
	if err != nil {
		return nil, err
	}

	signer, err := ssh.ParsePrivateKeyWithPassphrase(key, []byte(strings.TrimRight(string(passphrase), "\r\n")))
	if err != nil {
		return nil, sdk.Failed("the host grant's identity_file could not be decrypted with the passphrase the grant names")
	}
	return signer, nil
}

// readBoundedFile reads a file the operator's grant names.
func readBoundedFile(path string, limit int64, field string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, sdk.Failed("the host grant's %s (%q) cannot be read: %v", field, truncate(path, 256), err)
	}
	if info.IsDir() {
		return nil, sdk.Failed("the host grant's %s (%q) is a directory", field, truncate(path, 256))
	}
	if info.Size() > limit {
		return nil, sdk.Failed("the host grant's %s (%q) is %d bytes, over the %d-byte limit", field, truncate(path, 256), info.Size(), limit)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, sdk.Failed("the host grant's %s (%q) cannot be read: %v", field, truncate(path, 256), err)
	}
	return data, nil
}

// parseHostKeys reads the public keys a host is permitted to present.
func parseHostKeys(entries []string) ([]ssh.PublicKey, error) {
	keys := make([]ssh.PublicKey, 0, len(entries))
	for _, entry := range entries {
		key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(entry))
		if err != nil {
			return nil, sdk.Failed("a pinned host key is not in authorized_keys format: %v", err)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, sdk.Failed("the host grant pins no usable host keys")
	}
	return keys, nil
}

// pinnedHostKeys is the callback that decides whether this is the right host.
//
// There is no trust-on-first-use and no known_hosts file to fall back to: the
// keys come from the operator's grant, and a host presenting anything else is
// refused before authentication - so a redirected or impersonated host is a
// failure rather than a command that ran somewhere nobody chose.
func pinnedHostKeys(pinned []ssh.PublicKey) ssh.HostKeyCallback {
	return func(_ string, _ net.Addr, presented ssh.PublicKey) error {
		presentedBytes := presented.Marshal()
		for _, key := range pinned {
			if key.Type() == presented.Type() && subtleEqual(key.Marshal(), presentedBytes) {
				return nil
			}
		}
		return fmt.Errorf("the host presented a %s key that this grant does not pin (%s)",
			presented.Type(), ssh.FingerprintSHA256(presented))
	}
}

// hostKeyAlgorithms restricts the handshake to the algorithms the grant pinned,
// so a host cannot choose an algorithm nobody pinned and be compared against
// nothing.
func hostKeyAlgorithms(pinned []ssh.PublicKey) []string {
	algorithms := make([]string, 0, len(pinned)+2)
	for _, key := range pinned {
		switch key.Type() {
		case ssh.KeyAlgoRSA:
			// An RSA host key may be presented under any of the SHA-2
			// signature algorithms; pinning the key does not pin the signature
			// algorithm, and refusing the modern ones would force SHA-1.
			algorithms = append(algorithms, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSA)
		default:
			algorithms = append(algorithms, key.Type())
		}
	}
	return algorithms
}

// classifyHandshake separates "this is not the host you pinned" from "the host
// would not talk to us".
func classifyHandshake(err error) error {
	message := err.Error()
	switch {
	case strings.Contains(message, "does not pin"):
		return sdk.PermissionDenied("host key verification failed: %v", err)
	case strings.Contains(message, "unable to authenticate"), strings.Contains(message, "no supported methods remain"):
		return sdk.PermissionDenied("the host refused the identity this grant names")
	default:
		return sdk.Unavailable("the SSH handshake failed: %v", err)
	}
}

// subtleEqual compares two byte slices in constant time. A host key is public,
// so this is not protecting a secret; it is protecting against the comparison
// itself becoming the thing an attacker measures.
func subtleEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

// boundedWriter collects a stream up to a limit and remembers that there was
// more, so a truncated stream is never reported as a complete one.
type boundedWriter struct {
	limit      int64
	written    int64
	buffer     strings.Builder
	overflowed bool
}

// Write implements io.Writer.
func (w *boundedWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		w.overflowed = true
		// The bytes are counted as written so the remote command is not blocked
		// on a writer that refuses them; they are simply not kept.
		return len(p), nil
	}
	if int64(len(p)) > remaining {
		w.buffer.Write(p[:remaining])
		w.written = w.limit
		w.overflowed = true
		return len(p), nil
	}

	w.buffer.Write(p)
	w.written += int64(len(p))
	return len(p), nil
}

// text is the stream as a string, empty when it is not valid UTF-8: a step
// output is not where arbitrary binary belongs, and a cut at the byte limit can
// leave half a rune behind.
func (w *boundedWriter) text() string {
	value := w.buffer.String()
	if !utf8.ValidString(value) {
		return ""
	}
	return value
}
