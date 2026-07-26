// Package protocol is the wire contract between the plugin host and a plugin
// process: the environment a plugin is launched with, the single line it prints
// to announce itself, and the header it authenticates the host by.
//
// It is internal because it is one definition shared by two packages that must
// agree exactly — the host in [github.com/picatz/flowstate/pkg/flowstate/v1/plugin]
// and the author-facing SDK in .../plugin/sdk. Two copies of a handshake format
// drift, and a handshake that drifts fails at the least diagnosable moment. A
// plugin written in another language implements this from the documentation in
// the host package, not from this code.
//
// # The launch environment
//
// The host passes everything a plugin needs to serve in the environment, and a
// plugin that finds any of it missing must refuse to serve:
//
//	FLOWSTATE_PLUGIN_MAGIC_COOKIE      must equal MagicCookieValue
//	FLOWSTATE_PLUGIN_PROTOCOL_VERSIONS versions the host speaks, e.g. "1"
//	FLOWSTATE_PLUGIN_SOCKET            absolute path the plugin must listen on
//	FLOWSTATE_PLUGIN_TOKEN             per-launch secret the host will present
//	FLOWSTATE_PLUGIN_HOST_FD           fd that closes when the host exits
//
// # The handshake line
//
// The plugin prints exactly one line to stdout once it is listening, and then
// never writes to stdout again — everything else it says goes to stderr, which
// the host captures as that plugin's logs. Reserving stdout for one line is what
// keeps a plugin's own logging from corrupting the protocol.
//
//	FLOWSTATE-PLUGIN|1|1|unix|/var/folders/.../s
//
// The fields are the sentinel, the version of this handshake format, the
// negotiated protocol version, the network, and the address. The handshake
// format carries its own version separately from the protocol version so that
// the way a plugin announces itself can change without changing what it serves.
package protocol

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Environment variable names the host sets when it launches a plugin.
const (
	// MagicCookieEnv carries the cookie a plugin must find set to
	// [MagicCookieValue] before it serves anything.
	MagicCookieEnv = "FLOWSTATE_PLUGIN_MAGIC_COOKIE"

	// VersionsEnv carries the protocol versions the host speaks, as a
	// comma-separated list of decimal integers in no particular order.
	VersionsEnv = "FLOWSTATE_PLUGIN_PROTOCOL_VERSIONS"

	// SocketEnv carries the absolute path of the Unix domain socket the plugin
	// must listen on.
	//
	// The host chooses the path rather than letting the plugin report one, so
	// that the socket lands in a directory the host created and only the host's
	// user can enter. A plugin free to choose could put its socket somewhere
	// world-writable, and the filesystem permissions are what authenticate this
	// channel.
	SocketEnv = "FLOWSTATE_PLUGIN_SOCKET"

	// TokenEnv carries the per-launch secret the host will present in
	// [TokenHeader] on every request. A plugin must reject a request that does
	// not carry it.
	TokenEnv = "FLOWSTATE_PLUGIN_TOKEN"

	// HostFDEnv carries the number of an inherited file descriptor that the
	// operating system closes when the host process exits, whether or not the
	// host got the chance to clean up. A plugin reads it and exits on EOF, which
	// is what keeps a plugin from outliving a host that crashed.
	HostFDEnv = "FLOWSTATE_PLUGIN_HOST_FD"
)

// MagicCookieValue is the value [MagicCookieEnv] must hold.
//
// It exists so that a `flowstate-plugin-*` binary run directly by a curious
// human explains itself and exits, instead of printing a handshake line and then
// speaking a binary protocol into a terminal. Nobody sets this by accident.
//
// It is not a security measure and must not be treated as one: it is a constant
// compiled into every plugin, so anything that can read a plugin binary knows
// it. [TokenEnv] is the value that authenticates, and the socket's directory
// permissions are what actually keep other users out.
const MagicCookieValue = "flowstate-plugin-8f2b1c4e6a9d47f3b5e8c1a0d9f6b3e7"

// TokenHeader is the HTTP header carrying the per-launch secret from [TokenEnv]
// on every request the host makes.
//
// It is defense in depth behind the socket's permissions: a process that somehow
// reaches the socket — a bug in a directory mode, a plugin that re-listens
// somewhere else, another process running as the same user — still cannot
// impersonate the host without a secret that existed only for this launch.
const TokenHeader = "Flowstate-Plugin-Token"

// Sentinel is the first field of the handshake line, identifying what protocol
// the line belongs to. A process that prints anything else on its first line is
// not a Flowstate plugin, and saying so is a better diagnostic than a parse
// error about field counts.
const Sentinel = "FLOWSTATE-PLUGIN"

// HandshakeVersion is the version of the handshake line format itself, as
// distinct from the protocol version negotiated within it.
const HandshakeVersion = 1

// Version1 is the first version of the plugin protocol: the services defined in
// proto/flowstate/v1/plugin.proto, served over Connect on a Unix socket.
const Version1 = 1

// MaxHandshakeLine bounds the handshake line, because it is the first thing an
// untrusted process gets to say and the host reads it before it knows anything
// about the process at all. A plugin that never prints a newline must not be
// able to make the host allocate.
const MaxHandshakeLine = 4096

// NetworkUnix is the only network a plugin may serve on.
//
// A Unix socket rather than a TCP port: there is no port for anything to scan,
// nothing exposed on loopback for another process on the machine to reach, and
// filesystem permissions do the authentication. The field exists in the line so
// that adding a network later does not need a new handshake format, not because
// anything else is permitted today.
const NetworkUnix = "unix"

// HostVersions returns the protocol versions this build of the host speaks,
// highest preference last is not implied — [Negotiate] picks the highest common
// version.
func HostVersions() []int { return []int{Version1} }

// Handshake is what a plugin announces about itself once it is listening.
type Handshake struct {
	// HandshakeVersion is the version of the line format the plugin wrote.
	HandshakeVersion int

	// ProtocolVersion is the version the plugin chose from the ones the host
	// offered in [VersionsEnv].
	ProtocolVersion int

	// Network and Address are where the plugin is serving.
	Network string
	Address string
}

// String renders the handshake line, without a trailing newline.
func (h Handshake) String() string {
	return strings.Join([]string{
		Sentinel,
		strconv.Itoa(h.HandshakeVersion),
		strconv.Itoa(h.ProtocolVersion),
		h.Network,
		h.Address,
	}, "|")
}

// handshakeFields is how many fields a well-formed handshake line has.
const handshakeFields = 5

// ParseHandshake parses a handshake line.
//
// It is deliberately strict. This is the first thing an untrusted process says,
// and every check here is one the host would otherwise have to make later with
// less context: a line that does not parse cleanly is a plugin to refuse, not
// one to interpret generously.
func ParseHandshake(line string) (Handshake, error) {
	line = strings.TrimRight(line, "\r\n")

	if line == "" {
		return Handshake{}, fmt.Errorf("empty handshake line")
	}
	if len(line) > MaxHandshakeLine {
		return Handshake{}, fmt.Errorf("handshake line is longer than %d bytes", MaxHandshakeLine)
	}

	fields := strings.Split(line, "|")

	// The sentinel is checked before the field count, because the overwhelmingly
	// common cause of an unparseable line is a binary that is not a plugin at
	// all, and "is this a Flowstate plugin?" is a far more useful thing to tell
	// someone than a complaint about how many fields their program's greeting
	// has.
	if fields[0] != Sentinel {
		return Handshake{}, fmt.Errorf(
			"handshake line starts with %q, want %q — is this a Flowstate plugin?",
			truncate(fields[0], 64), Sentinel,
		)
	}

	if len(fields) != handshakeFields {
		return Handshake{}, fmt.Errorf(
			"handshake line has %d fields, want %d of the form %q",
			len(fields), handshakeFields, Sentinel+"|1|1|unix|/path/to/socket",
		)
	}

	handshakeVersion, err := parsePositive(fields[1])
	if err != nil {
		return Handshake{}, fmt.Errorf("handshake format version: %w", err)
	}

	protocolVersion, err := parsePositive(fields[2])
	if err != nil {
		return Handshake{}, fmt.Errorf("protocol version: %w", err)
	}

	network, address := fields[3], fields[4]
	switch {
	case network == "":
		return Handshake{}, fmt.Errorf("handshake line names no network")
	case address == "":
		return Handshake{}, fmt.Errorf("handshake line names no address")
	case network == NetworkUnix && !filepath.IsAbs(address):
		// A relative socket path would be resolved against whatever working
		// directory each side happens to have, which is exactly the ambiguity
		// this protocol should not contain.
		return Handshake{}, fmt.Errorf("socket address %q is not absolute", truncate(address, 128))
	}

	return Handshake{
		HandshakeVersion: handshakeVersion,
		ProtocolVersion:  protocolVersion,
		Network:          network,
		Address:          address,
	}, nil
}

// parsePositive parses a positive decimal integer, rejecting the forms
// strconv.Atoi accepts that a version number should not take, such as "+1".
func parsePositive(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", truncate(s, 32))
	}
	if n <= 0 {
		return 0, fmt.Errorf("%d is not a positive version", n)
	}
	if strconv.Itoa(n) != s {
		return 0, fmt.Errorf("%q is not a canonical number", truncate(s, 32))
	}
	return n, nil
}

// FormatVersions renders protocol versions for [VersionsEnv].
func FormatVersions(versions []int) string {
	parts := make([]string, 0, len(versions))
	for _, v := range versions {
		parts = append(parts, strconv.Itoa(v))
	}
	return strings.Join(parts, ",")
}

// MaxOfferedVersions bounds how many versions the host may offer, so that
// parsing the list in a plugin is bounded too. It is generous: a protocol with
// more than this many live versions has a different problem.
const MaxOfferedVersions = 32

// ParseVersions parses the value of [VersionsEnv].
//
// A plugin calls it on input from its own environment, which the host set — but
// a plugin cannot know it was launched by the host it thinks it was, so this is
// still bounded and still refuses malformed input rather than guessing.
func ParseVersions(s string) ([]int, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("no protocol versions offered")
	}

	parts := strings.Split(s, ",")
	if len(parts) > MaxOfferedVersions {
		return nil, fmt.Errorf("more than %d protocol versions offered", MaxOfferedVersions)
	}

	versions := make([]int, 0, len(parts))
	for _, part := range parts {
		v, err := parsePositive(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("protocol version list %q: %w", truncate(s, 128), err)
		}
		versions = append(versions, v)
	}

	return versions, nil
}

// Negotiate returns the highest version present in both lists.
//
// The host declares what it speaks and the plugin picks; a plugin that finds
// nothing in common must say so and exit rather than serving a version it does
// not implement, so that the mismatch is one clear message at startup instead of
// an unexplained failure on some request later.
func Negotiate(offered, supported []int) (int, bool) {
	best, found := 0, false
	for _, o := range offered {
		for _, s := range supported {
			if o == s && o > best {
				best, found = o, true
			}
		}
	}
	return best, found
}

// truncate bounds text before it goes into an error message. Everything this
// package parses came from another process, and an error naming what was wrong
// with it must not be able to carry a megabyte of that process's choosing.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Cut on a rune boundary: this bounds text another process chose, and a
	// broken rune in a log line is a line some consumer will refuse to parse.
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}
