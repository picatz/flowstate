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
//	FLOWSTATE_PLUGIN_PROTOCOL_VERSIONS versions the host speaks, e.g. "5"
//	FLOWSTATE_PLUGIN_SOCKET            absolute path the plugin must listen on
//	FLOWSTATE_PLUGIN_TOKEN_FD          fd carrying the per-launch secret
//	FLOWSTATE_PLUGIN_HOST_FD           fd that closes when the host exits
//	FLOWSTATE_EGRESS_POLICY_B64        the deployment's egress policy, base64
//	HTTP_PROXY HTTPS_PROXY NO_PROXY    only when the policy proxies (see below)
//
// The secret itself is never in the environment; only the number of the
// descriptor carrying it is. See [TokenFDEnv] and [ReadToken].
//
// The last line is not a protocol variable. The proxy variables belong to Go's
// own [net/http.ProxyFromEnvironment] and to every other HTTP stack that reads
// them, and they are here because they are *granted* alongside the policy rather
// than inherited: a plugin's environment is built from nothing, so a policy that
// says "proxy from the environment" would find no environment to proxy from.
// See [ProxyEnv].
//
// The egress grant is set whenever the deployment configured a policy, and is
// present-but-empty when that policy is an empty document — which is a policy,
// and a different fact from the variable being absent. See [EgressPolicyEnv].
//
// # The handshake line
//
// The plugin prints exactly one line to stdout once it is listening, and then
// never writes to stdout again — everything else it says goes to stderr, which
// the host captures as that plugin's logs. Reserving stdout for one line is what
// keeps a plugin's own logging from corrupting the protocol.
//
//	FLOWSTATE-PLUGIN|1|5|unix|/var/folders/.../s
//
// The fields are the sentinel, the version of this handshake format, the
// negotiated protocol version, the network, and the address. The handshake
// format carries its own version separately from the protocol version so that
// the way a plugin announces itself can change without changing what it serves.
package protocol

import (
	"fmt"
	"io"
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

	// TokenEnv carried the per-launch secret directly, up to [Version3]. The
	// host no longer sets it, and a plugin must not read it.
	//
	// It moved to [TokenFDEnv] because an environment variable is not a place a
	// secret can be withdrawn from. On Linux /proc/<pid>/environ shows the block
	// the kernel copied at execve(2); a later unsetenv edits the process's own
	// copy and changes nothing that file shows. The token was therefore readable
	// for the plugin's whole life — to root, to anything that can ptrace it, and
	// to any tool that sweeps environments into a diagnostic bundle or a core
	// dump — rather than for the startup window the SDK's comment claimed.
	//
	// The name stays reserved rather than deleted, for the reason [Version1]'s
	// number does: an operator entry spelling it is still refused, so the name
	// cannot come back meaning something new.
	TokenEnv = "FLOWSTATE_PLUGIN_TOKEN"

	// TokenFDEnv carries the number of an inherited file descriptor holding the
	// per-launch secret the host will present in [TokenHeader] on every request.
	// A plugin must reject a request that does not carry it.
	//
	// The host writes one [ReadToken] line and closes its end before the plugin
	// starts, so a plugin reads to EOF without waiting on anything. What the
	// descriptor held is gone once read: it exists in kernel buffer space, not
	// in any file, and not in the environment block execve(2) copied.
	TokenFDEnv = "FLOWSTATE_PLUGIN_TOKEN_FD"

	// HostFDEnv carries the number of an inherited file descriptor that the
	// operating system closes when the host process exits, whether or not the
	// host got the chance to clean up. A plugin reads it and exits on EOF, which
	// is what keeps a plugin from outliving a host that crashed.
	HostFDEnv = "FLOWSTATE_PLUGIN_HOST_FD"

	// EgressPolicyEnv carries the deployment's egress policy — the exact bytes
	// the worker parsed for the built-in http task — base64-encoded, to every
	// plugin the host launches.
	//
	// It is a grant rather than an inheritance. A plugin's environment is built
	// from nothing (see the host's pluginEnv), so a plugin that reaches the
	// network is governed by a policy the operator wrote and the worker handed
	// it, not by whatever the worker's own environment happened to contain. One
	// name for every plugin, because a per-plugin name is a per-plugin decision
	// about whether to make the grant at all, and the answer is always yes.
	//
	// Presence is the grant, not length. A deployment whose policy file is an
	// empty document configured a policy — the one an empty document builds,
	// which is what the built-in http task runs under in that case — so the
	// variable is set to the empty string rather than left out. Left out means
	// only that nothing granted anything, which is why the reader can fail
	// closed on it. os.Getenv cannot tell those apart; os.LookupEnv can, and is
	// what both sides use.
	//
	// A worker with no operator policy configured grants its own default,
	// written out as a document marked `deployment_default: true`
	// (flowstatev1.DefaultEgressPolicyDocument), rather than leaving the
	// variable out. So under `flow` the variable is always set, and unset means
	// only that whatever launched this process is not a Flowstate worker. A
	// plugin that reads the marker can take a posture toward the default —
	// `sql` refuses a database under it, everything else accepts it — which is
	// a decision it could not make if the default and no grant at all arrived
	// as the same thing (#1332).
	//
	// [github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk.EgressPolicy] is
	// what reads it. Nothing here obliges a plugin to; a plugin opening sockets
	// without it is doing so deliberately, which is the line ARCHITECTURE.md
	// draws about voluntary enforcement in vetted code.
	EgressPolicyEnv = "FLOWSTATE_EGRESS_POLICY_B64"
)

// MaxEgressPolicyBytes bounds the raw policy carried in [EgressPolicyEnv],
// before base64 encoding.
//
// The launch environment is passed through execve(2), and Linux bounds a single
// environment string at MAX_ARG_STRLEN — 128 KiB, and not configurable. Base64
// expands by 4/3, so 64 KiB of policy becomes 87,384 bytes plus the 28-byte
// name: comfortably under the limit, with room for the encoding to grow before
// anything has to be reconsidered. Past the limit exec fails with an errno that
// names neither this variable nor the policy, and every plugin on the worker
// stops launching for a reason nobody can read off the error.
//
// It is also simply a bound on input (AGENTS.md's fifth invariant): a policy is
// configuration, not a data transport, and 64 KiB is ample for the rules
// netpolicy supports while refusing a file handed over by accident.
//
// One ceiling, three enforcement points, because each is a boundary someone can
// arrive at without passing the others: the CLI reading the operator's file
// (cmd/flow/egress.go), the host accepting a Config from a program that embeds
// it (plugin.Config.validate), and the SDK reading the grant out of an
// environment it did not build (sdk.EgressPolicy).
const MaxEgressPolicyBytes = 64 << 10

// ProxyEnv returns the environment variable names [net/http.ProxyFromEnvironment]
// reads, in both the uppercase and lowercase spellings it accepts.
//
// These are granted, not protocol. Nothing in this package sets or reads them;
// they are ordinary variables that every HTTP stack already understands, and the
// host forwards the worker's own values verbatim — and only when the deployment's
// policy has `proxy_from_environment` enabled, because that is the operator
// saying the proxy is part of how this deployment reaches the network.
//
// Without the grant a plugin dials directly while the worker's built-in http
// task proxies: the plugin's environment is built from nothing, so
// ProxyFromEnvironment inside it finds nothing and returns no proxy. On a
// deployment whose egress leaves through a mandatory proxy that is not a
// difference in routing, it is the plugin going around the control — silently,
// and only for plugins.
//
// They are deliberately not in [MagicCookieEnv]'s company in the host's
// isProtocolEnv list: an operator who names a proxy in Config.Env is being more
// specific than the worker's own environment, and that entry wins rather than
// being dropped.
//
// REQUEST_METHOD is not here. ProxyFromEnvironment also consults it — a
// non-empty value means the process is a CGI script, and HTTP_PROXY is then
// ignored as untrusted — and forwarding it would let the worker's environment
// turn a plugin's proxy off in a way no operator wrote down.
func ProxyEnv() []string {
	return []string{
		"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
		"http_proxy", "https_proxy", "no_proxy",
	}
}

// MagicCookieValue is the value [MagicCookieEnv] must hold.
//
// It exists so that a `flowstate-plugin-*` binary run directly by a curious
// human explains itself and exits, instead of printing a handshake line and then
// speaking a binary protocol into a terminal. Nobody sets this by accident.
//
// It is not a security measure and must not be treated as one: it is a constant
// compiled into every plugin, so anything that can read a plugin binary knows
// it. The per-launch secret from [TokenFDEnv] is the value that authenticates,
// and the socket's directory permissions are what actually keep other users out.
const MagicCookieValue = "flowstate-plugin-8f2b1c4e6a9d47f3b5e8c1a0d9f6b3e7"

// TokenHeader is the HTTP header carrying the per-launch secret from
// [TokenFDEnv] on every request the host makes.
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

// Version1 was the first version of the plugin protocol. It is no longer served.
//
// Its services lived in the `flowstate.v1` package, so every Connect route was
// `/flowstate.v1.PluginService/…`. Moving the protocol to a package of its own
// changed all of them, and a set of routes is what a protocol version names — so
// that move ended version 1 rather than editing it.
//
// Keeping the number would have been the quiet failure. A plugin built against
// the old routes offers version 1, the host offers version 1, negotiation agrees,
// the handshake succeeds — and then the first `Describe` goes to a route that
// plugin does not serve. The error surfaces as a plugin that cannot be described,
// which reads as a broken plugin rather than a version mismatch, at the far end
// of a launch from the thing that was actually wrong.
//
// Retired rather than deleted, for the reason `Task.description`'s field number
// is reserved rather than reused: a number that meant something else must never
// come back meaning something new.
const Version1 = 1

// Version2 was the second version of the plugin protocol: the services defined
// in proto/flowstate/plugin/v1/plugin.proto, served over Connect on a Unix
// socket, with every route under `/flowstate.plugin.v1.`. It is no longer
// served.
//
// The routes are unchanged from it. What ended version 2 is the other half of
// what a plugin and host agree on: the descriptor exchange. A plugin ships
// descriptors for its own messages and omits every file the engine is known to
// have, deriving that set by walking flowstate's schema. Splitting
// flowstate/v1/flowstate.proto into twelve files (#658) changed that set on
// both sides at once, and neither side can read the other's:
//
//   - a version 2 plugin imports flowstate/v1/flowstate.proto and does not ship
//     it, and a host built after the split does not have that path;
//   - a plugin built after the split imports flowstate/v1/value.proto and does
//     not ship it, and a version 2 host does not have that path either.
//
// So the incompatibility runs in both directions, and neither is expressible as
// a route that is missing — it is the *contents* of a manifest that stopped
// being mutually readable.
//
// Retired rather than deleted, for the reason [Version1] is: a number that
// meant something else must never come back meaning something new.
const Version2 = 2

// Version3 was the third version of the plugin protocol: the same services and
// routes as [Version2], with the descriptor exchange speaking the twelve-file
// flowstate/v1 schema rather than the single flowstate/v1/flowstate.proto. It is
// no longer served.
//
// The number moves because the compatibility it asserts stopped being true.
// Nothing in the route table changed, and it would have been easy to leave the
// version at 2 on that basis — which is exactly the quiet failure [Version1]'s
// doc describes, arriving one step later. Both sides negotiate 2, the handshake
// succeeds, the plugin loads, and the *first* task manifest fails to
// reconstruct with an error about an import path. The operator has been told
// everything is fine and then handed a descriptor problem, deep in a launch,
// for what is really a "these two builds cannot work together".
//
// Moving the number turns that into one refusal at startup, in terms of the
// thing that actually changed, from whichever side is older — and the older
// side refuses using code that already shipped, which is the only way to reach
// a host that predates this change. A version that does not move across a
// breaking change is a version that is lying.
//
// What ended version 3 is the launch environment rather than anything on the
// wire: the per-launch secret moved out of [TokenEnv] onto the descriptor
// [TokenFDEnv] names. Retired rather than deleted, for the reason [Version1] is.
const Version3 = 3

// Version4 was the fourth version of the plugin protocol: the same services and
// routes as [Version3], with the per-launch secret delivered on an inherited
// descriptor ([TokenFDEnv]) instead of in the environment ([TokenEnv]).
//
// The number moves because the launch contract is half of what the two sides
// agree on, and this half stopped being mutually satisfiable. A version 3 plugin
// looks for a variable a version 4 host does not set; a version 4 plugin looks
// for a descriptor a version 3 host does not pass. Neither is expressible as a
// route, and neither side can fix it by being generous — a host that kept
// setting the variable for old plugins would still be leaving the token in
// /proc/<pid>/environ, which is the entire defect.
//
// Left at 3 it would fail the way [Version1]'s doc describes: negotiation
// agrees, the plugin loads, and then it either refuses to start over a variable
// name — which reads as a misconfigured deployment rather than two builds that
// cannot work together — or, for an implementation less careful than this SDK,
// serves with no token and rejects every request the host makes as
// unauthenticated. Moving the number turns both into one refusal at startup
// naming two versions, from whichever side is older.
//
// It is no longer served. What ended it is the launch environment again: the
// deployment's egress policy in [EgressPolicyEnv] was added to it, and version 4
// had already shipped without that variable.
//
// Retired rather than deleted, for the reason [Version1] is.
const Version4 = 4

// Version5 is the current version of the plugin protocol: the same services and
// routes as [Version4], with the deployment's egress policy carried in the
// launch environment under [EgressPolicyEnv] (#1332).
//
// The grant was very nearly folded into version 4, on the reasoning that both
// changes are launch-environment changes landing in the same release. That was
// wrong, and the way it was wrong is worth keeping: version 4 had *already
// shipped* — #1389 merged it before the grant existed — so a host and a plugin
// both built from that point negotiate 4 and neither knows about the variable,
// while a host built after the grant also negotiates 4 and does set it. One
// number would then have named two different launch contracts, which is exactly
// what a version exists to prevent. A version is not a release note; it names
// what the two sides may assume about each other, and it can only be spent once.
//
// Left at 4 the failure is the quiet one every retired version's doc describes,
// pointed at an authorization boundary: negotiation agrees, the plugin loads,
// and then a plugin built before the grant reaches the network with no policy
// where its operator configured one — no error, no refusal, just an egress
// control that is not there. Moving the number makes that pairing refuse at the
// handshake, naming both numbers, from whichever side is older.
//
// The grant's *contents* are a separate question from its presence. A later
// change to what the policy snapshot carries — marking the deployment default,
// which #1332's decision leaves to PR B — decides its own version question on
// its own terms; nothing here settles it in advance.
const Version5 = 5

// MaxHandshakeLine bounds the handshake line, because it is the first thing an
// untrusted process gets to say and the host reads it before it knows anything
// about the process at all. A plugin that never prints a newline must not be
// able to make the host allocate.
const MaxHandshakeLine = 4096

// MaxTokenBytes bounds what [ReadToken] will read, newline included.
//
// A plugin cannot know it was launched by the host it thinks it was, so the
// descriptor it is handed is input like any other and gets a limit. The bound is
// generous against the 26 bytes crypto/rand's text form actually mints and small
// against a pipe's buffer, so a writer that never sends a newline is a refusal
// rather than a plugin sitting on an allocation.
const MaxTokenBytes = 512

// WriteToken writes the per-launch secret in the framing [ReadToken] expects:
// the token, then one newline, then nothing.
//
// The host calls this on the write end of the pipe whose read end the plugin
// inherits on [TokenFDEnv], and closes that end before the plugin starts. The
// framing lives beside its reader because a launch contract whose two halves are
// spelled in two packages is one that drifts.
func WriteToken(w io.Writer, token string) error {
	if _, err := io.WriteString(w, token+"\n"); err != nil {
		return fmt.Errorf("writing the plugin token: %w", err)
	}
	return nil
}

// ReadToken reads the per-launch secret from the descriptor [TokenFDEnv] names.
//
// It reads to EOF rather than one line, which is what makes the framing strict:
// the host writes exactly one newline-terminated token and closes its end, so
// anything after that newline means this descriptor is not carrying what the
// protocol says it carries, and the plugin refuses instead of serving on a
// secret it half-understands.
//
// The result goes into a [TokenHeader] comparison, so every byte must be one a
// header value can hold. A token that could not travel in the header would make
// every request from the host look unauthenticated — a plugin that appears
// broken, for a reason nothing in its logs would name.
func ReadToken(r io.Reader) (string, error) {
	raw, err := io.ReadAll(io.LimitReader(r, MaxTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("reading the plugin token: %w", err)
	}

	if len(raw) > MaxTokenBytes {
		return "", fmt.Errorf("the plugin token is longer than %d bytes", MaxTokenBytes)
	}

	// Exactly one newline, at the end. A second line means this descriptor is
	// carrying something other than what the protocol says, and taking the first
	// line of it anyway would be the generous reading a launch contract cannot
	// afford.
	line := string(raw)
	if line == "" || strings.IndexByte(line, '\n') != len(line)-1 {
		return "", fmt.Errorf("the plugin token is not one newline-terminated line")
	}

	token := line[:len(line)-1]
	if token == "" {
		return "", fmt.Errorf("the plugin token is empty")
	}

	for i := range len(token) {
		// Visible ASCII only: the printable range a header value may hold,
		// excluding the space that would let a token carry structure.
		if token[i] < '!' || token[i] > '~' {
			return "", fmt.Errorf(
				"the plugin token holds a byte that cannot travel in %s", TokenHeader,
			)
		}
	}

	return token, nil
}

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
//
// [Version1] through [Version4] are absent because they are not served. A plugin
// built against any of them finds no version in common and refuses at startup
// with a message naming both sides, which is the failure this list exists to
// produce: one clear refusal before anything runs, rather than a request to a
// route nobody answers, a manifest nobody can reconstruct, a token nobody
// delivered, or a network reached under no policy at all.
//
// A retired version is left out rather than offered alongside the current one
// deliberately. Offering it would let a plugin negotiate successfully and fail
// later — at descriptor linking for version 2, at reading a secret that is not
// where it looked for version 3, at reaching the network ungoverned for version
// 4 — which is precisely the failure each bump exists to prevent. A version that
// cannot work must not be offered.
func HostVersions() []int { return []int{Version5} }

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
