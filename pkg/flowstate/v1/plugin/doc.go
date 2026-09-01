// Package plugin runs Flowstate plugins: separate processes that extend the
// engine, discovered on a configured path, launched and supervised by this
// package, and spoken to over Connect RPC on a Unix domain socket.
//
// A plugin is someone else's code running inside a worker that holds credentials
// and can reach internal networks. Out of process, a panic in it does not take
// the worker down, its dependencies cannot conflict with the engine's, and a bug
// in it cannot read the worker's memory. Those become the operating system's
// problem, which is where isolation actually exists. The protocol is the schema
// in proto/flowstate/plugin/v1/plugin.proto, so a plugin may be written in any language
// with Connect or gRPC support and the engine loads nothing to talk to one.
//
// # Using it
//
// A worker builds a [Host] over the directories it will accept plugins from,
// opens it, and takes adapters from it:
//
//	host, err := plugin.NewHost(plugin.Config{
//		SearchPath: []string{"/usr/local/lib/flowstate/plugins"},
//		Logger:     logger,
//	})
//	if err != nil {
//		return err
//	}
//	defer host.Close(context.Background())
//
//	if err := host.Open(ctx); err != nil {
//		return err
//	}
//
//	if err := host.Register(flowstatev1.DefaultRegistry(), providers); err != nil {
//		return err
//	}
//
// [flowstatev1.DefaultRegistry] specifically, and not a registry made for the
// occasion. Every surface that looks a task up — execution dispatch, the split
// between inputs the engine resolves and inputs the task does, whether a step
// ships prior outputs, and validation — reaches it through a package-level
// function over the default registry. A host registered anywhere else is a host
// whose plugins launched, passed their health checks, and answer `unknown task`.
//
// [Host.Close] must run. It is what kills the plugin processes; nothing else
// does.
//
// # What is refused, and why it is refused rather than worked around
//
// Every one of these is a configuration error that would otherwise become a
// runtime surprise:
//
//   - A search path entry that is relative, or writable by anyone other than
//     its owner — group-writable as well as world-writable. A plugin directory
//     is arbitrary code execution; a directory anyone can write to is arbitrary
//     code execution by anyone, and a group is a list of users this process does
//     not curate. See [Config.AllowInsecureSearchPath] for the escape hatch and
//     what it costs.
//   - A search path entry, or a plugin binary, reached through a directory
//     another identity owns or can write. Whoever owns `/opt` can rename
//     `/opt/plugins` and put their own directory there, whatever the
//     permissions on the directory that used to be at that path, so the
//     ownership question is asked of every component of the path and not only
//     of its last one. On a platform with no POSIX ownership none of this can
//     be decided, and discovery says so in a warning rather than reporting a
//     check that did not run as one that passed.
//   - A binary that does not handshake within [Config.HandshakeTimeout]. It is
//     killed rather than waited on.
//   - A handshake naming a protocol version the host did not offer, or an
//     address other than the socket the host assigned.
//   - A manifest that does not satisfy its protovalidate rules, or that
//     advertises no capabilities. A plugin that can do nothing is a live process
//     with nothing to do.
//   - Two plugins claiming one secret scheme. Two answers for one scheme is a
//     configuration error, and resolving it by load order means the answer
//     depends on directory iteration.
//   - A scheme not in [Config.PermittedSchemes], when that is set. A deployment
//     that lists what it permits gets exactly that and nothing a newly dropped-in
//     binary adds.
//   - A binary whose digest is not the one [Config.PinnedDigests] declared for
//     the name it answers to. Refused before the process is started, so nothing
//     the plugin says about itself is part of the decision. Digest pinning is
//     admission of *bytes*: it says these exact bytes and is deliberately silent
//     about provenance — who built them, and whether anyone vouches for them.
//     Whether a handshake should carry a signature instead, and what a
//     deployment would trust to verify one, is open (#146).
//
// # The handshake, end to end
//
// The host creates a directory only its own user can enter, mode 0700, and
// assigns a socket path inside it. It launches the binary with an explicit argv
// and no shell, in its own process group, with an environment carrying: a magic
// cookie, the protocol versions the host speaks, the socket path, and the
// numbers of two inherited file descriptors — one carrying a per-launch secret
// generated from crypto/rand, one that closes when the host exits.
//
// That environment also carries the deployment's egress policy, base64-encoded,
// whenever the deployment configured one — the grant every plugin receives, the
// same bytes the built-in http task runs under, bounded at 64 KiB before
// encoding. Set-but-empty is a policy whose document is empty; unset is no
// grant, and the reader must fail closed on it rather than treat it as
// permission. It is part of the launch environment protocol version 4 names, so
// a plugin that predates it is refused at the handshake rather than launched
// ungoverned. See [Config.EgressPolicy] and the sdk package's EgressPolicy.
//
// The secret is on a descriptor rather than in the environment, and any language
// that can inherit one can read it: the host writes the token and a single "\n"
// to a pipe, closes its end before the plugin starts, and the plugin reads that
// descriptor to EOF, bounded, takes everything before the newline, and closes
// it — so the token does not stay reachable through /proc/<pid>/fd, and does not
// travel to anything the plugin itself launches. There is nothing to wait for
// and nothing to write back. What made the environment the
// wrong place is that a variable cannot be withdrawn — on Linux
// /proc/<pid>/environ shows the block the kernel copied at execve(2), so a
// secret delivered there is readable for as long as the plugin runs, no matter
// how promptly the plugin unsets it, and it is collected by anything that sweeps
// environments into a diagnostic bundle or a core dump.
//
// The plugin checks the cookie. Without it — someone ran the binary from a
// shell — it prints an explanation to stderr and exits, rather than printing a
// handshake line and speaking a binary protocol into a terminal. It then picks
// the highest protocol version it shares with the host, or exits saying it
// cannot serve any of them; a plugin built against a host that still put the
// secret in the environment shares no version with this one, and each side
// refuses at that point naming both numbers rather than failing later over a
// credential that is not where it looked. It reads the token, listens on the
// assigned path, sets the socket to mode 0600, and prints one line to stdout:
//
//	FLOWSTATE-PLUGIN|1|2|unix|/var/folders/.../s
//
// After that line the plugin never uses stdout again; everything else it says
// goes to stderr, which the host reads line by line and logs attributed to that
// plugin. The host bounds the wait for that line, bounds its length, and checks
// every field: the sentinel, a handshake format version it understands, a
// protocol version it offered, and the exact socket path it assigned.
//
// Then it dials the socket and calls Describe, validates the manifest with
// [github.com/picatz/flowstate/pkg/flowstate/v1.Validate], and refuses the
// plugin if it does not validate or advertises nothing. Every subsequent request
// carries the per-launch secret as a header, so a process that somehow reaches
// the socket still cannot impersonate the host.
//
// The socket's mode is defense in depth rather than the defense. Linux checks
// write permission on a socket file, but several BSD-derived systems — macOS
// among them — do not, and enforce only the traversal permission of the
// directory containing it. The 0700 directory is therefore what actually keeps
// other users out on every platform this runs on, and the 0600 socket is the
// second line for the platforms that honor it.
//
// # Lifecycle in both directions
//
// [Host.Close] signals each plugin's process group, waits, and escalates to a
// kill: a process group rather than a process, because a plugin that spawned
// children of its own would otherwise leave them behind. In the other direction
// the host holds the write end of a pipe whose read end the plugin inherited, so
// a host that crashes without running any cleanup still closes that descriptor,
// and the plugin sees EOF and exits. Neither side depends on the other behaving
// well.
//
// A plugin that exits on its own is relaunched with exponential backoff, capped
// at [Config.MaxRestarts]. One that crashes on every launch stops being
// relaunched and is reported through [Host.Plugins] rather than being retried
// forever. A relaunched plugin must describe itself the same way it did before:
// a plugin that comes back claiming different schemes or different tasks is
// refused, because adapters already handed to the engine are bound to what it
// claimed the first time.
//
// # Bounds
//
// A plugin is not trusted because an operator installed it. Every call is bound
// by a timeout and by connect.WithReadMaxBytes, the handshake by a timeout and a
// line length, captured stderr by a line length, and reconstructed descriptors
// by size and file count. See [Config] for the knobs and their defaults.
//
// # Writing a plugin
//
// Use the SDK in [github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk],
// which does the handshake, the socket, the server, the token check, and the
// signal handling, so that a plugin is a manifest and its implementations. The
// worked example under pkg/flowstate/v1/plugin/examples/flowstate-plugin-example advertises both
// capabilities. A plugin in another language implements what is described above;
// the format is documented here so it can be, and the SDK is a convenience
// rather than a requirement.
//
// # Telemetry boundary
//
// Host metrics deliberately have bounded dimensions: plugin name, task name,
// operation, outcome, and health status. A plugin may use those same attributes.
// Workflow, run, and step identifiers may appear on spans and structured logs,
// but never as metric attributes. W3C trace context crosses the local Connect
// transport. Baggage is reduced to host-created plugin and task names; arbitrary
// caller baggage, credentials, resolved secrets, and authorization scopes never
// cross the process boundary.
package plugin
