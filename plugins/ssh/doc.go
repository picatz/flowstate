// Command flowstate-plugin-ssh runs operator-defined commands on
// operator-defined hosts.
//
// # The refusal this plugin is an answer to
//
// A generic SSH task - address, command, key, all from the Flowfile - is
// remote arbitrary code execution with the workflow choosing the machine, the
// program and the identity. This repository declined to build that, and the
// reasons were right: host-key policy, private-key custody, and an outcome
// nobody can resolve when a connection drops mid-command.
//
// What is wrong with that shape is not SSH. It is that the *Flowfile names the
// authority*. Every part of the refusal dissolves when the operator names it
// instead, which is the same move #1348 makes for a container's mounts - grant
// identifiers resolved by operator configuration, never arbitrary host paths
// from a workflow - and the same one plugins/sql makes for a query, where every
// value is bound and none is ever interpolated into SQL text.
//
// So: an operator writes a grants file. A host grant names an address, the user
// to connect as, the private key to use, and the exact public keys that host
// may present. A command grant names an argv - a program and its arguments,
// with placeholders - and the pattern each placeholder's value must match. A
// Flowfile picks one host grant, one command grant the host permits, and fills
// the placeholders. It cannot name an address, a user, a key, a port, or a
// program, because none of those are inputs.
//
// # What the contract buys, in the order the refusal raised it
//
// Host-key policy: there is no trust-on-first-use and no known_hosts file. A
// host grant lists the public keys that host may present, and a host presenting
// anything else is refused before authentication, so a redirected or
// impersonated host is a failure rather than a successful command somewhere
// else.
//
// Key custody: a private key never crosses this plugin's boundary as data. A
// Flowfile cannot supply one, and no task input carries one; the operator's
// grant names a file the worker can read. An SSH identity is host authority
// rather than a per-call credential, and the operator who granted the host
// chose what it acts as.
//
// Unknown outcomes: a failure before the exec request is a definite no-run and
// is retryable. A failure after it - a dropped connection, a timeout - is
// [sdk.OutcomeUnknown], because the command may be running on the far side
// right now. Nothing here retries it, and the README says why.
//
// # What the session is, and is not
//
// One session channel, one exec request, no PTY, no agent forwarding, no port
// or X11 forwarding, no subsystem, no shell. Those are not flags this plugin
// leaves off by default; nothing here requests them, and a grant cannot ask
// for them.
//
// The command line an exec request carries is a string the remote shell parses,
// so every argument the operator wrote and every parameter a workflow filled is
// single-quoted before it is joined. A parameter is checked against its own
// pattern first, and quoted second, on the principle that one of those should
// be redundant: a value that got past the pattern still cannot become a second
// command, a flag, or a redirection.
//
// # Tenancy
//
// A host grant may name the namespaces allowed to spend it. The namespace is
// the one the host established for the calling workload, not one the workload
// declared, so a grant listing namespaces is a grant one tenant's workflows
// cannot reach from another's.
package main
