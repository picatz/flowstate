// Package execpolicy decides which local programs a workflow may start, and
// starts the ones it admits: the policy and the runner behind the built-in
// `exec` task.
//
// # This is not a sandbox
//
// Read this before relying on anything else in the package. A program this
// package starts runs as the worker's user, with the worker's privileges, on
// the worker's machine. There is no namespace, no cgroup, no seccomp filter,
// no filesystem confinement and no network restriction. The deployment's egress
// policy (package netpolicy) governs requests the worker makes for a workflow;
// it does not govern what a child process connects to. The policy here decides
// what may be *started* (which executable, with what arguments, in which
// directory, with which environment, for how long, printing how much). What a
// started program then does is outside it.
//
// The consequence for an operator: enabling `exec` on a worker is granting
// every workflow that worker runs, and every identity the rules admit, the
// ability to run the listed programs as the worker. List programs the way a
// sudoers file lists them: narrowly, with absolute paths, and with no program
// that is itself an interpreter (a shell, `python`, `env`, `xargs`, `find
// -exec`) unless handing out arbitrary code execution is the intent. A
// deployment that needs confinement runs the worker, or a dedicated worker
// queue, inside the boundary it trusts (a container, a microVM, a separate
// host); isolation tiers are deliberately a later layer on top of this one.
//
// # What the policy does enforce
//
//   - Programs are named, never located by a workflow. argv[0] is a bare name
//     looked up only in the operator's table; PATH is never searched and a path
//     written by a workflow is refused.
//   - Each table entry is verified when the policy loads (absolute, present, a
//     regular executable file, not world-writable, symbolic links resolved) and
//     may be pinned to a SHA-256. On Linux the verified file is opened once and
//     executed through that descriptor, so replacing the path after the check
//     does not change what runs; see [Command.Run] for where that does not hold.
//   - The working directory must resolve, through symbolic links, to a place
//     under a configured root. The worker's own working directory is never used.
//   - The environment is built from nothing: operator literals, then listed
//     passthrough variables, then only those step variables the operator named.
//   - Run time and per-stream output are bounded by values the policy must set,
//     below fixed ceilings. A timeout or cancellation ends the whole process
//     group.
//   - Operator CEL rules, deny first, see the final resolved values; a rule that
//     cannot be evaluated denies.
//
// # What is deliberately not here yet
//
// Secret-valued environment, resource limits (rlimits), an opt-in for absolute
// program paths, isolation runners, and standard input. Standard input is
// always /dev/null.
package execpolicy
