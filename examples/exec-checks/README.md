# A program the operator allowed, not one the workflow chose

[`workflow.yaml`](workflow.yaml) runs `git --version` and `git rev-parse --git-dir`
with the built-in `exec:` task. The file only asks; [`exec-policy.yaml`](exec-policy.yaml)
decides.

```sh
flow run local examples/exec-checks/workflow.yaml --exec-policy examples/exec-checks/exec-policy.yaml
```

Without `--exec-policy` (or `FLOWSTATE_EXEC_POLICY`) every `exec:` step is denied with
an error naming the flag. The flag exists on the commands that run tasks (`worker`,
`run local`, `server dev`, `mcp`, `dap`, `task run`).

The policy's paths are the operator's: edit `/usr/bin/git` and the
`/var/lib/flowstate/workspaces` root for your machine and create the `demo`
directory the `workspace` input defaults to.

## What the policy fixes, and the workflow cannot choose

- **Which program.** `argv[0]` is a bare name looked up only in `executables`;
  no `PATH` search, and an absolute path in a workflow is refused.
- **Where.** `dir` is required, absolute, and must resolve (through symlinks) under a root.
- **The environment.** Built from nothing: operator `env`, then `env_passthrough`, then
  step `env:` only for keys in `env_authored`. Loader variables are refused.
- **How long and how much.** `timeout` and `max_output_bytes` are required and have
  ceilings (1h, 16MiB). On timeout the whole process group is killed.
- **Whether this run may.** CEL `allow`/`deny` rules see `argv`, `executable`, `name`,
  `dir`, `env_keys` and `identity`; deny wins and a rule that errors denies.

A nonzero exit is output (`exit_code`), not a failure, as an HTTP status is. A step
fails only on a policy denial, a program that cannot start, a timeout, or cancellation.

## What this is not

It is **not a sandbox**. The program runs as the worker's user with no namespace,
cgroup, seccomp or filesystem confinement, and the egress policy does not apply to what
it connects to. `roots` confines `dir` only: path words in `argv` are not confined, which
is why the policy allows exact argv shapes (`rev-parse --git-dir`, not `status`, `diff` or
`log`, which read repo-local config that can name a program to run) and denies `--output`, `-c`, `--no-index`,
absolute paths and `..` rather than allowing any arguments after a subcommand.

Deferred: secret-valued environment, resource limits, an absolute-path opt-in,
isolation tiers or remote runners, and stdin (it is `/dev/null`).

## Testing

[`workflow.test.yaml`](workflow.test.yaml) stubs `exec`, so `flow test` never starts a
process and an `exec:` step with no stub fails the case. The example harnesses that run
every example for real skip this one, because its policy is machine-specific; the task's
behavior on both drivers is covered by the shared conformance cases.
