# flowstate-plugin-sql

Ordinary SQL for a workflow: `sql.query` (bounded, parameterized reads) and
`sql.exec` (one or more statements as a single transaction), over
[PostgreSQL](https://github.com/jackc/pgx). The pure-Go SQLite driver remains
compiled for hermetic package tests but is refused by the distributed plugin:
an embedded database is worker-filesystem authority, not a network connection
that egress policy can confine. Both drivers are chosen so
this plugin never links cgo and never execs a database client binary. See
[`doc.go`](doc.go) for the full design: why every value in `params:` is
bound rather than ever interpolated into SQL text, what this plugin could
and could not bound on the wire per engine, and why a transaction can never
outlive the activity that opened it.

This is the connectivity family's first member (issue #181) - the proof
that flowstate runs ordinary business workloads ("read something, decide,
write something, tell someone") and not only developer tooling.

An example that runs both tasks lives at
[`examples/plugins/sql`](../../examples/plugins/sql); read that first if you
want to see it work rather than read about it. Two files live there: a read
(`workflow.yaml`, `sql.query`) and a write (`transfer.yaml`, `sql.exec`) -
both require a real database and a resolvable `dsn` secret, so neither runs
by accident.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-sql ./plugins/sql
```

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `sql.query` | reads | yes (a `SELECT` has no side effect to repeat) | always |
| `sql.exec` | writes | only if the statements and their params are (see "Transactions," below) | always |

### Execution-mode posture

`sql.exec` deliberately writes in local rehearsals. A rehearsal against a
development database is useful only when it exercises the same transaction and
backend semantics as durable execution; it is not a dry run. Execution mode is
therefore not treated as database authorization. Task policy, secret policy,
and egress policy separately decide whether the task, DSN, and destination are
available. `TestSQLExecCommitsEveryStatementTogether` performs a
credential-free SQLite mutation with no production caller and verifies the
committed state, preserving that contract.

## Parameterized only, structurally

`query:` (and each statement's own `sql:` under `sql.exec`) takes SQL text.
`params:` is a **separate** list of values, bound to that text's own
placeholders - `?` for sqlite, `$1`, `$2`, ... for postgres - by the driver
itself. There is no function anywhere in this plugin that appends a value
to a query string or formats one into it; see [`params.go`](params.go)'s
own doc comment on `paramsToArgs`. A CEL expression written into `params:`
therefore produces a *parameter value*, never a SQL fragment, no matter
what that value contains:

<!-- example: examples/plugins/sql/workflow.yaml -->
```yaml
edition: v2026.3
name: sql-query
description: Reads bounded, typed rows from PostgreSQL using the "sql" plugin's sql.query task - a parameterized WHERE clause, a required row bound, and a result a later step can filter with CEL.

# sql.query is one of two tasks the "sql" plugin provides; the other,
# sql.exec, writes one or more statements as a single transaction - see
# transfer.yaml. The dot in "sql.query" is what marks this as a plugin task
# rather than a built-in: no built-in task has one. The engine has never
# compiled sql.v1.QueryInputs; it learns the shape from descriptors this
# plugin ships in its manifest at launch. See plugins/sql for the source and
# plugins/sql/README.md for what this plugin needs configured on the worker
# before this file can run for real (a PostgreSQL database with an
# "accounts" table, SQL_DSN naming it, and an operator egress policy).
#
# Requires configuration - a real database and a resolvable dsn secret - so
# it never runs by accident. See plugins/sql/README.md, "Trying this
# example."
vars:
  min_balance_cents: 0
steps:
  - id: accounts
    sql.query:
      # A secret reference, resolved inside the task, never a literal
      # connection string - see plugins/sql/README.md, "Secrets," for what
      # SQL_SECRET_DSN (or whatever this deployment's provider names it)
      # resolves to. Literal DSNs are refused by the plugin host.
      dsn: ${secret('env:SQL_DSN')}
      engine: ENGINE_POSTGRES
      # Required: there is no default. A result with more rows than this
      # is refused outright, naming the bound, rather than returned as a
      # silently truncated prefix - see plugins/sql/doc.go, "Bounded
      # results."
      max_rows: 1000
      # The only way a value from vars reaches the database: bound as a
      # parameter, never spliced into query text above - see
      # plugins/sql/doc.go, "Parameterized only, structurally."
      params:
        - ${vars.min_balance_cents}
      query: SELECT id, name, balance_cents FROM accounts WHERE balance_cents >= $1 ORDER BY id
  - id: announce
    log:
      # rows is a list of maps CEL can filter and index by column name,
      # exactly like any other step output - the #177 convergence
      # plugins/sql/doc.go calls out as this plugin's headline property.
      # filter() here is the same expression style QueryOutputs.rows's own
      # doc comment in sql.proto uses as its worked example.
      message: ${"%d account(s) at or above %d cents; %d of them are exactly at zero".format([steps.accounts.row_count, vars.min_balance_cents, steps.accounts.rows.filter(r, r.balance_cents == 0).size()])}
outputs:
  accounts:
    value: ${steps.accounts.rows}
    description: every account row at or above min_balance_cents, typed for a later step's own CEL expression
  row_count:
    value: ${steps.accounts.row_count}
    description: how many rows came back - never more than max_rows, and never a truncated prefix of more that did
```

`params_test.go`'s `TestParamsToArgsNeverInterpolatesIntoSQLText` proves
this directly: a value shaped like `alice'; DROP TABLE accounts; --`, run
through `sql.query` as an ordinary bound parameter against a real database,
matches nothing and leaves the table untouched - not a mock standing in for
what a naive implementation might do, the actual driver.

Dynamic identifiers - a table or column name chosen at runtime - are
refused for the same reason, and there is no input in this version's schema
that accepts one. See `doc.go`'s own section on this for the reasoning and
the future path (a dedicated, quoted-identifier input, never text
splicing).

## Secrets

Both tasks declare `dsn` in `secret_inputs` and `required_secret_inputs`: a Flowfile writes
`dsn: ${secret('provider:name')}`, and the host resolves that reference
under the caller's identity before this task's `Fn` ever runs. This plugin
process never holds a provider credential, a vault token, or a reference of
its own - only the one resolved value, for the duration of one call.

The connection string itself is resolved whole. The host refuses a literal
`dsn:` before invoking either task, so it cannot put a credential into a
Flowfile or workflow history. A secret reference is not destination authority:
the operator must separately provide an egress policy that permits every
resolved PostgreSQL address and port.

Every DSN this plugin resolves is registered with a
[`secrets.Scrubber`](../../pkg/flowstate/v1/secrets/scrub.go) before a
connection is ever opened, and every error and output this plugin returns
passes through it - database drivers echo connection strings in their own
error messages more often than you'd like. `scrub_test.go` proves this
holds under `%v`, `%+v`, `%#v`, and `%s`, on the error value itself, on a
struct wrapping it, and on a slice of those.

## Bounded results

`max_rows` is required on every `sql.query` call, with a hard ceiling - no
default, because an unbounded query is a request this plugin refuses to
have an opinion about on a workflow author's behalf. When a result would
exceed `max_rows`, the call is refused outright, naming the bound, **never**
returned as the first `max_rows` rows with the rest silently dropped - a
truncated result that looks complete is the worst possible shape of wrong
answer for a query a workflow is about to make a decision from.

What this plugin could bound on the wire, and what it honestly could not,
differs by engine - see `doc.go`, "Byte bounds," for the full argument.
Short version: postgres gets a real bound below the driver (the connection's
own socket is wrapped and refuses to read past a fixed budget, the same
RoundTripper-shaped lesson CLAUDE.md draws for HTTP); sqlite, an embedded
engine with no network wire to bound, gets the honest analogue instead - a
cap on decoded result size, checked as each row is scanned, before it is
ever assembled into an output.

## Typed rows

A result set comes back as `rows`: a list of maps, one per row, column name
to value, typed from the driver (an integer column stays an integer, never
a stringified one) - so `${steps.lookup.rows.filter(r, r.balance_cents >
0)}` type-checks and runs the way any other CEL expression over a step
output does. `columns` names each column independently, useful when a
result can legitimately be empty. See `rows.go`'s own doc comment on
`convertColumnValue` for exactly which Go types convert, and the one
documented rough edge: a `BLOB`/`BYTEA` column and a `TEXT` column both
arrive as bytes from these drivers, and both become a Go string - binary
data that is not valid UTF-8 still round-trips (Go strings are byte
sequences, not validated text), but a CEL expression treating it as text may
see something that does not look readable.

## Transactions end where the activity ends

One `sql.exec` call is at most one transaction: every statement in
`statements:` runs in order, and the whole set commits together or none of
it does, inside this single activity invocation. There is no `BEGIN` task
and no `COMMIT` task in this schema - a step is an activity, retried on
failure and resumable on a different worker after a crash, and a
transaction held open across that boundary is worktree-state in database
costume (CLAUDE.md's own values-not-worktrees rule). See `doc.go` for the
full argument, including the idiomatic alternatives this plugin leans on
instead of inventing its own: sagas (`undo:`) for a write that spans several
steps, an idempotency key as an ordinary bound parameter for a retried write
that should converge rather than duplicate, and
[`sdk.OutcomeUnknown`](../../pkg/flowstate/v1/plugin/sdk/errors.go) for the
commit-acknowledgement-lost case - the INSERT that may have committed.

<!-- example: examples/plugins/sql/transfer.yaml -->
```yaml
edition: v2026.3
name: sql-transfer
description: Moves money between two accounts using the "sql" plugin's sql.exec task - four statements, one transaction, committed or rolled back together inside this single step, provably idempotent on retry.

# sql.exec is the write half of the "sql" plugin: every statement in
# `statements:` runs inside one transaction that begins and ends inside this
# one activity invocation - see plugins/sql/doc.go, "Transactions end where
# the activity ends." There is no BEGIN task and no COMMIT task in this
# plugin's schema, on purpose: the debit and the credit below either both
# take effect or neither does, and there is no spelling in this file that
# could hold the transaction open across a second step.
#
# idempotency_key is bound as an ordinary parameter, not a special field -
# see plugins/sql/doc.go, "Idempotency keys as ordinary params." A retried
# call (the engine retries a step whose attempt failed for a reason that
# looks transient, or after a commit acknowledgement was lost - see
# sdk.OutcomeUnknown) sends the same key, and this file has to make that
# retry a genuine no-op, not merely insert a ledger row nobody reads.
#
# # Why an ON CONFLICT DO NOTHING insert is not enough on its own
#
# An earlier version of this file inserted a ledger row keyed on
# idempotency_key with ON CONFLICT (idempotency_key) DO NOTHING and left
# the two balance UPDATEs unconditional, reasoning that a duplicate insert
# would be silently absorbed. That reasoning was wrong, and worth stating
# plainly rather than quietly fixing: ON CONFLICT DO NOTHING suppresses
# only the INSERT itself. Both UPDATE statements below it still ran
# unconditionally on a retry, moving the money a second time - the exact
# bug idempotency keys exist to prevent, taught by the example meant to
# demonstrate preventing it.
#
# The fix every statement below implements: accounts_ledger carries an
# `applied` flag (0 by default), and both balance UPDATEs are guarded by
# `WHERE ... AND EXISTS (SELECT 1 FROM accounts_ledger WHERE
# idempotency_key = ? AND applied = 0)`, with the ledger flipped to
# `applied = 1` as this transaction's last statement. A fresh key: the
# insert claims the row (applied stays 0), both guards pass, both balances
# move, and the flag flips true before commit. A replayed key after a
# lost acknowledgement: the insert hits its conflict and changes nothing,
# the ledger row already has applied = 1, so both guards evaluate false and
# neither UPDATE's WHERE clause matches a row - the balances are untouched,
# and only the harmless "set applied = 1" statement (already true) runs
# again. See plugins/sql/exec_test.go's
# TestSQLTransferPatternMovesMoneyExactlyOnceAcrossARetry, which runs this
# exact four-statement pattern against a hermetic test database twice with the same key and
# asserts both balances moved on the first call and stayed put on the
# second - the point of an idempotency claim is a test proving it, not a
# comment asserting it.
#
# Requires configuration - a real database with accounts and
# accounts_ledger(idempotency_key TEXT PRIMARY KEY, applied INTEGER NOT
# NULL DEFAULT 0) tables, and a resolvable dsn secret - so it never runs by
# accident. See plugins/sql/README.md, "Trying this example."
# The operator must also permit the database destination with --egress-policy
# and permit the qualified sql.exec capability in task policy. sql.query and
# sql.exec are separate policy names so read access does not imply write access.
#
# Written for ENGINE_POSTGRES, with $1, $2, ... placeholders - pgx does not
# rewrite `?`; PostgreSQL statements use numbered placeholders.
inputs:
  from_account_id:
    type: int
    required: true
    description: the account debited
  to_account_id:
    type: int
    required: true
    description: the account credited
  amount_cents:
    type: int
    required: true
    description: how much moves, in cents (must be positive)
  idempotency_key:
    type: string
    required: true
    description: a value unique to this transfer request, so a retried call converges rather than moving the money twice
steps:
  - id: transfer
    sql.exec:
      dsn: ${secret('env:SQL_DSN')}
      engine: ENGINE_POSTGRES
      statements:
        - sql: INSERT INTO accounts_ledger (idempotency_key) VALUES ($1) ON CONFLICT (idempotency_key) DO NOTHING
          params:
            - ${inputs.idempotency_key}
        - sql: UPDATE accounts SET balance_cents = balance_cents - $1 WHERE id = $2 AND EXISTS (SELECT 1 FROM accounts_ledger WHERE idempotency_key = $3 AND applied = 0)
          params:
            - ${inputs.amount_cents}
            - ${inputs.from_account_id}
            - ${inputs.idempotency_key}
        - sql: UPDATE accounts SET balance_cents = balance_cents + $1 WHERE id = $2 AND EXISTS (SELECT 1 FROM accounts_ledger WHERE idempotency_key = $3 AND applied = 0)
          params:
            - ${inputs.amount_cents}
            - ${inputs.to_account_id}
            - ${inputs.idempotency_key}
        - sql: UPDATE accounts_ledger SET applied = 1 WHERE idempotency_key = $1
          params:
            - ${inputs.idempotency_key}
  - id: announce
    log:
      message: ${"moved %d cents from account %d to account %d across %d statement(s), one transaction".format([inputs.amount_cents, inputs.from_account_id, inputs.to_account_id, steps.transfer.statement_count])}
outputs:
  rows_affected:
    value: ${steps.transfer.total_rows_affected}
    description: 4 on a fresh transfer (insert, debit, credit, flag), 1 on a converged retry (only the harmless flag-set touches a row)
```

PostgreSQL statements use `$1`, `$2`, ... placeholders. Values remain separate
bound parameters; the plugin does not translate placeholder dialects.

## Drivers

PostgreSQL ([`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx)) is the
distributed plugin's supported runtime engine. SQLite
([`modernc.org/sqlite`](https://modernc.org/sqlite)) is compiled only for
hermetic package tests - no cgo anywhere in this module.
`Engine` is a closed proto enum naming exactly the drivers this build ships;
naming one this build lacks (`engine: ENGINE_ORACLE`) is refused by `flow
validate` with a positioned diagnostic listing the choices, the same way any
other closed enum field is. Per issue #181's driver-vs-plugin rule, a driver
is a Go dependency compiled into this plugin, never a runtime-loadable
backend - adding one is a PR here, reviewed and released, not a runtime
event.

File-backed and in-memory SQLite DSNs are refused in released binaries. This
avoids granting arbitrary file open, URI, symlink, `ATTACH`, `VACUUM INTO`, or
extension-loading authority without pretending plugin process separation is a
sandbox. Package tests enable SQLite only through test-compiled code.

## Trying this example

Neither example runs with no arguments. Put the complete PostgreSQL DSN in the
configured secret backend, then pass a deployment-owned policy permitting only
the database host, resolved network, and port. This deliberately local example
is for a loopback development database; production deployments must replace
the target exactly:

```console
# Provision FLOWSTATE_SECRET_SQL_DSN out of band; do not paste its value into
# shell history, this README, or the Flowfile.
flow worker --plugin-dir /path/to/plugins \
  --secret-env SQL_DSN --auth-policy /path/to/auth-policy.yaml \
  --egress-policy examples/plugins/sql/egress-policy.yaml
```

`SQL_DSN` above is illustrative - which environment variable a `${secret('env:SQL_DSN')}`
reference actually resolves to depends on which secret provider this
deployment configured (see `pkg/flowstate/v1/secrets`); check that
package's own docs for the exact variable name your worker expects. Do not put
the DSN on the command line or in the Flowfile. `sql.query` and `sql.exec` are
separate qualified task-policy capabilities; granting the former does not grant
the latter's write authority.

<!-- example: examples/plugins/sql/egress-policy.yaml -->
```yaml
# Deliberately local-only policy for the SQL example. Production deployments
# should replace both the host and network with their database's exact values.
egress:
  schemes: [postgres]
  allow:
    - host == "localhost" && port == 5432
  allow_networks:
    - 127.0.0.0/8
    - ::1/128
  allow_ports: [5432]
  min_tls_version: "1.2"
```

Missing or malformed policy fails closed. PostgreSQL requires verified TLS,
rejects Unix sockets and filesystem-reading connection options, checks every
address of every host before dialing, pins those resolutions against rebinding,
and rechecks the actual socket target immediately before each dial. Because
DSN-selected `sslrootcert` files would restore arbitrary worker-file reads,
private database CAs must be installed in the worker's system trust store; this
release has no separate operator-owned SQL CA-bundle setting.

## Security properties, and what holds by construction

**No shell-out, ever.** Nothing in this plugin calls `exec.Command`,
`os/exec`, or anything that spawns a process - both drivers are pure Go.
There is no `psql` or `sqlite3` binary anywhere in this plugin's reach, and
no argument-injection-shaped path through one.

**Parameters cannot become SQL text.** See "Parameterized only,
structurally," above - this is a structural property of this plugin's code,
proven by `params_test.go`'s injection-shaped test, not a validation rule
layered on top of a more general (and reopenable) mechanism.

**Every connection is opened and closed within one call.** No connection
pool, no cached connection, nothing held between calls - see "Transactions
end where the activity ends," above.

**Column values fail closed on an unrecognized shape.** `convertColumnValue`
(`rows.go`) converts exactly the six Go types `database/sql` documents for a
generic scan; anything else is refused with a diagnosable error rather than
stringified with `%v`, which would silently misrepresent a value CEL has no
way to tell apart from a real string column.

## What was proven to bite

Every containment and injection claim above was checked by breaking the
code on purpose, confirming the test goes red, then restoring the fix:
scrubbing removed from the query and exec error paths (`TestClassifyFunctionsScrubBeforeClassifying`,
`TestClassifyExecErrorScrubsBeforeClassifying` failed, naming the leaked
DSN, exactly as intended), and query text built by naive string splicing in
place of parameter binding (`TestParamsToArgsNeverInterpolatesIntoSQLText`
failed with a SQL-level error from the corrupted query, rather than passing
silently).

## Bounds and remaining limits

PostgreSQL retains the real wire-level byte bound below pgx, plus row and
decoded-result bounds above it. There is no live PostgreSQL integration server
in this module; the actual dial path is tested with bounded local listeners,
while protocol semantics are covered without external infrastructure.
