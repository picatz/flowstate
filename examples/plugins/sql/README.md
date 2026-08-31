# A task a plugin provides: sql.query and sql.exec

[`workflow.yaml`](workflow.yaml) reads bounded, typed rows from PostgreSQL
database with `sql.query:`, and [`transfer.yaml`](transfer.yaml) moves money
between two accounts with `sql.exec:` - the `sql` plugin's two tasks, one for
reading and one for writing.

Nothing about either step is special: each takes inputs, produces outputs a
later step reads, and its schema is checked before it runs. What is special is
that the schema belongs to the plugin - the engine has never compiled
`sql.v1.QueryInputs`, and learns the shape of `engine`, `query`, `params`,
`max_rows`, `rows` and the rest from descriptors the plugin ships in its
manifest and hands over at launch. See
[`plugins/sql`](../../../plugins/sql) for this one's source, and
[`plugins/sql/README.md`](../../../plugins/sql/README.md) for what it needs
configured on a worker.

## Two files, because one of them writes

`workflow.yaml` only reads. `sql.query` and `sql.exec` are distinct qualified
task names so an operator can allow reads without granting writes.
`transfer.yaml` moves money, and it is a separate parameterized file for the
same reason `plugins/git` and `plugins/github` split theirs: a mutation should
not be something a reader runs by accident while trying the read.

## What the read file is actually demonstrating

The interesting property is not that a workflow can reach a database. It is
that the rows come back as a value the rest of the language already knows how
to handle - `steps.accounts.rows` is a list of maps CEL filters and indexes by
column name, exactly like any other step output, so the `announce` step needs
no adapter between "the database answered" and "an expression reads it."

Two bounds are worth reading the file for, because both are refusals rather
than conveniences:

- **`params:` is the only way a value reaches the database.** A parameter is
  bound, never spliced into the query text - the plugin has no spelling that
  would let `vars.min_balance_cents` become part of the statement. See
  `plugins/sql/doc.go`, "Parameterized only, structurally."
- **`max_rows:` is required and has no default.** A result larger than it is
  refused outright, naming the bound, rather than returned as a silently
  truncated prefix - a short answer that looks complete is the failure this
  avoids.

## What the write file is actually demonstrating

Every statement in `statements:` runs inside one transaction that begins and
ends inside one activity invocation. There is no BEGIN task and no COMMIT task
in this plugin's schema, deliberately: the debit and the credit either both
take effect or neither does, and no spelling in a Flowfile could hold a
transaction open across two steps for a worker to be restarted in the middle
of.

`idempotency_key` is an ordinary bound parameter rather than a special field,
which is what makes a retried attempt a genuine no-op instead of a second
transfer. Read the file's own comments for why an `ON CONFLICT DO NOTHING`
insert is not sufficient on its own.

## Running it

From the repository root, build the separate plugin executable and inspect the
catalog the same configured path will provide to a worker:

```console
$ mkdir -p ./plugins
$ go -C plugins/sql build -o ../../plugins/flowstate-plugin-sql .
$ go run ./cmd/flow plugins --plugin-dir ./plugins
```

Both files need configuration that is not expressible in the file - a built
plugin, a worker told where to find it, a real PostgreSQL database, a
resolvable `dsn` secret, an egress policy allowing the database destination,
and task policy allowing the qualified task - so neither runs by accident and
neither is executed by CI.
[`plugins/sql/README.md`](../../../plugins/sql/README.md), "Trying this
example," is the procedure: it names the schema the `accounts` table needs and
the environment variable the `${secret('env:SQL_DSN')}` reference resolves
through.

The connection string must be a secret reference resolved by the host, never a
literal in the file. Credential source and destination authorization are
separate: the reference keeps the DSN out of the Flowfile and history, while
the operator-owned egress policy authorizes every resolved socket address.
See [`egress-policy.yaml`](egress-policy.yaml) for a deliberately local-only
example policy and the plugin README for the secret setup.

For installation integrity and SDK compatibility limits, use the canonical
[plugin contract guide](../../../docs/PLUGINS.md#five-places-the-contract-is-implicit).
Task schemas and capabilities come from discovered descriptors; this README
does not maintain a second inventory.

Both files use `ENGINE_POSTGRES`. The distributed plugin refuses SQLite because
an embedded driver grants worker-filesystem authority that process separation
does not confine. Hermetic package tests retain SQLite only as a test fixture.
