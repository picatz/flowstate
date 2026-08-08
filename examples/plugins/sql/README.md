# A task a plugin provides: sql.query and sql.exec

[`workflow.yaml`](workflow.yaml) reads bounded, typed rows from a sqlite
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

`workflow.yaml` only reads, so the worst a misconfigured run does is fail.
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

Both files need configuration that is not expressible in the file - a built
plugin, a worker told where to find it, a real database, and a resolvable
`dsn` secret - so neither runs by accident and neither is executed by CI.
[`plugins/sql/README.md`](../../../plugins/sql/README.md), "Trying this
example," is the procedure: it names the schema the `accounts` table needs and
the environment variable the `${secret('env:SQL_DSN')}` reference resolves
through.

The connection string is a secret reference resolved inside the task, never a
literal in the file — the same rule `examples/http-secret` follows.

The two files deliberately target different engines: `workflow.yaml` is
sqlite-flavored and `transfer.yaml` is written for `ENGINE_POSTGRES`. Between
them they make a point the plugin's README states outright under "Drivers" and
worth knowing before assuming otherwise: **a query is not portable between
engines just because the task is.** `engine:` swaps in one line, but pgx does
not rewrite `?` into `$1`, so the statement text has to change with it. Two
engines, one task, two dialects.
