// Command flowstate-plugin-sql provides two tasks over ordinary SQL
// databases: sql.query, a bounded read, and sql.exec, one or more
// statements run as a single transaction. This is the connectivity
// family's first member (issue #181) - the proof that flowstate runs
// ordinary business workloads ("read something, decide, write something,
// tell someone") and not only developer tooling.
//
// # Parameterized only, structurally
//
// query: and every statement's sql: field take SQL text, and params:
// (QueryInputs.params, Statement.params) is a separate list of values bound
// to that text's own placeholders (`?` for sqlite, `$1`, `$2`, ... for
// postgres) by the driver itself. There is no function anywhere in this
// plugin that appends a value to a query string, formats one into it, or
// otherwise turns a params entry into SQL text - see params.go's own doc
// comment on paramsToArgs. That is not a validation rule layered on top of
// a more general mechanism; it is the only mechanism this schema has. A CEL
// expression written into params: therefore cannot produce a SQL fragment,
// only a parameter value, regardless of what that value contains -
// params_test.go's TestParamsToArgsNeverInterpolatesIntoSQLText proves this
// by running `'; DROP TABLE users; --` through as a bound string parameter
// against a real database and asserting the table is still there.
//
// Dynamic identifiers - a table or column name chosen at runtime - are
// refused for the same reason: there is no quoted-identifier input in this
// version's schema, and gluing an identifier into query text the way a
// naive implementation gluess a value in would reopen exactly the
// injection class this design refuses. Issue #181 records this as a real,
// deliberate gap: "an advanced identifier-substitution need is refused
// until a real workload argues it, and then designed as its own quoted-
// identifier input, never text splicing." Nothing here works around that by
// building a query string from a workflow-controlled table name; a workflow
// that needs to touch several tables writes several calls, one per table,
// each with its own literal query text.
//
// # Secrets: dsn is never a literal a plugin process holds long
//
// Both tasks declare dsn in their own SecretInputs (main.go), the #160
// mechanism this plugin is the third consumer of, after plugins/codex's
// api_key and vault-backed secrets generally: a Flowfile writes
// `dsn: ${secret('vault:prod/db#dsn')}`, and the host resolves that
// reference under the caller's identity before this task's Fn ever runs -
// this plugin process never holds a provider credential, a vault token, or
// a reference of its own, only the one resolved value for the duration of
// one call (dsnFromValue in secrets.go).
//
// A literal dsn: value is accepted rather than refused, for the same
// reason plugins/codex's api_key is: by the time Fn runs, a resolved secret
// and an author's own literal arrive as the identical
// [flowstatev1.Value_Literal] shape, and this task has no way to tell them
// apart. Writing one directly is discouraged (it puts a credential in the
// Flowfile and in workflow history) but not mechanically prevented at this
// layer - the mechanical prevention is `flow validate` refusing anything
// but a secret reference for an input declared in secret_inputs, which is
// the host's enforcement, not this plugin's.
//
// Every DSN this plugin resolves is registered with a
// [secrets.Scrubber] before the connection is ever opened (query.go,
// exec.go), and every error and output this plugin returns passes through
// it. This matters more than usual for a SQL plugin specifically: database
// drivers echo connection strings in their own error messages routinely -
// "dial tcp: lookup db.internal: no such host" carries no secret, but
// "pq: SSL error: password authentication failed for user \"app\" (dsn:
// postgres://app:hunter2@db.internal/prod)" does, and this plugin has no
// way to know in advance which of a driver's error paths does which. See
// scrub_test.go for the containment-shape tests CLAUDE.md requires: %v,
// %+v, %#v, and %s, on the value, on a struct holding it, and on a slice of
// those, proven to bite by deliberately leaking the DSN and watching the
// test go red before the fix, then green after.
//
// # Bounded results, and what a truncated one means here
//
// max_rows is required on every sql.query call, with a hard ceiling
// (maxMaxRows, bounds.go) - there is no default, because an unbounded
// query is a request this plugin refuses to have an opinion about on a
// workflow author's behalf. When a result would exceed max_rows, this task
// refuses the call outright, naming the bound, rather than returning the
// first max_rows rows as though that were the whole answer - see
// scanBoundedRows in rows.go. This is CLAUDE.md's no-silent-caps rule
// applied to its sharpest instance yet: a query result silently cut to a
// plausible-looking prefix is the worst possible shape of wrong answer for
// a query a workflow is about to make a decision from, because it looks
// exactly like a complete one.
//
// # Byte bounds: what this plugin could bound, and what it honestly could not
//
// The RoundTripper lesson from CLAUDE.md - bound the resource on the path
// an attacker would actually take, below the library that might not cover
// its own error paths - applies differently to each engine, and this
// plugin does not pretend otherwise:
//
//   - Postgres speaks its wire protocol over an ordinary net.Conn, so
//     driver.go wraps the connection's own socket (via pgx's AfterNetConnect,
//     which runs after TLS is established, so it sees the same bytes the
//     protocol layer does regardless of whether the DSN asked for TLS) in a
//     wireBoundConn that refuses to read past maxWireBytes. This is a real
//     bound below the client library, the same shape plugin/transport.go's
//     comment in CLAUDE.md describes for HTTP: a hostile server cannot make
//     this plugin buffer an unbounded response by returning one, because the
//     refusal happens at the socket, before pgx's own parsing ever sees the
//     excess bytes.
//   - Sqlite is an embedded engine with no network wire to bound at all -
//     the driver reads from a file or from memory in-process, so there is no
//     equivalent connection to wrap. What this plugin bounds instead is the
//     honest analogue: decoded result size, checked in scanBoundedRows as
//     each row is scanned, before it is ever assembled into this call's
//     output (maxRowBytes per row, maxResultBytes across the whole result).
//     That is real prevention (a row over budget stops the scan and refuses
//     before more work happens), but it is a different bound catching a
//     different thing than the postgres wire cap: a hostile *query* that
//     computes a huge value from tiny stored data (a recursive CTE, a
//     pathological string aggregate) is bounded either way, but nothing
//     here bounds how much work sqlite itself does producing bytes this
//     plugin has not read yet - the same class of gap plugins/git's own
//     doc.go names for pack decompression ratio, reported here rather than
//     solved.
//   - Both engines apply the row-count refusal (max_rows) and the
//     decoded-byte bounds (maxRowBytes, maxResultBytes) uniformly, as a
//     backstop under the postgres wire bound and as the primary defense for
//     sqlite.
//
// # Transactions end where the activity ends
//
// One sql.exec call is at most one transaction, and that transaction
// begins (BeginTx) and either commits or rolls back (runTransaction,
// exec.go) before Fn returns - never held open across the RPC boundary,
// never carried in an output for a later step to resume. This is not a
// convenience default with an escape hatch; there is no field anywhere in
// ExecInputs or ExecOutputs that could name an open transaction, because a
// step is an activity - retried on failure, and resumable on an entirely
// different worker process after a crash - and a transaction held open
// across that boundary is worktree-state in database costume (the #149
// values-not-worktrees rule this repository applies everywhere else).
//
// The idiomatic alternative for work that spans steps is what the engine
// already has, not something this plugin invents:
//
//   - Sagas (`undo:`) for a multi-step write that needs to be undone if a
//     later step fails - each step's own transaction commits or rolls back
//     independently, and a later failure triggers the earlier step's `undo:`
//     rather than reaching back into a transaction that no longer exists.
//   - Idempotency keys as ordinary params: a workflow that might retry a
//     write includes a key value (a UUID it generated, an order ID) as a
//     bound parameter and gives the target table a unique constraint on it -
//     `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` for postgres, its
//     sqlite equivalent for the other engine - so a retried sql.exec call
//     converges to a no-op rather than a duplicate row. This plugin does not
//     do this automatically, because whether a caller wants convergence-by-
//     constraint or a hard failure on retry is the workflow's decision, not
//     this task's.
//   - [sdk.OutcomeUnknown] for the case a retry cannot safely resolve on its
//     own at all: a commit acknowledgement lost to a network failure right
//     as this call asked the backend to make a write durable. See errors.go's
//     classifyExecError, "The commit-ack-lost case" - the INSERT that may
//     have committed is this classification's sharpest real instance, named
//     directly in issue #181's own design comment, and this plugin's commit
//     phase is where it is actually implemented rather than merely
//     described.
//
// The wrong instinct this plugin refuses to make possible: BEGIN in one
// step, work in a second, COMMIT in a third. There is no BEGIN task and no
// COMMIT task in this schema for a workflow to spell that with - the only
// unit of transactional work this plugin can express is "every statement
// in one sql.exec call, together."
//
// # Drivers
//
// sqlite (modernc.org/sqlite) and postgres (github.com/jackc/pgx/v5) are
// both pure Go, compiled directly into this plugin - no cgo anywhere in
// this module, and no third layer of runtime-loadable database backends:
// issue #181's own driver-vs-plugin rule settles this explicitly ("a driver
// is a Go dependency compiled into its plugin... adding a driver is a PR to
// the plugin - reviewed, vetted, released - not a runtime event"). Engine
// is a closed proto enum (sql.proto) naming exactly the drivers this build
// ships; naming one this build lacks is refused by `flow validate` with a
// positioned diagnostic listing the choices, the same way any other closed
// enum field is (flowfile/schema.go's literalMismatch, which this plugin
// gets for free from declaring Engine as an enum rather than a string).
//
// # Why sqlite is enumerated first
//
// Issue #181's own design comment orders the drivers "postgres first" for
// production weight - postgres is the enterprise target this family exists
// to reach. This plugin's own test suite runs against sqlite first anyway,
// deliberately, for the reason CLAUDE.md's "run what CI runs" guidance
// gives the same weight to: a plugin whose tests need a running server is a
// plugin CI cannot honestly verify. Every hermetic test in this module -
// query.go, exec.go, params.go, rows.go, scrub_test.go, errors_test.go -
// runs against an in-process sqlite database, in-memory, with nothing to
// stand up, nothing to tear down, and no flake from a container that was
// slow to become ready. The postgres driver is real, compiled in, and
// exercised by unit tests that do not require a live server (DSN parsing,
// wire-bound conn wrapping, PgError classification against constructed
// values) - but this module has no live-postgres integration test, and
// that gap is named here rather than left for someone to discover: nothing
// in CI today proves sql.query and sql.exec work end to end against a real
// postgres server, only that they are wired to try. Both engines are
// enumerated in the manifest and both are real, shipped drivers; only one
// of them has this plugin's own tests standing behind its wire format.
package main
