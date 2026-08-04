package main

import (
	"context"
	"database/sql"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// readOnlyRows runs query under an engine-enforced read-only mode, so a
// write submitted through sql.query is refused by the database itself
// rather than merely relying on this task's own method name and
// classification (documented and retried as read-only/idempotent - see
// doc.go). database/sql's QueryContext alone never refuses DML: an
// `UPDATE ... RETURNING` (or, on sqlite, a second statement smuggled in
// after a `;`) executes and autocommits exactly as a SELECT would, so a
// retried "read" after a lost response could apply a write a second time.
//
// This is deliberately not solved by inspecting query for a write keyword.
// Detecting DML by parsing or pattern-matching SQL text is the same class
// of trap doc.go's whole design refuses for params: a detector is a
// second, incomplete parser standing in for the one authority that
// actually knows the grammar - the database itself - and a construct the
// detector does not recognize (a CTE that writes, a stored procedure call,
// whatever the next dialect adds) walks straight through. So this asks the
// engine to refuse instead, the same direction CLAUDE.md's fail-closed
// guidance points every other policy surface in this repository: deny by
// default, on infrastructure that cannot be argued past by a query shape
// nobody anticipated.
//
// Every engine gets its own enforcement, applied to the *same* physical
// connection the query itself runs on - db.Conn(ctx) for sqlite, a single
// transaction for postgres - because a read-only bound set on one
// connection and a query run on another enforces nothing at all. The
// returned cleanup function must be deferred *after* rows.Close() (i.e.
// called second), so query.go's own defer order is significant.
func readOnlyRows(ctx context.Context, db *sql.DB, engine sqlv1.Engine, query string, args []any) (*sql.Rows, func() error, error) {
	switch engine {
	case sqlv1.Engine_ENGINE_SQLITE:
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, nil, err
		}
		// query_only refuses every write statement sqlite recognizes -
		// INSERT, UPDATE, DELETE, DDL, ATTACH's own writes - with "attempt
		// to write a readonly database," scoped to this one connection
		// rather than the database file itself, which is what lets a
		// concurrent sql.exec call still write through a different
		// connection while this read runs.
		if _, err := conn.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
			conn.Close()
			return nil, nil, err
		}
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
		return rows, conn.Close, nil

	case sqlv1.Engine_ENGINE_POSTGRES:
		// sql.TxOptions.ReadOnly maps onto pgx's own pgx.ReadOnly access
		// mode (stdlib/sql.go), which pgx sends as the wire-level
		// equivalent of BEGIN ... READ ONLY - postgres itself then refuses
		// any data-modifying statement for the lifetime of this
		// transaction, not this plugin's own guess at which statements
		// modify data.
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return nil, nil, err
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			_ = tx.Rollback()
			return nil, nil, err
		}
		// Rollback rather than Commit: this transaction never wrote
		// anything (postgres would refuse it if it tried), so there is
		// nothing to commit, and Rollback is the unconditionally safe
		// choice regardless of how the read itself concluded.
		return rows, tx.Rollback, nil

	default:
		return nil, nil, sdk.InvalidInput(
			"engine %q is not one this build supports; this build was compiled with: sqlite, postgres",
			engine.String())
	}
}
