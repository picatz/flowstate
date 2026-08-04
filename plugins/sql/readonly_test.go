package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// TestSQLQueryRefusesAWriteOnSQLite is the P1-2 regression test: sql.query
// is documented and classified as read-only and therefore retried on
// failure (doc.go), so a write statement reaching the database through it
// would let a retry after a lost response apply that write again. This
// proves the database itself refuses it - not a check on the query string,
// which this plugin's whole design (see readonly.go's own doc comment)
// refuses to rely on.
func TestSQLQueryRefusesAWriteOnSQLite(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance_cents INTEGER)`,
		`INSERT INTO accounts (id, balance_cents) VALUES (1, 500)`)

	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "UPDATE accounts SET balance_cents = 0 WHERE id = 1",
		"max_rows": int32(10),
	}), nil)
	if err == nil {
		t.Fatal("sql.query running an UPDATE: got no error, want the database to refuse it")
	}

	// Confirm the balance is untouched - the write was refused, not merely
	// reported as an error after actually taking effect.
	db, dbErr := sql.Open("sqlite", dsn)
	if dbErr != nil {
		t.Fatalf("opening db to verify: %v", dbErr)
	}
	defer db.Close()
	var balance int
	if err := db.QueryRowContext(context.Background(), "SELECT balance_cents FROM accounts WHERE id = 1").Scan(&balance); err != nil {
		t.Fatalf("verifying balance: %v", err)
	}
	if balance != 500 {
		t.Errorf("balance_cents = %d, want 500 (the UPDATE submitted through sql.query must not have taken effect)", balance)
	}
}

// TestSQLQueryRefusesADeleteOnSQLite covers a second write statement kind,
// so the refusal is proven to be about writes in general (enforced by the
// engine) rather than a coincidence of how UPDATE happens to fail.
func TestSQLQueryRefusesADeleteOnSQLite(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY)`, `INSERT INTO accounts (id) VALUES (1)`)

	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "DELETE FROM accounts",
		"max_rows": int32(10),
	}), nil)
	if err == nil {
		t.Fatal("sql.query running a DELETE: got no error, want the database to refuse it")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "readonly") && !strings.Contains(strings.ToLower(err.Error()), "read-only") &&
		!strings.Contains(strings.ToLower(err.Error()), "read only") {
		t.Errorf("error does not indicate a read-only refusal: %v", err)
	}
}

// TestSQLQueryStillWorksForAnOrdinarySelectOnSQLite is the other half of
// the boundary: enforcing read-only must not break the ordinary case.
func TestSQLQueryStillWorksForAnOrdinarySelectOnSQLite(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO accounts (id, name) VALUES (1, 'alice')`)

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT name FROM accounts WHERE id = 1",
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("an ordinary SELECT under read-only enforcement: unexpected error: %v", err)
	}
	if n := outputs.GetNamedValues()["row_count"].GetLiteral().GetInt64Value(); n != 1 {
		t.Fatalf("row_count = %d, want 1", n)
	}
}

// TestSQLQueryConcurrentWriteStillWorksOnSQLite proves query_only's own
// scoping claim in readonly.go's doc comment: enforcing read-only on the
// connection sql.query uses must not block a concurrent sql.exec write
// through a *different* connection to the same database.
func TestSQLQueryConcurrentWriteStillWorksOnSQLite(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance_cents INTEGER)`,
		`INSERT INTO accounts (id, balance_cents) VALUES (1, 500)`)

	if _, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT balance_cents FROM accounts WHERE id = 1",
		"max_rows": int32(10),
	}), nil); err != nil {
		t.Fatalf("sql.query: unexpected error: %v", err)
	}

	// A later, independent sql.exec call (a fresh connection, per doc.go's
	// "Transactions end where the activity ends") must still be able to
	// write - the read-only PRAGMA was never global.
	if _, err := sqlExec(context.Background(), inputsFor(map[string]any{
		"engine": "ENGINE_SQLITE",
		"dsn":    dsn,
		"statements": []any{
			map[string]any{"sql": "UPDATE accounts SET balance_cents = 600 WHERE id = 1"},
		},
	}), nil); err != nil {
		t.Fatalf("sql.exec after a prior sql.query: unexpected error: %v", err)
	}
}

// fakePGConn and fakePGDriver implement just enough of database/sql/driver
// to prove readOnlyRows actually requests postgres's own read-only access
// mode (sql.TxOptions.ReadOnly) - pgx/v5/stdlib's own translation of that
// into "BEGIN ... READ ONLY" is verified by reading stdlib/sql.go directly
// (see readonly.go's doc comment); what this test proves is that this
// plugin's own code asks for it, on a real database/sql code path rather
// than a live postgres server, which this module's test suite does not
// stand up (see doc.go, "Why sqlite is enumerated first").
type fakePGConn struct {
	mu           sync.Mutex
	sawReadOnly  bool
	sawBeginTxAt bool
}

func (c *fakePGConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (c *fakePGConn) Close() error              { return nil }
func (c *fakePGConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

func (c *fakePGConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	c.sawBeginTxAt = true
	c.sawReadOnly = opts.ReadOnly
	c.mu.Unlock()
	return fakePGTx{}, nil
}

func (c *fakePGConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return &fakePGRows{}, nil
}

type fakePGTx struct{}

func (fakePGTx) Commit() error   { return nil }
func (fakePGTx) Rollback() error { return nil }

type fakePGRows struct{ done bool }

func (r *fakePGRows) Columns() []string { return []string{"n"} }
func (r *fakePGRows) Close() error      { return nil }
func (r *fakePGRows) Next(dest []driver.Value) error {
	if r.done {
		return sql.ErrNoRows
	}
	r.done = true
	dest[0] = int64(1)
	return nil
}

type fakePGDriver struct{ conn *fakePGConn }

func (d fakePGDriver) Open(name string) (driver.Conn, error) { return d.conn, nil }

// TestReadOnlyRowsRequestsPostgresReadOnlyAccessMode is the unit-level
// proof (no live server) that the postgres branch of readOnlyRows asks the
// driver for a read-only transaction rather than an ordinary one.
func TestReadOnlyRowsRequestsPostgresReadOnlyAccessMode(t *testing.T) {
	conn := &fakePGConn{}
	sql.Register("fakepg_readonly_test", fakePGDriver{conn: conn})
	db, err := sql.Open("fakepg_readonly_test", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	rows, cleanup, err := readOnlyRows(context.Background(), db, sqlv1.Engine_ENGINE_POSTGRES, "SELECT 1", nil)
	if err != nil {
		t.Fatalf("readOnlyRows: unexpected error: %v", err)
	}
	rows.Close()
	_ = cleanup()

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.sawBeginTxAt {
		t.Fatal("readOnlyRows(ENGINE_POSTGRES) never began a transaction at all")
	}
	if !conn.sawReadOnly {
		t.Error("readOnlyRows(ENGINE_POSTGRES) began a transaction without requesting ReadOnly - " +
			"postgres would allow a write through it")
	}
}
