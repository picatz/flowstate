package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"

	_ "modernc.org/sqlite"
)

// testDSN is a fresh, private sqlite database per test, backed by a file in
// t.TempDir() rather than sqlite's own :memory: - the hermetic substrate
// every test in this package runs against, per doc.go's "Why sqlite is
// enumerated first." A true in-memory database (or a named
// mode=memory&cache=shared one) only lives as long as some connection to it
// stays open, and this plugin's own tasks each open and close their own
// connection per call (doc.go, "Transactions end where the activity ends")
// - so a fixture written through a *different*, already-closed connection
// would already be gone by the time a task under test opens its own. A
// TempDir-backed file persists exactly the way a real deployment's sqlite
// file does.
func testDSN(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.sqlite")
}

// mustExecDirect runs setup SQL directly against a DSN, outside this
// plugin's own tasks, for building test fixtures - a real sqlite
// connection, not a mock, so a fixture behaves exactly as the database
// this plugin's own tasks will see.
func mustExecDirect(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening test fixture db: %v", err)
	}
	defer db.Close()

	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("fixture statement %q: %v", s, err)
		}
	}
}

// insertHugeRow inserts one row whose data column holds a value larger
// than this plugin's own param-size bound would allow through sql.exec -
// deliberately outside this plugin's own tasks, using database/sql
// directly, so a test of the *result*-side byte bound (rows.go) is not
// confused with the *parameter*-side one (params.go), which is bounded
// separately and would refuse a value this large before it ever reached
// the database.
func insertHugeRow(t *testing.T, dsn string, data string) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening test fixture db: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(context.Background(), "INSERT INTO blobs (id, data) VALUES (1, ?)", data); err != nil {
		t.Fatalf("inserting huge fixture row: %v", err)
	}
}

// inputsFor builds a task's input map the way DecodeInputs expects to
// receive it, mirroring plugins/codex/exec_test.go's identical helper.
func inputsFor(fields map[string]any) map[string]*flowstatev1.Value {
	out := make(map[string]*flowstatev1.Value, len(fields))
	for k, v := range fields {
		out[k] = flowstatev1.NewValue(v)
	}
	return out
}
