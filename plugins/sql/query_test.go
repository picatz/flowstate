package main

import (
	"context"
	"strings"
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// TestSQLQueryReturnsTypedRows proves the headline convergence: a query's
// result set comes back as a list of maps a CEL expression can index and
// filter by field name, with values typed from the driver (an int64 stays
// a number, not a stringified one).
func TestSQLQueryReturnsTypedRows(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance_cents INTEGER, active INTEGER)`,
		`INSERT INTO accounts (id, name, balance_cents, active) VALUES (1, 'alice', 500, 1), (2, 'bob', -50, 1)`,
	)

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT id, name, balance_cents, active FROM accounts ORDER BY id",
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("sqlQuery: unexpected error: %v", err)
	}

	got := outputs.GetNamedValues()
	if n := got["row_count"].GetLiteral().GetInt64Value(); n != 2 {
		t.Fatalf("row_count = %d, want 2", n)
	}

	rows := got["rows"].GetLiteral().GetListValue().GetValues()
	if len(rows) != 2 {
		t.Fatalf("rows has %d entries, want 2", len(rows))
	}

	first := rows[0].GetMapValue()
	if first == nil {
		t.Fatal("rows[0] is not a map")
	}
	entries := make(map[string]int64, len(first.GetEntries()))
	for _, e := range first.GetEntries() {
		if e.GetKey().GetStringValue() == "balance_cents" {
			if e.GetValue().GetInt64Value() != 500 {
				t.Errorf("balance_cents = %v, want the int64 500 (typed, not stringified)", e.GetValue())
			}
		}
		if e.GetKey().GetStringValue() == "name" && e.GetValue().GetStringValue() != "alice" {
			t.Errorf("name = %v, want %q", e.GetValue(), "alice")
		}
		entries[e.GetKey().GetStringValue()] = 1
	}
	for _, want := range []string{"id", "name", "balance_cents", "active"} {
		if entries[want] == 0 {
			t.Errorf("row is missing column %q", want)
		}
	}

	columns := got["columns"].GetLiteral().GetListValue().GetValues()
	if len(columns) != 4 {
		t.Fatalf("columns has %d entries, want 4", len(columns))
	}
}

// TestSQLQueryRefusesRatherThanTruncates is the no-silent-caps rule at its
// sharpest instance: a result with more rows than max_rows must fail the
// call outright, naming the bound, never return the first max_rows rows as
// though that were the whole answer.
func TestSQLQueryRefusesRatherThanTruncates(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE items (id INTEGER PRIMARY KEY)`,
		`INSERT INTO items (id) VALUES (1), (2), (3), (4), (5)`,
	)

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT id FROM items",
		"max_rows": int32(3),
	}), nil)
	if err == nil {
		t.Fatalf("sqlQuery with 5 rows and max_rows=3: got outputs %v, want a refusal", outputs)
	}
	if !strings.Contains(err.Error(), "max_rows") {
		t.Errorf("error does not name the bound it refused on: %v", err)
	}
	if outputs != nil {
		t.Errorf("sqlQuery returned both an error and outputs; a refusal must return nil outputs, never a silent partial result: %v", outputs)
	}
}

// TestSQLQueryAcceptsExactlyMaxRows proves the boundary itself: a result
// with exactly max_rows rows (no more) must succeed, so the refusal above
// is a genuine "there is more than you asked for," not an off-by-one that
// refuses a request the caller sized correctly.
func TestSQLQueryAcceptsExactlyMaxRows(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE items (id INTEGER PRIMARY KEY)`,
		`INSERT INTO items (id) VALUES (1), (2), (3)`,
	)

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT id FROM items",
		"max_rows": int32(3),
	}), nil)
	if err != nil {
		t.Fatalf("sqlQuery with exactly max_rows rows: unexpected error: %v", err)
	}
	if n := outputs.GetNamedValues()["row_count"].GetLiteral().GetInt64Value(); n != 3 {
		t.Fatalf("row_count = %d, want 3", n)
	}
}

// TestSQLQueryRequiresMaxRows proves there is no default: an unset or
// zero-valued max_rows is refused, not silently treated as "unbounded."
func TestSQLQueryRequiresMaxRows(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE items (id INTEGER PRIMARY KEY)`)

	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine": "ENGINE_SQLITE",
		"dsn":    dsn,
		"query":  "SELECT id FROM items",
	}), nil)
	if err == nil {
		t.Fatal("sqlQuery with no max_rows: got no error, want a refusal")
	}
}

// TestSQLQueryRequiresAnEngine proves ENGINE_UNSPECIFIED is refused rather
// than silently defaulted to either driver.
func TestSQLQueryRequiresAnEngine(t *testing.T) {
	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"dsn":      testDSN(t),
		"query":    "SELECT 1",
		"max_rows": int32(1),
	}), nil)
	if err == nil {
		t.Fatal("sqlQuery with no engine: got no error, want a refusal")
	}
}

// TestSQLQueryRefusesAnEmptyQuery proves an empty query string is refused
// before ever reaching a driver.
func TestSQLQueryRefusesAnEmptyQuery(t *testing.T) {
	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      testDSN(t),
		"query":    "   ",
		"max_rows": int32(1),
	}), nil)
	if err == nil {
		t.Fatal("sqlQuery with a blank query: got no error, want a refusal")
	}
}

// TestSQLQueryHandlesNullsAndEmptyResults proves a zero-row result is a
// success (not an error), and that a NULL column becomes nil rather than a
// zero value or an error.
func TestSQLQueryHandlesNullsAndEmptyResults(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, nickname TEXT)`,
		`INSERT INTO accounts (id, nickname) VALUES (1, NULL)`,
	)

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT nickname FROM accounts WHERE id = 1",
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("sqlQuery: unexpected error: %v", err)
	}
	rows := outputs.GetNamedValues()["rows"].GetLiteral().GetListValue().GetValues()
	if len(rows) != 1 {
		t.Fatalf("rows has %d entries, want 1", len(rows))
	}
	entry := rows[0].GetMapValue().GetEntries()[0]
	if _, isNull := entry.GetValue().GetKind().(*expr.Value_NullValue); !isNull {
		t.Fatalf("NULL column decoded to %v, want an explicit null value", entry.GetValue())
	}

	empty, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT nickname FROM accounts WHERE id = 999",
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("sqlQuery over an empty result: unexpected error: %v", err)
	}
	if n := empty.GetNamedValues()["row_count"].GetLiteral().GetInt64Value(); n != 0 {
		t.Errorf("row_count for an empty result = %d, want 0", n)
	}
}

// TestSQLQueryRefusesOverTheRowByteBound proves maxRowBytes is enforced
// while scanning, independent of max_rows: a single row decoding to more
// than the per-row byte ceiling is refused even though it is only one row,
// well under any row-count bound.
func TestSQLQueryRefusesOverTheRowByteBound(t *testing.T) {
	dsn := testDSN(t)
	huge := strings.Repeat("x", maxRowBytes+1)
	mustExecDirect(t, dsn,
		`CREATE TABLE blobs (id INTEGER PRIMARY KEY, data TEXT)`,
	)
	insertHugeRow(t, dsn, huge)

	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT data FROM blobs",
		"max_rows": int32(10),
	}), nil)
	if err == nil {
		t.Fatal("sqlQuery over a row exceeding maxRowBytes: got no error, want a refusal")
	}
	if !strings.Contains(err.Error(), "byte") {
		t.Errorf("error does not name a byte bound: %v", err)
	}
}

// TestSQLQueryBindsAPositionalParameter proves the ordinary, honest path
// through params: works end to end, before params_test.go's injection-shaped
// test proves the dishonest path is refused.
func TestSQLQueryBindsAPositionalParameter(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO accounts (id, name) VALUES (1, 'alice'), (2, 'bob')`,
	)

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT id FROM accounts WHERE name = ?",
		"params":   []any{"bob"},
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("sqlQuery: unexpected error: %v", err)
	}
	if n := outputs.GetNamedValues()["row_count"].GetLiteral().GetInt64Value(); n != 1 {
		t.Fatalf("row_count = %d, want 1", n)
	}
}
