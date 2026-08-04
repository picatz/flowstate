package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// columnName builds a deterministic, distinct column name for a
// programmatically generated wide table.
func columnName(i int) string {
	return fmt.Sprintf("c%d", i)
}

func TestConvertColumnValueHandlesEveryDocumentedShape(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"int64", int64(42), int64(42)},
		{"float64", 3.5, 3.5},
		{"string", "hi", "hi"},
		{"bytes", []byte("blob"), "blob"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _, err := convertColumnValue(c.in)
			if err != nil {
				t.Fatalf("convertColumnValue(%v): unexpected error: %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("convertColumnValue(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestConvertColumnValueFormatsTimeAsRFC3339(t *testing.T) {
	when := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	got, n, err := convertColumnValue(when)
	if err != nil {
		t.Fatalf("convertColumnValue(time.Time): unexpected error: %v", err)
	}
	want := when.Format(time.RFC3339Nano)
	if got != want {
		t.Errorf("convertColumnValue(time.Time) = %v, want %v", got, want)
	}
	if n != len(want) {
		t.Errorf("reported size = %d, want %d", n, len(want))
	}
}

// TestConvertColumnValueRefusesAnUnrecognizedType proves this task fails
// closed on a Go type outside database/sql's own documented scan shapes,
// rather than silently stringifying something that might not be text.
func TestConvertColumnValueRefusesAnUnrecognizedType(t *testing.T) {
	type notAScanShape struct{ X int }
	if _, _, err := convertColumnValue(notAScanShape{X: 1}); err == nil {
		t.Fatal("convertColumnValue with an unrecognized type: got no error, want one")
	}
}

// openTestRows runs a query against a fresh sqlite fixture and returns the
// live *sql.Rows for a test to hand directly to scanBoundedRows - lower
// level than sqlQuery, so a test can pass small, precise custom bounds
// instead of the production constants.
func openTestRows(t *testing.T, ddl, query string) (*sql.Rows, func()) {
	t.Helper()
	dsn := testDSN(t)
	mustExecDirect(t, dsn, ddl)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening db: %v", err)
	}
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		db.Close()
		t.Fatalf("query: %v", err)
	}
	return rows, func() { rows.Close(); db.Close() }
}

// TestScanBoundedRowsStructuralOverheadIsReachedExactly is the P1-3
// regression test, at the boundary the house rule (CLAUDE.md, "Test the
// traversal, not just the step") asks for: a bound is proven not merely
// "not exceeded" but actually *reached*. Three NULL columns report 0 bytes
// each from convertColumnValue - a naive sum-of-values bound would never
// refuse this row no matter how small maxRowBytes was set - so the only
// thing that can trigger a refusal here is bounds.go's own structural
// accounting (perRowOverheadBytes + 3*perCellOverheadBytes), computed once
// and asserted exactly: that size succeeds, one byte less refuses.
func TestScanBoundedRowsStructuralOverheadIsReachedExactly(t *testing.T) {
	const columnCount = 3
	exact := perRowOverheadBytes + columnCount*perCellOverheadBytes

	rows, cleanup := openTestRows(t, `CREATE TABLE t (a, b, c)`, `SELECT NULL AS a, NULL AS b, NULL AS c`)
	_, _, err := scanBoundedRows(rows, maxMaxRows, exact, maxResultBytes)
	cleanup()
	if err != nil {
		t.Fatalf("scanBoundedRows at exactly the structural size (%d bytes): unexpected error: %v", exact, err)
	}

	rows2, cleanup2 := openTestRows(t, `CREATE TABLE t (a, b, c)`, `SELECT NULL AS a, NULL AS b, NULL AS c`)
	_, _, err = scanBoundedRows(rows2, maxMaxRows, exact-1, maxResultBytes)
	cleanup2()
	if err == nil {
		t.Fatalf("scanBoundedRows one byte under the structural size (%d bytes): got no error, want a "+
			"refusal - three all-NULL columns report 0 value bytes, so only the structural accounting "+
			"(bounds.go's perRowOverheadBytes/perCellOverheadBytes) can be what refuses this", exact-1)
	}
}

// TestScanBoundedRowsRefusesAWideAllNullRowOnStructureAlone is the same
// proof against maxResultBytes rather than maxRowBytes, and against a row
// wide enough (columnCount columns, all NULL) that a value-bytes-only
// accounting - resultBytes staying at 0 regardless of row count - would
// never refuse it no matter how many such rows arrived, which is exactly
// the bypass Codex's review reported: "100,000 rows with hundreds of NULL
// columns can consume gigabytes while resultBytes remains zero."
func TestScanBoundedRowsRefusesAWideAllNullRowOnStructureAlone(t *testing.T) {
	const columnCount = 50
	rowStructuralBytes := perRowOverheadBytes + columnCount*perCellOverheadBytes

	var ddl, query strings.Builder
	ddl.WriteString("CREATE TABLE wide (")
	query.WriteString("SELECT ")
	for i := range columnCount {
		if i > 0 {
			ddl.WriteString(", ")
			query.WriteString(", ")
		}
		col := columnName(i)
		ddl.WriteString(col)
		query.WriteString("NULL AS " + col)
	}
	ddl.WriteString(")")

	rows, cleanup := openTestRows(t, ddl.String(), query.String())
	defer cleanup()

	// A value-bytes-only bound at rowStructuralBytes-1 would happily accept
	// this row (every column reports 0 value bytes); with structural
	// accounting, one all-NULL row alone reaches it.
	_, _, err := scanBoundedRows(rows, maxMaxRows, maxRowBytes, rowStructuralBytes-1)
	if err == nil {
		t.Fatalf("a %d-column all-NULL row against a %d byte result ceiling: got no error, want a "+
			"refusal from structural accounting alone (every column's own value is 0 bytes)",
			columnCount, rowStructuralBytes-1)
	}
}

// TestScanBoundedRowsRefusesDuplicateColumnNames is the P2-2 regression
// test: a join producing two columns of the same name must be refused with
// a diagnostic, not silently let the second overwrite the first in the row
// map.
func TestScanBoundedRowsRefusesDuplicateColumnNames(t *testing.T) {
	rows, cleanup := openTestRows(t, `CREATE TABLE t (id INTEGER)`, `SELECT 1 AS id, 2 AS id`)
	defer cleanup()

	_, _, err := scanBoundedRows(rows, maxMaxRows, maxRowBytes, maxResultBytes)
	if err == nil {
		t.Fatal("scanBoundedRows with two columns named \"id\": got no error, want a refusal")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Errorf("error does not name the duplicate column: %v", err)
	}
}

// TestSQLQueryEndToEndCatchesAWideNullResultThatValueBytesAloneWouldMiss
// runs the exact scenario Codex's review named, end to end through
// sqlQuery with the real production constants (maxRowBytes, maxResultBytes)
// - not a synthetic small bound - so this is proof the fix actually closes
// the gap in the shipped configuration, not only in a test-scaled one.
// Scaled down from "100,000 rows, hundreds of columns" to a few hundred
// rows of 100 NULL columns each, which is enough to cross maxResultBytes on
// structural overhead alone (100 * perCellOverheadBytes + perRowOverheadBytes
// per row) while staying fast.
func TestSQLQueryEndToEndCatchesAWideNullResultThatValueBytesAloneWouldMiss(t *testing.T) {
	const columnCount = 100
	rowStructuralBytes := perRowOverheadBytes + columnCount*perCellOverheadBytes
	rowsNeeded := maxResultBytes/rowStructuralBytes + 10 // comfortably over the ceiling

	dsn := testDSN(t)
	var ddl strings.Builder
	ddl.WriteString("CREATE TABLE wide (")
	for i := range columnCount {
		if i > 0 {
			ddl.WriteString(", ")
		}
		ddl.WriteString(columnName(i))
	}
	ddl.WriteString(")")
	mustExecDirect(t, dsn, ddl.String())
	mustExecDirect(t, dsn, fmt.Sprintf(
		`INSERT INTO wide (%s) WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < %d) SELECT NULL FROM seq`,
		columnName(0), rowsNeeded))

	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT * FROM wide",
		"max_rows": int32(maxMaxRows),
	}), nil)
	if err == nil {
		t.Fatalf("sql.query over %d rows of %d all-NULL columns: got no error, want the production "+
			"maxResultBytes ceiling to refuse this on structural overhead alone (value bytes reported "+
			"~0 throughout)", rowsNeeded, columnCount)
	}
}
