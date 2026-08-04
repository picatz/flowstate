package main

import (
	"context"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestParamsToArgsNeverInterpolatesIntoSQLText is the injection-shaped
// proof CLAUDE.md's parameterized-only design calls for: a value carrying
// what would be a devastating SQL fragment if it were ever spliced into
// query text, run through this plugin's own sql.query task as an ordinary
// bound parameter, against a real database - not a mock, so there is
// nothing between this test and the actual driver to have quietly done the
// splicing instead.
//
// If paramsToArgs (or anything upstream of it) ever started building query
// text from a parameter, this test would either error (a syntactically
// invalid multi-statement fragment where one value was expected) or - far
// worse - silently drop the accounts table. It does neither: the row
// matching the literal string is simply absent, and the table is provably
// still there afterward.
func TestParamsToArgsNeverInterpolatesIntoSQLText(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO accounts (id, name) VALUES (1, 'alice')`,
	)

	const payload = `alice'; DROP TABLE accounts; --`

	outputs, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT id FROM accounts WHERE name = ?",
		"params":   []any{payload},
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("sqlQuery with an injection-shaped parameter: unexpected error: %v - "+
			"a bound parameter must never produce a SQL syntax error, because it must never "+
			"become part of the SQL text at all", err)
	}
	if n := outputs.GetNamedValues()["row_count"].GetLiteral().GetInt64Value(); n != 0 {
		t.Fatalf("row_count = %d, want 0: the payload does not equal the stored name 'alice', "+
			"so a correctly bound parameter matches nothing", n)
	}

	// The table must still exist and still hold its one real row - the
	// direct proof that "; DROP TABLE accounts; --" never executed as SQL.
	confirm, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_SQLITE",
		"dsn":      dsn,
		"query":    "SELECT id, name FROM accounts",
		"max_rows": int32(10),
	}), nil)
	if err != nil {
		t.Fatalf("querying accounts after the injection-shaped parameter: unexpected error "+
			"(the table may have been dropped): %v", err)
	}
	if n := confirm.GetNamedValues()["row_count"].GetLiteral().GetInt64Value(); n != 1 {
		t.Fatalf("accounts has %d rows after the injection-shaped parameter, want 1 (the table "+
			"must be untouched)", n)
	}
}

// TestParamsToArgsRefusesAListOrMap proves the structural half of
// "parameterized only": a param has no single-placeholder binding for a
// list or a map, and this is refused rather than silently stringified into
// something that would look like it worked.
func TestParamsToArgsRefusesAListOrMap(t *testing.T) {
	list := flowstatev1.NewValue([]any{"a", "b"})
	if _, err := paramsToArgs([]*flowstatev1.Value{list}); err == nil {
		t.Error("paramsToArgs with a list parameter: got no error, want one")
	}

	m := flowstatev1.NewValue(map[string]any{"a": 1})
	if _, err := paramsToArgs([]*flowstatev1.Value{m}); err == nil {
		t.Error("paramsToArgs with a map parameter: got no error, want one")
	}
}

// TestParamsToArgsConvertsEveryScalarKind is the ordinary path's own
// coverage, so the refusal tests above are read against a baseline of what
// does work.
func TestParamsToArgsConvertsEveryScalarKind(t *testing.T) {
	values := []*flowstatev1.Value{
		flowstatev1.NewValue("text"),
		flowstatev1.NewValue(int64(42)),
		flowstatev1.NewValue(3.5),
		flowstatev1.NewValue(true),
		flowstatev1.NewValue(nil),
	}
	args, err := paramsToArgs(values)
	if err != nil {
		t.Fatalf("paramsToArgs: unexpected error: %v", err)
	}
	if len(args) != 5 {
		t.Fatalf("paramsToArgs returned %d args, want 5", len(args))
	}
	if args[0] != "text" {
		t.Errorf("args[0] = %v, want %q", args[0], "text")
	}
	if args[1] != int64(42) {
		t.Errorf("args[1] = %v, want int64(42)", args[1])
	}
	if args[4] != nil {
		t.Errorf("args[4] = %v, want nil", args[4])
	}
}

// TestParamsToArgsBoundsCount proves the parameter-count ceiling is
// enforced.
func TestParamsToArgsBoundsCount(t *testing.T) {
	values := make([]*flowstatev1.Value, maxParams+1)
	for i := range values {
		values[i] = flowstatev1.NewValue(i)
	}
	if _, err := paramsToArgs(values); err == nil {
		t.Error("paramsToArgs over maxParams: got no error, want a refusal")
	}
}
