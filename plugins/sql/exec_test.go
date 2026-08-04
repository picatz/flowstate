package main

import (
	"context"
	"database/sql"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	_ "modernc.org/sqlite"
)

func TestStatementsFromValueRefusesEmpty(t *testing.T) {
	if _, err := statementsFromValue(flowstatev1.NewValue([]any{})); err == nil {
		t.Error("statementsFromValue([]): got no error, want a refusal")
	}
}

func TestStatementsFromValueRefusesOverCeiling(t *testing.T) {
	stmts := make([]any, maxStatements+1)
	for i := range stmts {
		stmts[i] = map[string]any{"sql": "SELECT 1"}
	}
	if _, err := statementsFromValue(flowstatev1.NewValue(stmts)); err == nil {
		t.Error("statementsFromValue over maxStatements: got no error, want a refusal")
	}
}

func TestStatementsFromValueRequiresSQLPerEntry(t *testing.T) {
	stmts := []any{map[string]any{"params": []any{1}}}
	if _, err := statementsFromValue(flowstatev1.NewValue(stmts)); err == nil {
		t.Error("statementsFromValue with a missing sql key: got no error, want a refusal")
	}
}

func TestStatementsFromValueParsesSQLAndParams(t *testing.T) {
	stmts := []any{
		map[string]any{"sql": "INSERT INTO t VALUES (?)", "params": []any{1}},
		map[string]any{"sql": "DELETE FROM t"},
	}
	got, err := statementsFromValue(flowstatev1.NewValue(stmts))
	if err != nil {
		t.Fatalf("statementsFromValue: unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("statementsFromValue returned %d statements, want 2", len(got))
	}
	if got[0].sql != "INSERT INTO t VALUES (?)" {
		t.Errorf("got[0].sql = %q", got[0].sql)
	}
	if len(got[0].params) != 1 {
		t.Errorf("got[0].params has %d entries, want 1", len(got[0].params))
	}
	if len(got[1].params) != 0 {
		t.Errorf("got[1].params has %d entries, want 0", len(got[1].params))
	}
}

// TestSQLExecCommitsEveryStatementTogether proves the transaction-per-call
// contract's happy path: several statements in one sql.exec call all take
// effect together.
func TestSQLExecCommitsEveryStatementTogether(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance_cents INTEGER)`)

	outputs, err := sqlExec(context.Background(), inputsFor(map[string]any{
		"engine": "ENGINE_SQLITE",
		"dsn":    dsn,
		"statements": []any{
			map[string]any{"sql": "INSERT INTO accounts (id, balance_cents) VALUES (?, ?)", "params": []any{1, 1000}},
			map[string]any{"sql": "INSERT INTO accounts (id, balance_cents) VALUES (?, ?)", "params": []any{2, 2000}},
			map[string]any{"sql": "UPDATE accounts SET balance_cents = balance_cents - 100 WHERE id = ?", "params": []any{1}},
		},
	}), nil)
	if err != nil {
		t.Fatalf("sqlExec: unexpected error: %v", err)
	}

	got := outputs.GetNamedValues()
	if n := got["statement_count"].GetLiteral().GetInt64Value(); n != 3 {
		t.Errorf("statement_count = %d, want 3", n)
	}
	if n := got["total_rows_affected"].GetLiteral().GetInt64Value(); n != 3 {
		t.Errorf("total_rows_affected = %d, want 3 (one per statement)", n)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening db to verify: %v", err)
	}
	defer db.Close()

	var balance int
	if err := db.QueryRowContext(context.Background(), "SELECT balance_cents FROM accounts WHERE id = 1").Scan(&balance); err != nil {
		t.Fatalf("verifying committed state: %v", err)
	}
	if balance != 900 {
		t.Errorf("balance_cents for account 1 = %d, want 900 (all three statements should have committed)", balance)
	}
}

// TestSQLExecRollsBackEveryStatementTogether proves the other half: when
// any statement in the call fails, nothing from that call is visible
// afterward - not even the statements that ran successfully before the
// failing one.
func TestSQLExecRollsBackEveryStatementTogether(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance_cents INTEGER)`)

	_, err := sqlExec(context.Background(), inputsFor(map[string]any{
		"engine": "ENGINE_SQLITE",
		"dsn":    dsn,
		"statements": []any{
			map[string]any{"sql": "INSERT INTO accounts (id, balance_cents) VALUES (?, ?)", "params": []any{1, 1000}},
			map[string]any{"sql": "INSERT INTO accounts (id, balance_cents) VALUES (?, ?)", "params": []any{1, 2000}}, // duplicate primary key
		},
	}), nil)
	if err == nil {
		t.Fatal("sqlExec with a duplicate primary key: got no error, want a refusal")
	}
	if !sdk.IsConflict(err) {
		t.Errorf("a primary-key violation should classify as sdk.Conflict, got: %v", err)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("opening db to verify: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM accounts").Scan(&count); err != nil {
		t.Fatalf("verifying rollback: %v", err)
	}
	if count != 0 {
		t.Errorf("accounts has %d rows after a failed sql.exec call, want 0 (the whole transaction "+
			"must roll back, including the statement that succeeded before the failing one)", count)
	}
}

// TestSQLExecRequiresAtLeastOneStatement proves the schema's own floor:
// there is no BEGIN task and no COMMIT task (see doc.go), and there is also
// no empty-transaction call.
func TestSQLExecRequiresAtLeastOneStatement(t *testing.T) {
	_, err := sqlExec(context.Background(), inputsFor(map[string]any{
		"engine":     "ENGINE_SQLITE",
		"dsn":        testDSN(t),
		"statements": []any{},
	}), nil)
	if err == nil {
		t.Fatal("sqlExec with no statements: got no error, want a refusal")
	}
}

// TestSQLExecReportsLastInsertID proves sqlite's own rowid comes back for
// an INSERT. Postgres never populates this field - pgx's stdlib driver
// returns an error from sql.Result.LastInsertId (the wire protocol has no
// equivalent RPC), and runTransaction's own "if err == nil" guard (exec.go)
// leaves lastInsertID at its zero value whenever that happens - documented
// in sql.proto's own comment on ExecOutputs.last_insert_id rather than
// exercised against a live postgres server, which this module's test suite
// does not stand up; see doc.go, "Why sqlite is enumerated first."
func TestSQLExecReportsLastInsertID(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn, `CREATE TABLE accounts (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`)

	outputs, err := sqlExec(context.Background(), inputsFor(map[string]any{
		"engine": "ENGINE_SQLITE",
		"dsn":    dsn,
		"statements": []any{
			map[string]any{"sql": "INSERT INTO accounts (name) VALUES (?)", "params": []any{"alice"}},
		},
	}), nil)
	if err != nil {
		t.Fatalf("sqlExec: unexpected error: %v", err)
	}
	if id := outputs.GetNamedValues()["last_insert_id"].GetLiteral().GetInt64Value(); id != 1 {
		t.Errorf("last_insert_id = %d, want 1", id)
	}
}
