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

// TestSQLTransferPatternMovesMoneyExactlyOnceAcrossARetry is the P1-1
// regression test: examples/plugins/sql/transfer.yaml's own four-statement
// pattern (an idempotency-key claim, two guarded balance updates, and a
// flag flip - see that file's own doc comment for "why an ON CONFLICT DO
// NOTHING insert is not enough on its own"), run twice with the identical
// idempotency_key through the real sql.exec task function against sqlite,
// the hermetic engine this module's tests can actually stand up (see
// doc.go, "Why sqlite is enumerated first" - transfer.yaml itself targets
// ENGINE_POSTGRES and uses $1-style placeholders, which this test cannot
// run without a live server; it proves the identical *pattern* instead,
// translated to sqlite's `?` placeholders, which is what an idempotency
// claim actually rests on - the guard logic, not the engine).
//
// An earlier version of transfer.yaml suppressed only the ledger INSERT on
// conflict and left both balance UPDATEs unconditional - this test is what
// that version would have failed: the second call's balances would have
// moved a second time. See doc.go's own "Transactions end where the
// activity ends" for why sql.exec's retry story depends on this working,
// not merely reading as though it should.
func TestSQLTransferPatternMovesMoneyExactlyOnceAcrossARetry(t *testing.T) {
	dsn := testDSN(t)
	mustExecDirect(t, dsn,
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance_cents INTEGER)`,
		`CREATE TABLE accounts_ledger (idempotency_key TEXT PRIMARY KEY, applied INTEGER NOT NULL DEFAULT 0)`,
		`INSERT INTO accounts (id, balance_cents) VALUES (1, 1000), (2, 2000)`,
	)

	const idempotencyKey = "transfer-42"
	const amountCents = 250

	transfer := func() *flowstatev1.Node_Outputs {
		outputs, err := sqlExec(context.Background(), inputsFor(map[string]any{
			"engine": "ENGINE_SQLITE",
			"dsn":    dsn,
			"statements": []any{
				map[string]any{
					"sql":    "INSERT INTO accounts_ledger (idempotency_key) VALUES (?) ON CONFLICT (idempotency_key) DO NOTHING",
					"params": []any{idempotencyKey},
				},
				map[string]any{
					"sql":    "UPDATE accounts SET balance_cents = balance_cents - ? WHERE id = ? AND EXISTS (SELECT 1 FROM accounts_ledger WHERE idempotency_key = ? AND applied = 0)",
					"params": []any{amountCents, 1, idempotencyKey},
				},
				map[string]any{
					"sql":    "UPDATE accounts SET balance_cents = balance_cents + ? WHERE id = ? AND EXISTS (SELECT 1 FROM accounts_ledger WHERE idempotency_key = ? AND applied = 0)",
					"params": []any{amountCents, 2, idempotencyKey},
				},
				map[string]any{
					"sql":    "UPDATE accounts_ledger SET applied = 1 WHERE idempotency_key = ?",
					"params": []any{idempotencyKey},
				},
			},
		}), nil)
		if err != nil {
			t.Fatalf("sqlExec (transfer pattern): unexpected error: %v", err)
		}
		return outputs
	}

	balances := func() (from, to int) {
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("opening db to verify: %v", err)
		}
		defer db.Close()
		if err := db.QueryRowContext(context.Background(), "SELECT balance_cents FROM accounts WHERE id = 1").Scan(&from); err != nil {
			t.Fatalf("reading account 1 balance: %v", err)
		}
		if err := db.QueryRowContext(context.Background(), "SELECT balance_cents FROM accounts WHERE id = 2").Scan(&to); err != nil {
			t.Fatalf("reading account 2 balance: %v", err)
		}
		return from, to
	}

	first := transfer()
	if n := first.GetNamedValues()["total_rows_affected"].GetLiteral().GetInt64Value(); n != 4 {
		t.Errorf("first call total_rows_affected = %d, want 4 (insert, debit, credit, flag)", n)
	}
	fromAfterFirst, toAfterFirst := balances()
	if fromAfterFirst != 1000-amountCents {
		t.Fatalf("account 1 balance after first transfer = %d, want %d", fromAfterFirst, 1000-amountCents)
	}
	if toAfterFirst != 2000+amountCents {
		t.Fatalf("account 2 balance after first transfer = %d, want %d", toAfterFirst, 2000+amountCents)
	}

	// The retry: same idempotency_key, simulating a replay after this
	// call's own commit acknowledgement was lost (sdk.OutcomeUnknown,
	// exec.go).
	second := transfer()
	if n := second.GetNamedValues()["total_rows_affected"].GetLiteral().GetInt64Value(); n != 1 {
		t.Errorf("retried call total_rows_affected = %d, want 1 (only the harmless flag-set touches a "+
			"row; the insert conflicts and both balance guards evaluate false)", n)
	}

	fromAfterRetry, toAfterRetry := balances()
	if fromAfterRetry != fromAfterFirst {
		t.Errorf("account 1 balance changed on retry: %d -> %d; a replayed idempotency_key must not "+
			"move money a second time", fromAfterFirst, fromAfterRetry)
	}
	if toAfterRetry != toAfterFirst {
		t.Errorf("account 2 balance changed on retry: %d -> %d; a replayed idempotency_key must not "+
			"move money a second time", toAfterFirst, toAfterRetry)
	}
}
