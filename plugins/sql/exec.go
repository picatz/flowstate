package main

import (
	"context"
	"database/sql"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// sqlExec implements sql.exec: every statement in one call runs inside one
// transaction that begins and ends inside this single activity invocation -
// see doc.go, "Transactions end where the activity ends," for why that is
// not merely this task's default behavior but the only behavior this
// schema can express at all.
func sqlExec(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in sqlv1.ExecInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	if err := validateEngine(in.GetEngine()); err != nil {
		return nil, err
	}
	statements, err := statementsFromValue(in.GetStatements())
	if err != nil {
		return nil, err
	}

	dsn, err := dsnFromValue(in.GetDsn())
	if err != nil {
		return nil, err
	}

	scrubber := secrets.NewScrubber()
	scrubber.AddValue(dsn)

	callCtx, cancel := context.WithTimeout(ctx, queryTimeout*time.Second)
	defer cancel()

	db, err := openDB(in.GetEngine(), dsn)
	if err != nil {
		return nil, scrubber.ScrubError(err)
	}
	defer db.Close()

	total, lastID, err := runTransaction(callCtx, db, statements, scrubber)
	if err != nil {
		return nil, err // already classified and scrubbed by runTransaction
	}

	return sdk.EncodeOutputs(&sqlv1.ExecOutputs{
		TotalRowsAffected: total,
		LastInsertId:      lastID,
		StatementCount:    int32(len(statements)),
	})
}

// execStatement is one parsed statement from ExecInputs.statements - see
// statementsFromValue for why this plugin parses that field itself rather
// than declaring a repeated message of its own.
type execStatement struct {
	sql    string
	params []*flowstatev1.Value
}

// statementsFromValue parses and validates ExecInputs.statements, working
// around the same [sdk.DecodeInputs] limitation plugins/codex/exec.go
// documents on the output side for [sdk.EncodeOutputs]: neither function
// converts a repeated field of a plugin-defined message type, only
// flowstate.v1.Value and google.api.expr.v1alpha1.Value themselves. So this
// field is declared as flowstate.v1.Value (sql.proto) and parsed here by
// hand: a list of maps, each with a required "sql" string and an optional
// "params" list.
func statementsFromValue(v *flowstatev1.Value) ([]execStatement, error) {
	list := v.GetLiteral().GetListValue()
	if list == nil {
		return nil, sdk.InvalidInput("statements must be a list; sql.exec runs at least one statement")
	}
	if len(list.GetValues()) == 0 {
		return nil, sdk.InvalidInput("statements must not be empty; sql.exec runs at least one statement")
	}
	if len(list.GetValues()) > maxStatements {
		return nil, sdk.InvalidInput("statements has %d entries, over the %d statement ceiling this task enforces", len(list.GetValues()), maxStatements)
	}

	out := make([]execStatement, len(list.GetValues()))
	for i, entry := range list.GetValues() {
		m := entry.GetMapValue()
		if m == nil {
			return nil, sdk.InvalidInput("statements[%d] must be a map with \"sql\" and, optionally, \"params\"", i)
		}

		var stmt execStatement
		var sawSQL bool
		for _, e := range m.GetEntries() {
			switch e.GetKey().GetStringValue() {
			case "sql":
				s, ok := e.GetValue().GetKind().(*expr.Value_StringValue)
				if !ok {
					return nil, sdk.InvalidInput("statements[%d].sql must be a string", i)
				}
				stmt.sql = s.StringValue
				sawSQL = true
			case "params":
				params := e.GetValue().GetListValue()
				if params == nil {
					return nil, sdk.InvalidInput("statements[%d].params must be a list", i)
				}
				stmt.params = make([]*flowstatev1.Value, len(params.GetValues()))
				for j, p := range params.GetValues() {
					stmt.params[j] = &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: p}}
				}
			}
		}
		if !sawSQL {
			return nil, sdk.InvalidInput("statements[%d] is missing \"sql\"", i)
		}
		if err := validateQueryText(stmt.sql, maxStatementBytes); err != nil {
			return nil, sdk.InvalidInput("statements[%d]: %v", i, err)
		}

		out[i] = stmt
	}

	return out, nil
}

// runTransaction runs every statement in order inside one transaction,
// committing only if every one of them succeeds, and returns a fully
// classified, scrubbed error on any failure - see errors.go's
// classifyExecError for what each phase means for retry safety.
func runTransaction(ctx context.Context, db *sql.DB, statements []execStatement, scrubber *secrets.Scrubber) (totalRowsAffected, lastInsertID int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, classifyExecError(err, phaseBegin, scrubber)
	}

	for i, stmt := range statements {
		args, err := paramsToArgs(stmt.params)
		if err != nil {
			_ = tx.Rollback() // best effort; the statement never ran, so nothing to be ambiguous about
			return 0, 0, scrubber.ScrubError(err)
		}

		res, err := tx.ExecContext(ctx, stmt.sql, args...)
		if err != nil {
			if rerr := tx.Rollback(); rerr != nil {
				// The statement failed *and* the rollback that was supposed
				// to undo it also failed - this task cannot now assert the
				// transaction never took effect, which is exactly the
				// ambiguity [sdk.OutcomeUnknown] exists for.
				return 0, 0, sdk.OutcomeUnknown(
					"statement %d failed and the follow-up rollback also failed; this transaction's "+
						"outcome cannot be determined from here, so this is not retried automatically: "+
						"statement error: %s; rollback error: %s",
					i, scrubber.Scrub(err.Error()), scrubber.Scrub(rerr.Error()))
			}
			return 0, 0, classifyExecError(err, phaseStatement, scrubber)
		}

		if n, err := res.RowsAffected(); err == nil {
			totalRowsAffected += n
		}
		if id, err := res.LastInsertId(); err == nil {
			lastInsertID = id
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, classifyExecError(err, phaseCommit, scrubber)
	}

	return totalRowsAffected, lastInsertID, nil
}
