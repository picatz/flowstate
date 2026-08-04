package main

import (
	"context"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// rowsValue renders a result set as the flowstate.v1.Value shape
// QueryOutputs.rows carries - see sql.proto's own comment on that field for
// why it is this rather than a repeated message of this plugin's own.
func rowsValue(rows []any) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: sdk.Literal(rows)}}
}

// sqlQuery implements sql.query: one bounded, read-only, parameterized
// query. See doc.go for the design this implements.
func sqlQuery(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in sqlv1.QueryInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	if err := validateEngine(in.GetEngine()); err != nil {
		return nil, err
	}
	if err := validateQueryText(in.GetQuery(), maxQueryBytes); err != nil {
		return nil, err
	}
	maxRows, err := clampMaxRows(in.GetMaxRows())
	if err != nil {
		return nil, err
	}

	dsn, err := dsnFromValue(in.GetDsn())
	if err != nil {
		return nil, err
	}

	// Registered before the connection ever opens, so that every error path
	// below - including one raised while merely opening the connection - is
	// scrubbed by the time it can leave this function. Same ordering
	// argument as plugins/codex/errors.go's own note: scrub the text first,
	// classify second, never the reverse.
	scrubber := secrets.NewScrubber()
	scrubber.AddValue(dsn)

	args, err := paramsToArgs(in.GetParams())
	if err != nil {
		return nil, scrubber.ScrubError(err)
	}

	callCtx, cancel := context.WithTimeout(ctx, queryTimeout*time.Second)
	defer cancel()

	db, err := openDB(in.GetEngine(), dsn)
	if err != nil {
		return nil, scrubber.ScrubError(err)
	}
	defer db.Close()

	rows, err := db.QueryContext(callCtx, in.GetQuery(), args...)
	if err != nil {
		return nil, classifyQueryError(err, scrubber)
	}
	defer rows.Close()

	columns, results, err := scanBoundedRows(rows, maxRows)
	if err != nil {
		return nil, scrubber.ScrubError(err)
	}

	rowValues := make([]any, len(results))
	for i, r := range results {
		rowValues[i] = r
	}

	return sdk.EncodeOutputs(&sqlv1.QueryOutputs{
		Rows:     rowsValue(rowValues),
		RowCount: int32(len(results)),
		Columns:  columns,
	})
}
