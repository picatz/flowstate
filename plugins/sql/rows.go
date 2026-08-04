package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// scanBoundedRows reads every row of a result set into CEL-shaped values,
// refusing rather than truncating the moment a row beyond maxRows is seen -
// see doc.go, "Bounded results," for why this task never returns a shorter
// result and calls it done.
//
// The query itself is never rewritten to add a LIMIT: doing so would mean
// this plugin parsing or appending to SQL text it promises to run verbatim
// (see doc.go, "Parameterized only, structurally" - the same rule that
// keeps params out of the query string keeps this plugin's own bound-check
// out of it too). Instead this reads at most maxRows+1 rows from the
// driver's own cursor and refuses if the (maxRows+1)th exists, which bounds
// what this task ever holds in memory or returns without touching the SQL
// the author wrote.
func scanBoundedRows(rows *sql.Rows, maxRows int) (columns []string, out []map[string]any, err error) {
	columns, err = rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	dest := make([]any, len(columns))
	scanTargets := make([]any, len(columns))
	for i := range dest {
		scanTargets[i] = &dest[i]
	}

	var resultBytes int
	count := 0
	for rows.Next() {
		if count == maxRows {
			return nil, nil, sdk.Failed(
				"the query returned more than max_rows=%d rows; refusing to return a truncated "+
					"result - narrow the query (a WHERE clause, a LIMIT the query itself declares) "+
					"or raise max_rows, up to the %d row ceiling this task enforces",
				maxRows, maxMaxRows)
		}

		if err := rows.Scan(scanTargets...); err != nil {
			return nil, nil, err
		}

		row := make(map[string]any, len(columns))
		rowBytes := 0
		for i, col := range columns {
			converted, n, err := convertColumnValue(dest[i])
			if err != nil {
				return nil, nil, fmt.Errorf("column %q: %w", col, err)
			}
			row[col] = converted
			rowBytes += n
		}

		if rowBytes > maxRowBytes {
			return nil, nil, sdk.Failed(
				"row %d decoded to %d bytes, over the %d byte per-row ceiling this task enforces; "+
					"refusing to return a truncated row - select fewer or narrower columns",
				count, rowBytes, maxRowBytes)
		}
		resultBytes += rowBytes
		if resultBytes > maxResultBytes {
			return nil, nil, sdk.Failed(
				"the result decoded to over %d bytes across %d rows, the ceiling this task enforces "+
					"on a result's total decoded size; refusing to return a truncated result - narrow "+
					"the query or lower max_rows",
				maxResultBytes, count+1)
		}

		out = append(out, row)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return columns, out, nil
}

// convertColumnValue turns one driver-scanned column value into a type
// [flowstatev1.NewValue] (via [sdk.Literal]) can represent, and reports its
// own approximate encoded size for the byte bounds above.
//
// database/sql's documented scan-into-any behavior is one of six shapes:
// nil, bool, []byte, float64, int64, string, or time.Time (see
// database/sql.Rows.Scan's own doc comment) - every driver used here scans
// into one of those, so an unrecognized Go type is refused rather than
// stringified with %v, which would silently misrepresent it as text CEL has
// no way to tell apart from a real string column.
func convertColumnValue(v any) (any, int, error) {
	switch val := v.(type) {
	case nil:
		return nil, 0, nil
	case bool:
		return val, 1, nil
	case int64:
		return val, 8, nil
	case float64:
		return val, 8, nil
	case string:
		return val, len(val), nil
	case []byte:
		// A BLOB/BYTEA column and a TEXT column scanned generically both
		// arrive as []byte from these drivers; converting to a Go string is
		// the same choice database/sql's own RawBytes documentation
		// describes as the common case, reported here rather than silently
		// assumed: binary column data that is not valid UTF-8 still
		// round-trips as a Go string (Go strings are byte sequences, not
		// validated UTF-8), but a CEL expression treating it as text may see
		// something that does not look like readable text. See the README,
		// "Typed rows," for this stated plainly.
		return string(val), len(val), nil
	case time.Time:
		s := val.Format(time.RFC3339Nano)
		return s, len(s), nil
	default:
		return nil, 0, fmt.Errorf(
			"scanned as unsupported Go type %T; this task only converts the six shapes "+
				"database/sql documents for a generic scan (nil, bool, []byte, float64, int64, "+
				"string, time.Time) - refusing rather than guessing at a text representation", v)
	}
}
