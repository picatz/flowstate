package main

import (
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// validateEngine refuses ENGINE_UNSPECIFIED and any engine number outside
// the set this build was compiled with - a value the proto enum's own
// closed set already keeps to {UNSPECIFIED, SQLITE, POSTGRES}, so this
// really only ever refuses UNSPECIFIED, but is written as a positive check
// rather than a single equality so that a future third engine value added
// to the enum without an opener() case here fails loudly instead of
// reaching openDB's own default branch silently.
func validateEngine(e sqlv1.Engine) error {
	switch e {
	case sqlv1.Engine_ENGINE_SQLITE, sqlv1.Engine_ENGINE_POSTGRES:
		return nil
	default:
		return sdk.InvalidInput(
			"engine is required; this build supports: sqlite, postgres")
	}
}

// validateQueryText bounds and sanity-checks a single query string. It does
// not - and structurally cannot - check for "SQL injection," because there
// is no code path in this plugin where a parameter becomes part of this
// text; see doc.go, "Parameterized only, structurally." What this checks is
// only what CLAUDE.md's diagnostics rule calls a property of the file: is
// there any text at all, and is it under the size this task will attempt to
// run.
func validateQueryText(query string, maxBytes int) error {
	if strings.TrimSpace(query) == "" {
		return sdk.InvalidInput("query must not be empty")
	}
	if len(query) > maxBytes {
		return sdk.InvalidInput("query is %d bytes, over the %d byte ceiling this task enforces", len(query), maxBytes)
	}
	return nil
}
