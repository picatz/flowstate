package main

import (
	"strings"
	"testing"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

func TestValidateEngineRefusesUnspecified(t *testing.T) {
	if err := validateEngine(sqlv1.Engine_ENGINE_UNSPECIFIED); err == nil {
		t.Error("validateEngine(UNSPECIFIED): got no error, want a refusal")
	}
}

// TestValidateEngineTellsAnUnknownNumberFromAnOmittedOne: a nonzero number
// the enum does not name is not "required", it is unsupported, and the
// refusal says which while still listing what the build opens.
func TestValidateEngineTellsAnUnknownNumberFromAnOmittedOne(t *testing.T) {
	err := validateEngine(sqlv1.Engine(99))
	if err == nil {
		t.Fatal("validateEngine(99): got no error, want a refusal")
	}
	if got := err.Error(); !strings.Contains(got, "not one this build supports") || strings.Contains(got, "required") {
		t.Errorf("validateEngine(99) = %q, want an unsupported-engine refusal rather than a required-field one", got)
	} else if !strings.Contains(got, "postgres") {
		t.Errorf("validateEngine(99) = %q, want the supported engines listed", got)
	}
}

func TestValidateEngineAcceptsEachSupportedEngine(t *testing.T) {
	for _, e := range []sqlv1.Engine{sqlv1.Engine_ENGINE_SQLITE, sqlv1.Engine_ENGINE_POSTGRES} {
		if err := validateEngine(e); err != nil {
			t.Errorf("validateEngine(%v): unexpected error: %v", e, err)
		}
	}
}

func TestValidateQueryTextRefusesBlank(t *testing.T) {
	if err := validateQueryText("   ", maxQueryBytes); err == nil {
		t.Error("validateQueryText(blank): got no error, want a refusal")
	}
}

func TestValidateQueryTextRefusesOverBound(t *testing.T) {
	if err := validateQueryText("SELECT 1", 4); err == nil {
		t.Error("validateQueryText over its byte bound: got no error, want a refusal")
	}
}
