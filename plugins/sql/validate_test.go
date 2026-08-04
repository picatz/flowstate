package main

import (
	"testing"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

func TestValidateEngineRefusesUnspecified(t *testing.T) {
	if err := validateEngine(sqlv1.Engine_ENGINE_UNSPECIFIED); err == nil {
		t.Error("validateEngine(UNSPECIFIED): got no error, want a refusal")
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
