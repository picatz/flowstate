package main

import (
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"

	openaiv1 "github.com/picatz/flowstate/plugins/openai/gen/openai/v1"
)

// TestModelIsARequiredInput pins the schema's claim that the model is never
// defaulted: validation reads it from the descriptor and refuses a step without it.
func TestModelIsARequiredInput(t *testing.T) {
	fields := (&openaiv1.DecideInputs{}).ProtoReflect().Descriptor().Fields()
	if !flowstatev1.RequiredInput(fields.ByName("model")) {
		t.Fatal("model must be marked required in the schema")
	}
	if flowstatev1.RequiredInput(fields.ByName("evidence")) {
		t.Fatal("evidence is not marked required here and must stay optional")
	}
}
