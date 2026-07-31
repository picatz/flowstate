package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTheSchemaSaysWhatTheServerWillRefuse pins the required derivation review
// asked for: a client validating {} against the advertised schema must be
// refused client-side where the tool boundary would refuse it, from the same
// protovalidate rules the server enforces.
func TestTheSchemaSaysWhatTheServerWillRefuse(t *testing.T) {
	t.Parallel()

	schema := schemaForMessage((&v1.ValidateRequest{}).ProtoReflect().Descriptor())
	require.Contains(t, schema, "required",
		"ValidateRequest requires files (min_items 1) and the schema says nothing is required")
	assert.Contains(t, schema["required"], "files")

	run := schemaForMessage((&v1.RunRequest{}).ProtoReflect().Descriptor())
	require.Contains(t, run, "required")
	assert.Contains(t, run["required"], "workflow")

	get := schemaForMessage((&v1.GetRequest{}).ProtoReflect().Descriptor())
	require.Contains(t, get, "required")
	assert.Contains(t, get["required"], "workflowId",
		"required names must be the protojson spelling, since that is what the arguments arrive in")

	// The negative direction, so this is not simply answering everything.
	list := schemaForMessage((&v1.ListRequest{}).ProtoReflect().Descriptor())
	assert.NotContains(t, list, "required",
		"ListRequest requires nothing and the schema claims otherwise, so a bare listing "+
			"would be refused client-side")
}
