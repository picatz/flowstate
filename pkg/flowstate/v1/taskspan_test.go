package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSecretReferenceAttributesSeeIntoAStructure pins the direction the original
// helper could not have: a reference nested inside a header map or a json body
// must be named on the span beside a top-level one, because a trace that lists
// some of a step's secrets reads as the whole list to anyone deciding what a
// denied step asked for.
//
// It moved here with the function it covers (#523's gap 3): the task span's
// vocabulary belongs to the package both drivers import, so this now pins the
// behaviour for local runs as well as durable ones rather than for the engine
// alone.
func TestSecretReferenceAttributesSeeIntoAStructure(t *testing.T) {
	t.Parallel()

	task := &v1.Task{
		Name: "http",
		Inputs: map[string]*v1.Value{
			"bearer": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}},
			"headers": {Kind: &v1.Value_Structure_{Structure: &v1.Value_Structure{
				Kind: &v1.Value_Structure_Map_{Map: &v1.Value_Structure_Map{Entries: map[string]*v1.Value{
					"X-Api-Key": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "API_KEY"}}},
				}}},
			}}},
		},
	}

	attrs := v1.SecretReferenceAttributes(task)

	var refs []string
	for _, attr := range attrs {
		if string(attr.Key) == v1.SpanAttributeSecretRefs {
			refs = attr.Value.AsStringSlice()
		}
	}

	assert.Equal(t, []string{"env:API_KEY", "env:TOKEN"}, refs,
		"a reference nested in a structure was missing from the span, so the trace under-reported what the step reads")
}
