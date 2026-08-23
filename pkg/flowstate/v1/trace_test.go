package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestTraceReferenceAcceptsOnlyCanonicalHexadecimalIdentifiers(t *testing.T) {
	valid := &v1.TraceReference{
		TraceId:    "0123456789abcdef0123456789abcdef",
		RootSpanId: stringPointer("0123456789abcdef"),
	}
	require.NoError(t, v1.Validate(valid))

	for _, malformed := range []*v1.TraceReference{
		{TraceId: "0123456789ABCDEF0123456789ABCDEF"},
		{TraceId: "0123456789abcdef"},
		{TraceId: "0123456789abcdef0123456789abcdeg"},
		{TraceId: "00000000000000000000000000000000"},
		{TraceId: valid.GetTraceId(), RootSpanId: stringPointer("0123456789ABCDEG")},
		{TraceId: valid.GetTraceId(), RootSpanId: stringPointer("0000000000000000")},
	} {
		require.Error(t, v1.Validate(malformed))
	}
}

func TestRunWithoutTelemetryHasNoTraceReference(t *testing.T) {
	response := &v1.GetResponse{}
	require.Nil(t, response.GetTrace())
}

func stringPointer(value string) *string { return &value }
