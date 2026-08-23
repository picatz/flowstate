package main

import (
	"bytes"
	"testing"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestTraceLinkSubstitutesOnlyTheValidatedTraceID(t *testing.T) {
	t.Setenv("FLOWSTATE_TRACE_LINK_TEMPLATE", "https://traces.example/view/{trace_id}")
	var stderr bytes.Buffer
	surface := ui.Plain(&bytes.Buffer{}, &stderr)
	require.NoError(t, writeTraceReference(surface, &v1.TraceReference{
		TraceId: "0123456789abcdef0123456789abcdef",
	}))
	require.Contains(t, stderr.String(),
		"https://traces.example/view/0123456789abcdef0123456789abcdef")
}

func TestTraceLinkRejectsAnotherPlaceholder(t *testing.T) {
	t.Setenv("FLOWSTATE_TRACE_LINK_TEMPLATE", "https://traces.example/{trace_id}/{vendor}")
	require.Error(t, writeTraceReference(ui.Plain(&bytes.Buffer{}, &bytes.Buffer{}), &v1.TraceReference{
		TraceId: "0123456789abcdef0123456789abcdef",
	}))
}
