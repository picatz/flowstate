package flowtest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestSuiteWarningBudgetBoundsRetainedDiagnostics(t *testing.T) {
	b := newSuiteWarningBudget()
	input := make([]*v1.Diagnostic, maxSuiteWarnings+20)
	for i := range input {
		input[i] = &v1.Diagnostic{Field: "stubs", Message: strings.Repeat("x", maxWarningMessageBytes+1)}
	}

	got := b.take(input)
	require.LessOrEqual(t, len(got), maxSuiteWarnings)
	for _, warning := range got[:len(got)-1] {
		assert.LessOrEqual(t, len(warning.GetMessage()), maxWarningMessageBytes)
	}
	assert.Contains(t, got[len(got)-1].GetMessage(), "additional warning(s) omitted")
	retainedBytes := 0
	for _, warning := range got {
		retainedBytes += len(warning.GetMessage())
	}
	assert.LessOrEqual(t, retainedBytes, maxSuiteWarningBytes)
}
