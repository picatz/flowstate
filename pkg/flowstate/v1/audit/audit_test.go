package audit

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

func TestRecordUsesSignalScrubbingAndBounds(t *testing.T) {
	s := secrets.NewScrubber()
	const material = "super-secret-material"
	s.AddValue(material)
	r := NewRecord(s, "signal", "denied", material, "team", strings.Repeat("é", 300), "contains "+material)
	require.Equal(t, secrets.Redacted, r.Subject)
	require.NotContains(t, r.Reason, material)
	require.LessOrEqual(t, len(r.Resource), MaxFieldBytes)
	require.Contains(t, r.Resource, "...(truncated)")
}
