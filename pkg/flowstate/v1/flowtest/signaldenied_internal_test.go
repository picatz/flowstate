package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestDeniedSignalsRefusesAWebhookRefusalPairing: a replayed delivery expected
// refused starts no run, so a signal denial claimed beside it could never have
// been judged, and the pair is refused rather than passing unchecked.
func TestDeniedSignalsRefusesAWebhookRefusalPairing(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{Signals: map[string]*v1.SignalPolicy{"go": {Allow: "true"}}}
	scripts := []SignalScript{{Name: "go"}}

	err := checkDeniedSignalNames(&Expectation{DeniedSignals: []string{"go"}, Refused: new(true)}, scripts, spec)
	assert.ErrorContains(t, err, "cannot be combined with `refused: true`")

	assert.NoError(t, checkDeniedSignalNames(&Expectation{DeniedSignals: []string{"go"}, Refused: new(false)}, scripts, spec))
}
