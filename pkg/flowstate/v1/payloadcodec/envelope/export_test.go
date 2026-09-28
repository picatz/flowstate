package envelope

import "time"

// SetClock gives a codec built from o the clock now, for tests of the data
// key window.
func SetClock(o *Options, now func() time.Time) { o.now = now }

// Labels and the magic, for the independent implementation of the
// construction in conformance_test.go.
const (
	Magic           = magic
	ContentKeyLabel = contentKeyLabel
	CommitmentLabel = commitmentLabel
	AADLabel        = aadLabel
)
