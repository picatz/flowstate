package envelope

import "time"

// SetClock gives a codec built from o the clock now, for tests of the data
// key window.
func SetClock(o *Options, now func() time.Time) { o.now = now }

// ProviderTimeout is the deadline c puts on one wrap or unwrap.
func (c *Codec) ProviderTimeout() time.Duration { return c.timeout }

// Labels and the magic, for the independent implementation of the
// construction in conformance_test.go.
const (
	Magic           = magic
	ContentKeyLabel = contentKeyLabel
	CommitmentLabel = commitmentLabel
	AADLabel        = aadLabel
)
