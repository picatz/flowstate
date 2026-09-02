package plugin

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

func TestStderrSecretScrubberRetainsThenExpiresValues(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	scrubber := newStderrSecretScrubber(func() time.Time { return now })
	release := scrubber.add(secrets.NewSecret(&flowstatev1.SecretRef{Scheme: "test", Name: "token"}, "late-secret"))

	// Time spent in flight does not consume the post-call retention window.
	now = now.Add(24 * time.Hour)
	got, changed := scrubber.scrub("in flight: late-secret")
	assert.True(t, changed)
	assert.Equal(t, "in flight: "+secrets.Redacted, got)
	release()

	now = now.Add(stderrSecretRetention - time.Nanosecond)
	got, changed = scrubber.scrub("after return: late-secret")
	assert.True(t, changed)
	assert.Equal(t, "after return: "+secrets.Redacted, got)

	now = now.Add(time.Nanosecond)
	got, changed = scrubber.scrub("expired: late-secret")
	assert.False(t, changed)
	assert.Equal(t, "expired: late-secret", got)
}

func TestStderrSecretScrubberEvictsOldestAtCountBound(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)
	scrubber := newStderrSecretScrubber(func() time.Time { return now })
	for i := range maxStderrSecrets + 1 {
		scrubber.add(secrets.NewSecret(
			&flowstatev1.SecretRef{Scheme: "test", Name: fmt.Sprintf("token-%d", i)},
			fmt.Sprintf("secret-value-%d", i),
		))
		now = now.Add(time.Nanosecond)
	}

	require.Len(t, scrubber.entries, maxStderrSecrets)
	got, changed := scrubber.scrub("secret-value-0 secret-value-1 secret-value-256")
	assert.True(t, changed)
	assert.Contains(t, got, "secret-value-0", "the oldest value must be evicted")
	assert.NotContains(t, got, "secret-value-1")
	assert.NotContains(t, got, "secret-value-256")
}

func TestStderrRelayScrubsEncodingsAndMarksTheRecord(t *testing.T) {
	t.Parallel()

	const material = `token-"with-json"`
	var logged capturedLogs
	cfg := Config{MaxStderrLinesPerMinute: -1, Logger: newCapturingLogger(t, &logged)}
	scrubber := newStderrSecretScrubber(nil)
	scrubber.add(secrets.NewSecret(&flowstatev1.SecretRef{Scheme: "test", Name: "token"}, material))
	relay, _ := stderrRelayFunc(cfg, cfg.logger(), scrubber)

	relay(strings.Join([]string{
		material,
		base64.StdEncoding.EncodeToString([]byte(material)),
		fmt.Sprintf(`{"token":%q}`, material),
	}, " "), false)

	output := logged.String()
	assert.NotContains(t, output, material)
	assert.NotContains(t, output, base64.StdEncoding.EncodeToString([]byte(material)))
	assert.Contains(t, output, secrets.Redacted)
	assert.Contains(t, output, "scrubbed=true")
}

func TestStderrSecretScrubberDoesNotRescrubItsPlaceholder(t *testing.T) {
	t.Parallel()

	scrubber := newStderrSecretScrubber(nil)
	for range 30 {
		scrubber.add(secrets.NewSecret(&flowstatev1.SecretRef{Scheme: "test", Name: "short"}, "E"))
	}

	got, changed := scrubber.scrub("E")
	assert.True(t, changed)
	assert.Equal(t, secrets.Redacted, got)
}
