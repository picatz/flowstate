package flowdap

import "time"

// ShortenWatchRetries makes a failing read's retries fast for a test, and
// returns the function that restores them.
func ShortenWatchRetries(backoff time.Duration) (restore func()) {
	previous := watchRetryBackoff
	watchRetryBackoff = backoff

	return func() { watchRetryBackoff = previous }
}
