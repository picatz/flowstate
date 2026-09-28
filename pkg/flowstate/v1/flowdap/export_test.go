package flowdap

import "time"

// ShortenWatchRetries makes a failing read's retries fast for a test, and
// returns the function that restores them.
func ShortenWatchRetries(backoff time.Duration) (restore func()) {
	previous := watchRetryBackoff
	watchRetryBackoff = backoff

	return func() { watchRetryBackoff = previous }
}

// BreakpointIDs is how many editor breakpoint numbers s is holding.
func BreakpointIDs(s *Server) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.ids)
}
