//go:build unix

package plugin

import (
	"os/signal"
	"syscall"
)

// ignoreSIGTERM sets this process's own disposition for SIGTERM to ignored.
//
// A forked child inherits its parent's signal disposition table atomically at
// fork, before any of its own code (or exec) runs — see the
// "exit-with-stubborn-child" fake plugin mode in helper_test.go for why that
// ordering is what makes the fixture reliable rather than a child racing to
// install its own trap after starting.
func ignoreSIGTERM() {
	signal.Ignore(syscall.SIGTERM)
}
