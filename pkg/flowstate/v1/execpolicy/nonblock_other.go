//go:build !unix

package execpolicy

import "os"

// openRegular opens path for reading. Platforms without O_NONBLOCK semantics
// rely on the caller's fstat; exec is refused on them anyway (see
// [ReasonPlatform]).
func openRegular(path string) (*os.File, error) { return os.Open(path) }
