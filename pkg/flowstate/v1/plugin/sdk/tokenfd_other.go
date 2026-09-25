//go:build !unix

package sdk

import (
	"fmt"
	"os"
	"runtime"
)

// openTokenDescriptor refuses, because this platform has no inherited
// descriptor for the token to arrive on.
//
// Passing extra descriptors through exec is a Unix arrangement — os/exec's
// ExtraFiles is documented as unsupported elsewhere — so no host speaking this
// protocol can deliver a token here in the first place. Refusing says that at
// startup, in one line naming the platform, rather than reading a number out of
// the environment and waiting on whatever it happens to name.
func openTokenDescriptor(fd int) (*os.File, error) {
	return nil, fmt.Errorf("%s cannot inherit a descriptor from the process that launched it", runtime.GOOS)
}
