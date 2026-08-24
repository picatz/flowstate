//go:build !unix

package pathsec

import "io/fs"

// Supported reports whether this platform can decide any of this. Off unix it
// cannot; see the package documentation.
const Supported = false

// The constraint is `!unix` rather than `windows` deliberately. Windows is the
// platform whose absence of POSIX ownership and mode bits motivates the
// carve-out — [io/fs.FileInfo] exposes no uid there, and Perm() reports bits
// synthesized from ACLs that Mode() cannot see, so every check the unix file
// performs would be reading fiction — but naming `windows` leaves plan9,
// wasip1 and js/wasm with no definition of these at all and breaks the build
// on all three. Nothing in CI cross-compiles them, so that break would surface
// first for whoever tried it.

// Checker is the away-from-unix half of the pair. It decides nothing, and says
// so through [ErrUnsupported] rather than answering "safe" — a check that
// cannot run and looks like it passed is worse than one that cannot run.
type Checker struct {
	UID     uint32
	OwnerOf OwnerFunc
}

// New builds a [Checker] that will answer [ErrUnsupported].
func New(uid uint32) *Checker { return &Checker{UID: uid} }

// Check reports [ErrUnsupported].
func (c *Checker) Check(string) error { return ErrUnsupported }

// CheckFrom reports [ErrUnsupported].
func (c *Checker) CheckFrom(string, string) error { return ErrUnsupported }

// CheckDirectory reports [ErrUnsupported].
func (c *Checker) CheckDirectory(string) error { return ErrUnsupported }

// Component reports [ErrUnsupported].
func (c *Checker) Component(string, fs.FileInfo) error { return ErrUnsupported }
