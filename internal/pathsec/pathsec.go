// Package pathsec answers one question for every surface in this repository
// that opens something an attacker must not be able to substitute: can another
// identity replace any part of the path this process is about to resolve?
//
// It exists because that question was answered twice. `cmd/flow` asked it of
// the ACME cache directory, which holds an account key and issued
// certificates' private keys, and walked the path component by component to do
// it. `pkg/flowstate/v1/plugin` asked it of the plugin search path, which is a
// list of programs the worker executes with its own credentials and network
// reach, and looked at exactly two paths — the directory and the binary —
// never at their ancestors. A `/opt` owned by an untrusted uid lets that uid
// rename `/opt/plugins` and put its own directory there, whatever the
// permissions on the directory that used to be at that path (#972).
//
// The walk is the part worth having once rather than twice: it took two
// corrections to fit real deployments (#735 for a relative working directory,
// #736 for macOS's `/var -> private/var`), and a thinner second copy beside it
// starts those rounds over.
//
// The prose stays at the call sites. A refusal is returned as a [*Refusal]
// naming the component and why it was refused, and each caller wraps it in the
// sentence its own operator needs — the flag they set, or the configuration
// field that turns the check off.
//
// # Platforms
//
// Ownership and mode bits are POSIX concepts. On a platform that does not
// expose them — Windows above all, where [os.FileInfo] carries no uid and
// Perm() reports bits synthesized from ACLs — every check here would be
// reading fiction, so [Checker.Check] answers [ErrUnsupported] rather than a
// verdict. Callers say so out loud; the one thing worse than a check that
// cannot run is a check that cannot run and looks like it passed.
package pathsec

import (
	"errors"
	"fmt"
	"io/fs"
)

// ErrUnsupported reports that this platform exposes no POSIX ownership or mode
// bits, so nothing here decided anything. It is not a refusal: a caller that
// treats it as one refuses every deployment on a supported platform.
var ErrUnsupported = errors.New("pathsec: path ownership is not represented on this platform")

// MaxSymlinkHops bounds how many symbolic links one path may be resolved
// through, which is what bounds the walk at all: every other step consumes a
// component of a fixed list, and only a link expansion puts new components
// back. A cycle (`a -> b`, `b -> a`) would otherwise loop forever, and a chain
// of links each naming a longer path would grow the pending list without
// bound. 40 is the figure Linux and the BSDs use before answering ELOOP, so a
// path this refuses for hops is a path the kernel would refuse to open anyway.
const MaxSymlinkHops = 40

// Kind says which refusal a [Refusal] is, so a caller can phrase its own
// sentence about the same finding rather than matching on message text.
type Kind int

const (
	// KindOwner: a component is owned by an identity that is neither this
	// process nor root.
	KindOwner Kind = iota + 1

	// KindWritable: a component's mode lets a user who is not its owner
	// write it.
	KindWritable

	// KindUnresolvedSymlink: a component the walk reached other than by
	// resolving it is a symbolic link, which means an assumption this walk
	// relies on does not hold.
	KindUnresolvedSymlink

	// KindSymlinkOwner: a symbolic link component is owned by another
	// identity, who can repoint it after this check.
	KindSymlinkOwner

	// KindSymlinkLoop: resolution passed through more than [MaxSymlinkHops]
	// links.
	KindSymlinkLoop

	// KindUndecidableOwner: ownership could not be read at all.
	KindUndecidableOwner

	// KindIO: a component could not be inspected.
	KindIO
)

// Refusal is why a path is not one this process may resolve safely.
//
// It carries the component rather than only a message because the component is
// what an operator fixes, and because callers phrase the surrounding sentence
// themselves.
type Refusal struct {
	// Kind is which refusal this is.
	Kind Kind

	// Component is the path of the offending component, as the walk reached
	// it — which for a path resolved through a symbolic link is not
	// necessarily a prefix of the path that was checked.
	Component string

	// Owner is the uid owning Component, for [KindOwner] and
	// [KindSymlinkOwner].
	Owner uint32

	// WorkingDir is the physical working directory a *relative* path was
	// resolved from, and empty for an absolute one. Without it an operator
	// reads a refusal naming a component that is not in the value they
	// configured, with nothing connecting the two.
	WorkingDir string

	// Err is the underlying failure for [KindIO], and nil otherwise.
	Err error
}

func (r *Refusal) Error() string {
	msg := r.reason()
	if r.WorkingDir != "" {
		return fmt.Sprintf("the path is relative, so the kernel resolves it from this "+
			"process's working directory %s: %s", r.WorkingDir, msg)
	}

	return msg
}

func (r *Refusal) reason() string {
	switch r.Kind {
	case KindOwner:
		return fmt.Sprintf("path component %s is owned by another identity (uid %d) and so can "+
			"be renamed or replaced regardless of its mode", r.Component, r.Owner)
	case KindWritable:
		return fmt.Sprintf("path component %s is writable by another identity and can be swapped",
			r.Component)
	case KindUnresolvedSymlink:
		return fmt.Sprintf("path component %s is a symbolic link this walk did not resolve, so "+
			"this path cannot be claimed to have been checked", r.Component)
	case KindSymlinkOwner:
		return fmt.Sprintf("path component %s is a symbolic link owned by another identity "+
			"(uid %d), which that identity can repoint after this check", r.Component, r.Owner)
	case KindSymlinkLoop:
		return fmt.Sprintf("the path resolves through more than %d symbolic links at %s; the "+
			"kernel would refuse to open it too (ELOOP)", MaxSymlinkHops, r.Component)
	case KindUndecidableOwner:
		return fmt.Sprintf("the ownership of path component %s could not be determined", r.Component)
	case KindIO:
		return fmt.Sprintf("path component %s could not be checked: %v", r.Component, r.Err)
	default:
		return fmt.Sprintf("path component %s was refused", r.Component)
	}
}

// Unwrap exposes the I/O failure behind a [KindIO] refusal, so a caller can ask
// [errors.Is] about [fs.ErrNotExist] rather than reading the message.
func (r *Refusal) Unwrap() error { return r.Err }

// OwnerFunc reports the uid owning a path, and whether that could be decided.
//
// It is the one part of the decision that is injected, and it exists so tests
// can pose a layout an unprivileged process cannot create: establishing that a
// *root-owned* component is accepted needs a root-owned component, and nothing
// unprivileged can chown one. Modes, link-ness and the walk itself are read
// from a real filesystem.
type OwnerFunc func(path string, info fs.FileInfo) (uint32, bool)
