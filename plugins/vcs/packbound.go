package main

import (
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/memory"
)

// packBoundedStorer wraps an in-memory git object storage with a running
// total over every object go-git's packfile parser materializes while
// parsing one clone's pack stream, refusing once that total crosses max.
//
// # The gap this closes, and the one it does not
//
// installEgressPolicy already bounds the *wire* bytes of a clone: every HTTP
// response go-git's transport reads is capped at maxResponseBytes by
// netpolicy's RoundTripper, which is the "cap belongs under the library,
// where no path it treats specially can miss it" placement CLAUDE.md's own
// connect-go example argues for, applied here to go-git's transport instead
// of the plugin RPC layer. That bound covers the *compressed* pack a remote
// sends. It does not cover what go-git does with those bytes afterward: a
// git packfile stores each object as an independent zlib (DEFLATE) stream,
// so a small number of compressed bytes can legitimately decompress into a
// much larger number of object bytes - highly repetitive content (long runs
// of the same byte, for instance) compresses extremely well and inflates
// back out to its original size the moment it is read. Wire bytes capped at
// 128 MiB can therefore still expand into gigabytes of decompressed object
// data entirely inside this process, entirely beneath the transport bound,
// while never re-crossing anything the RoundTripper watches.
//
// This is the residual issue #171 names. The obvious place to close it
// would be to intercept a per-object write at the point the packfile parser
// actually inflates one object's bytes and stop *that* write once its own
// running total crosses the bound - but go-git does not expose that hook
// publicly. plumbing/format/packfile/parser.go defines exactly such a hook,
// an unexported "lazyObjectWriter" interface a storer.EncodedObjectStorer
// can optionally implement to receive a streaming io.Writer per object as
// it is decoded - but both the interface and the function type its single
// method returns (objectHeaderWriter) are unexported. A type in another
// package cannot spell either name, so it cannot implement the interface no
// matter how its method looks; Go interface satisfaction requires the
// declared types to match, not merely structurally resemble one another.
// That was checked directly against go-git v5.19.2's source, not assumed.
//
// What this plugin can reach is one step later: storer.EncodedObjectStorer
// (the public interface memory.Storage implements) has one write path,
// SetEncodedObject, and go-git's parser calls it for *every* object it
// finishes materializing - both a whole non-delta object and each object a
// delta chain resolves to - before moving on to the next one
// (plumbing/format/packfile/parser.go's indexObjects and resolveObject).
// Overriding it here means every object's decompressed size is checked the
// moment that object finishes, and the object *after* the one that pushes
// the running total over max is never parsed at all: the parser sees this
// method's error and stops.
//
// The honest limit of that: the object that actually crosses the bound has
// already been fully decompressed into memory by the time SetEncodedObject
// sees it - there is no earlier point this package can intercept a single
// object's own write. So this bounds the *sum* across objects, stopping
// runaway growth as soon as it is detectable, but it does not cap any one
// object's own peak size while it is being decoded. A pack built from many
// moderately-sized, highly-compressible objects is refused with memory held
// bounded to roughly max plus one object's worth - see packbound_test.go's
// TestCloneBoundedRefusesInflationAcrossManyObjectsWithBoundedMemory. A pack
// that puts its entire compressed budget into a single pathological object
// is refused too, but only after that one object's inflation - bounded, in
// the worst case, only by maxResponseBytes and the DEFLATE format's own
// single-pass expansion ceiling (on the order of 1000x, an intrinsic
// property of DEFLATE rather than anything this plugin enforces), not by
// max. See TestCloneBoundedDoesNotBoundASingleEnormousObject, which proves
// that gap exists rather than leaving it to be discovered against a real
// worker.
//
// # Why duplicated rather than shared
//
// plugins/vcs and plugins/git are separate Go modules (see their own
// go.mod), the same reason plugins/vcs's boundedPatchWriter (diff.go) and
// plugins/codex's boundedWriter (diff.go) are two copies of the same
// writer rather than one imported type - and plugins/git's own packbound.go
// is this file's copy, kept identical in shape for the same reason.
// plugins/github never clones a repository at all (it speaks the GitHub
// REST API through go-github, not go-git - see its README), so it has no
// packfile to bound and carries no copy of this file.
type packBoundedStorer struct {
	*memory.Storage
	max   int64
	total int64
}

// newPackBoundedStorer returns a fresh in-memory storer whose
// SetEncodedObject refuses once the cumulative decompressed size of every
// object stored through it exceeds max.
func newPackBoundedStorer(max int64) *packBoundedStorer {
	return &packBoundedStorer{Storage: memory.NewStorage(), max: max}
}

// SetEncodedObject shadows memory.Storage's own promoted method (embedding
// *memory.Storage is what supplies every other method storage.Storer
// requires - EncodedObject, IterEncodedObjects, the reference/shallow/config
// storers, and so on - unchanged).
func (s *packBoundedStorer) SetEncodedObject(o plumbing.EncodedObject) (plumbing.Hash, error) {
	s.total += o.Size()
	if s.total > s.max {
		return plumbing.ZeroHash, fmt.Errorf(
			"clone refused: packfile inflation bound reached (%d bytes decompressed, over the %d byte limit "+
				"this plugin enforces on a pack's total object content); the remote may be answering with a "+
				"delta or decompression bomb", s.total, s.max)
	}
	return s.Storage.SetEncodedObject(o)
}
