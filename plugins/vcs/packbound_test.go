package main

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// -----------------------------------------------------------------------
// Unit-level: packBoundedStorer.SetEncodedObject's own accounting, isolated
// from any real clone or transport.
// -----------------------------------------------------------------------

// newSizedObject returns a plumbing.MemoryObject reporting the given size
// without holding size bytes of real content - packBoundedStorer's own
// accounting reads Size(), the same value go-git's real parser leaves set
// to the object's actual decompressed length once decoding an object
// finishes (see plumbing.MemoryObject.Write, which keeps sz equal to
// len(cont) on every write), so a synthetic object that only sets the size
// exercises the accounting this type does without needing a real packfile
// for a pure unit test of the running-total logic.
func newSizedObject(size int64) *plumbing.MemoryObject {
	o := &plumbing.MemoryObject{}
	o.SetType(plumbing.BlobObject)
	o.SetSize(size)
	return o
}

// TestPackBoundedStorerAllowsObjectsUnderTheBound proves the ordinary case
// first: objects whose sizes sum to no more than max are all accepted.
func TestPackBoundedStorerAllowsObjectsUnderTheBound(t *testing.T) {
	s := newPackBoundedStorer(10)
	for i := 0; i < 3; i++ {
		if _, err := s.SetEncodedObject(newSizedObject(3)); err != nil {
			t.Fatalf("object %d: unexpected error: %v", i, err)
		}
	}
}

// TestPackBoundedStorerBoundIsReached is the house rule's first half: the
// bound must be asserted reached, not merely present. The fourth object
// pushes the running total from 9 to 12, over max=10, and must be refused
// with a diagnostic naming the bound - both the limit and how much had
// already accumulated, so an operator reading this error knows what tripped
// it without instrumenting the process themselves.
func TestPackBoundedStorerBoundIsReached(t *testing.T) {
	const max = 10
	s := newPackBoundedStorer(max)

	for i := 0; i < 3; i++ {
		if _, err := s.SetEncodedObject(newSizedObject(3)); err != nil {
			t.Fatalf("object %d (under the bound): unexpected error: %v", i, err)
		}
	}

	_, err := s.SetEncodedObject(newSizedObject(3))
	if err == nil {
		t.Fatal("a fourth object pushing the running total to 12 (over max=10) was accepted")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("%d byte limit", max)) {
		t.Errorf("error does not name the bound (%d); err: %v", max, err)
	}
	if !strings.Contains(err.Error(), "12 bytes decompressed") {
		t.Errorf("error does not name how much had accumulated when it tripped; err: %v", err)
	}
}

// TestPackBoundedStorerRefusesASingleObjectOverTheBound is the same
// assertion for the degenerate one-object case: SetEncodedObject is what
// go-git's parser calls once an object has already been fully decoded (see
// packbound.go), so even the very first, only object can trip this bound on
// its own - the point at which it does, packbound_test.go's clone-level
// tests below establish, is after that object's own memory was already
// spent, which is the documented residual, not a bug in this accounting.
func TestPackBoundedStorerRefusesASingleObjectOverTheBound(t *testing.T) {
	s := newPackBoundedStorer(10)
	if _, err := s.SetEncodedObject(newSizedObject(11)); err == nil {
		t.Fatal("a single 11-byte object over a 10-byte bound was accepted")
	}
}

// TestPackBoundedStorerOtherMethodsStillWork proves embedding *memory.Storage
// still supplies every other method storage.Storer requires unchanged -
// packBoundedStorer only ever means to touch the write path.
func TestPackBoundedStorerOtherMethodsStillWork(t *testing.T) {
	s := newPackBoundedStorer(1 << 20)

	obj := newSizedObject(4)
	obj.SetType(plumbing.BlobObject)
	w, err := obj.Writer()
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := w.Write([]byte("abcd")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	hash, err := s.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("SetEncodedObject: %v", err)
	}

	got, err := s.EncodedObject(plumbing.BlobObject, hash)
	if err != nil {
		t.Fatalf("EncodedObject (promoted from *memory.Storage): %v", err)
	}
	if got.Size() != 4 {
		t.Fatalf("EncodedObject.Size(): got %d, want 4", got.Size())
	}

	if err := s.SetConfig(config.NewConfig()); err != nil {
		// Proves ConfigStorer's SetConfig is still reachable through the
		// embedded *memory.Storage, unaffected by packBoundedStorer
		// overriding only SetEncodedObject.
		t.Fatalf("SetConfig: unexpected error: %v", err)
	}
	got2, err := s.Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got2 == nil {
		t.Fatal("Config() returned nil after SetConfig")
	}
}

// -----------------------------------------------------------------------
// End-to-end: a real local git repository, served through go-git's real
// file transport (which shells out to the real `git upload-pack` binary -
// see plumbing/transport/file/client.go), cloned through cloneBounded's own
// real code path. Not a hand-rolled fixture standing in for a packfile.
// -----------------------------------------------------------------------

// newLocalRepoWithCompressibleBlobs creates a repository with n commits,
// each adding one new, distinctly-named file of sizeEach bytes of highly
// repetitive content (a short per-file marker followed by a single repeated
// byte). Distinct content per file matters: git is content-addressed, so
// n files of byte-for-byte identical content would collapse into one blob
// object and this test would only ever see one object, not n - defeating
// the point of a many-object cumulative-inflation test. Repetitive content
// matters for the opposite reason: it is what makes each file's compressed
// size in the pack tiny while its real, decompressed size stays sizeEach,
// which is the whole shape of the attack this bound exists for.
func newLocalRepoWithCompressibleBlobs(t *testing.T, n, sizeEach int) (dir string, totalBytes int) {
	t.Helper()

	dir, err := os.MkdirTemp("", "vcs-inflate-test-repo")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	repo, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	sig := &object.Signature{Name: "Test", Email: "test@example.com"}

	for i := 0; i < n; i++ {
		marker := fmt.Sprintf("blob-%d\n", i)
		content := append([]byte(marker), bytes.Repeat([]byte{'A'}, sizeEach-len(marker))...)
		totalBytes += len(content)

		name := fmt.Sprintf("blob%d.bin", i)
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if _, err := wt.Add(name); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if _, err := wt.Commit(fmt.Sprintf("add %s", name), &git.CommitOptions{Author: sig}); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	return dir, totalBytes
}

func fileURL(t *testing.T, dir string) *url.URL {
	t.Helper()
	u, err := url.Parse("file://" + dir)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	return u
}

// heapBytesUsed runs fn after forcing a GC and quiescing memory stats, and
// returns how many bytes of allocation fn itself caused - the same
// TotalAlloc-delta-after-GC technique diff_test.go's own memory-bound tests
// use, and for the same reason: TotalAlloc counts allocation regardless of
// whether the GC has since reclaimed it, which is the actual claim "peak
// allocation stayed bounded" is making - not "the process's resident set
// eventually shrank back down."
func heapBytesUsed(fn func()) uint64 {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	fn()
	runtime.ReadMemStats(&m2)
	return m2.TotalAlloc - m1.TotalAlloc
}

// TestCloneBoundedAllowsANormalCloneUnderTheBound is the house rule's other
// half: the bound must be proven not to fire on an ordinary clone, not just
// proven to fire on a hostile one. A cooperative repository well under the
// cap must clone exactly as it would have before this bound existed.
func TestCloneBoundedAllowsANormalCloneUnderTheBound(t *testing.T) {
	dir, totalBytes := newLocalRepoWithCompressibleBlobs(t, 3, 4096)

	repo, err := cloneBoundedWithInflationCap(context.Background(), cloneOptions{
		url: fileURL(t, dir), depth: 1,
	}, 1<<20) // 1 MiB cap, comfortably above the ~12 KiB this repo actually holds
	if err != nil {
		t.Fatalf("cloneBoundedWithInflationCap: unexpected error: %v", err)
	}
	if repo == nil {
		t.Fatal("cloneBoundedWithInflationCap returned a nil repository with no error")
	}

	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head.Hash().IsZero() {
		t.Fatal("cloned repository has a zero HEAD - the clone did not actually complete")
	}
	_ = totalBytes
}

// TestCloneBoundedRefusesInflationAcrossManyObjectsWithBoundedMemory is
// issue #171's central claim, proven rather than asserted: a hostile remote
// answering with many small-on-the-wire, highly-compressible objects whose
// summed decompressed size crosses maxInflated is refused, naming the
// bound - and the refusal arrives with this process's own allocation held
// to a small multiple of the cap, nowhere near the repository's real,
// uncapped content size. That second half is the one CLAUDE.md's own
// house rule insists on: an error that arrives only after the inflated
// bytes were already held is not a bound, it is a bound-shaped no-op.
//
// 40 files of 1 MiB of real, highly compressible content is what makes the
// comparison honest: a repository too small to begin with would pass
// whether or not the bound worked, telling this test nothing.
func TestCloneBoundedRefusesInflationAcrossManyObjectsWithBoundedMemory(t *testing.T) {
	const (
		numFiles     = 120
		sizeEach     = 1 << 20 // 1 MiB each, highly compressible
		inflationCap = 3 << 20 // 3 MiB - crossed within the first handful of objects
	)

	dir, totalBytes := newLocalRepoWithCompressibleBlobs(t, numFiles, sizeEach)
	if totalBytes < 100<<20 {
		t.Fatalf("this test's own fixture only holds %d bytes - too small to prove the allocation "+
			"claim below meaningfully; the fixture generator changed shape", totalBytes)
	}

	var cloneErr error
	used := heapBytesUsed(func() {
		_, cloneErr = cloneBoundedWithInflationCap(context.Background(), cloneOptions{
			url: fileURL(t, dir), depth: 1,
		}, inflationCap)
	})

	if cloneErr == nil {
		t.Fatal("clone of a repository whose real content is far over the inflation cap was accepted")
	}
	if !strings.Contains(cloneErr.Error(), fmt.Sprintf("%d byte limit", inflationCap)) {
		t.Errorf("error does not name the bound (%d); err: %v", inflationCap, cloneErr)
	}

	// Peak allocation stayed a small multiple of the cap, not anywhere near
	// this repository's real content (over 40 MiB): the refusal fired
	// before most of the objects it never reached were ever decompressed,
	// which is the whole point of checking the running total after every
	// object rather than only once at the end.
	const ceiling = 16 * inflationCap
	if used > ceiling {
		t.Fatalf("clone allocated %d bytes before refusing, want <= %d (%dx the %d byte cap) - "+
			"the refusal arrived only after most of this repository's %d real bytes were already "+
			"held, which is not a bound in the sense CLAUDE.md means", used, ceiling, 16, inflationCap, totalBytes)
	}
	if used >= uint64(totalBytes)/3 {
		t.Fatalf("clone allocated %d bytes, not meaningfully less than this repository's own %d "+
			"real bytes - the bound should keep allocation proportional to the cap, not to the "+
			"repository's real size", used, totalBytes)
	}
}

// TestCloneBoundedDoesNotBoundASingleEnormousObject documents, rather than
// hides, packbound.go's own named residual: a pack whose entire budget goes
// into one pathological object is still refused (packBoundedStorer's own
// running total does trip on it, per
// TestPackBoundedStorerRefusesASingleObjectOverTheBound above), but only
// after that one object has already been decompressed in full - so this
// process's own peak allocation tracks that object's real size, not the
// cap. This is what "the object that crosses the bound has already been
// fully decompressed into memory by the time SetEncodedObject sees it"
// (packbound.go) means concretely: proven here, not left to be discovered
// against a real worker under a real attack. A future change closing this
// gap (an actual lazyObjectWriter-shaped hook, if go-git ever exports one)
// should make this test fail, which is exactly what should happen to a
// test whose entire purpose is pinning down a known, named limitation.
func TestCloneBoundedDoesNotBoundASingleEnormousObject(t *testing.T) {
	const (
		singleObjectSize = 24 << 20 // 24 MiB, one file
		inflationCap     = 1 << 20  // 1 MiB - crossed entirely by this one object
	)

	dir, totalBytes := newLocalRepoWithCompressibleBlobs(t, 1, singleObjectSize)

	var cloneErr error
	used := heapBytesUsed(func() {
		_, cloneErr = cloneBoundedWithInflationCap(context.Background(), cloneOptions{
			url: fileURL(t, dir), depth: 1,
		}, inflationCap)
	})

	if cloneErr == nil {
		t.Fatal("clone of a single object over the inflation cap was accepted")
	}

	// The documented gap, made concrete: allocation tracks the object's
	// real size (tens of MiB), not the 1 MiB cap that was supposedly
	// enforced. If this assertion ever starts failing because `used` has
	// dropped near the cap, packbound.go's own doc comment is describing a
	// gap that no longer exists and should be updated to say so.
	if used < uint64(totalBytes)/2 {
		t.Fatalf("clone allocated only %d bytes before refusing a single %d byte object - "+
			"if a single object no longer costs this much memory before the bound catches it, "+
			"packbound.go's documented residual is stale and should be corrected, not this test",
			used, totalBytes)
	}
}
