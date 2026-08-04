package main

import (
	"bytes"
	"context"
	"fmt"
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
// from any real clone or transport. Mirrors plugins/vcs/packbound_test.go's
// own unit tests - see packbound.go for why this file exists as a
// duplicate rather than an import.
// -----------------------------------------------------------------------

func newSizedObject(size int64) *plumbing.MemoryObject {
	o := &plumbing.MemoryObject{}
	o.SetType(plumbing.BlobObject)
	o.SetSize(size)
	return o
}

// TestPackBoundedStorerAllowsObjectsUnderTheBound is the ordinary case: a
// running total that never crosses max is never refused.
func TestPackBoundedStorerAllowsObjectsUnderTheBound(t *testing.T) {
	s := newPackBoundedStorer(10)
	for i := 0; i < 3; i++ {
		if _, err := s.SetEncodedObject(newSizedObject(3)); err != nil {
			t.Fatalf("object %d: unexpected error: %v", i, err)
		}
	}
}

// TestPackBoundedStorerBoundIsReached is the house rule's first half: the
// bound must be asserted reached, and the diagnostic must name it -
// including how much had already accumulated, not just the ceiling.
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

// TestPackBoundedStorerRefusesASingleObjectOverTheBound: even the very
// first, only object can trip this on its own - the clone-level tests below
// establish that the point at which it does is after that object's own
// memory was already spent, packbound.go's documented residual.
func TestPackBoundedStorerRefusesASingleObjectOverTheBound(t *testing.T) {
	s := newPackBoundedStorer(10)
	if _, err := s.SetEncodedObject(newSizedObject(11)); err == nil {
		t.Fatal("a single 11-byte object over a 10-byte bound was accepted")
	}
}

// TestPackBoundedStorerOtherMethodsStillWork proves embedding *memory.Storage
// still supplies every other method storage.Storer requires, unchanged.
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
		t.Fatalf("SetConfig: unexpected error: %v", err)
	}
	if _, err := s.Config(); err != nil {
		t.Fatalf("Config: %v", err)
	}
}

// -----------------------------------------------------------------------
// End-to-end: a real local git repository, served through go-git's real
// file transport (which shells out to the real `git upload-pack` binary),
// cloned through cloneBounded's own real code path.
// -----------------------------------------------------------------------

// newLocalRepoWithCompressibleBlobs mirrors plugins/vcs's own helper of the
// same name: n commits, each adding one new, distinctly-named file of
// sizeEach bytes of highly repetitive content. Distinct content per file
// keeps git from deduplicating them into one blob; repetitive content is
// what keeps each file's compressed, on-the-wire size tiny while its real,
// decompressed size stays sizeEach - the shape of the attack this bound
// exists for.
func newLocalRepoWithCompressibleBlobs(t *testing.T, n, sizeEach int) (dir string, totalBytes int) {
	t.Helper()

	dir = t.TempDir()
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

// heapBytesUsed runs fn after forcing a GC and returns how many bytes of
// allocation fn itself caused - see plugins/vcs/packbound_test.go's own
// heapBytesUsed for the full argument for TotalAlloc-delta-after-GC as the
// honest measure of "peak allocation," not resident-set-after-the-fact.
func heapBytesUsed(fn func()) uint64 {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	fn()
	runtime.ReadMemStats(&m2)
	return m2.TotalAlloc - m1.TotalAlloc
}

// TestCloneBoundedAllowsANormalCloneUnderTheBound is the house rule's other
// half: an ordinary, cooperative repository well under the cap must clone
// exactly as it would have before this bound existed.
func TestCloneBoundedAllowsANormalCloneUnderTheBound(t *testing.T) {
	dir, _ := newLocalRepoWithCompressibleBlobs(t, 3, 4096)

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
}

// TestCloneBoundedRefusesInflationAcrossManyObjectsWithBoundedMemory is
// issue #171's central claim: a hostile remote answering with many
// small-on-the-wire, highly-compressible objects whose summed decompressed
// size crosses maxInflated is refused, naming the bound - and the refusal
// arrives with this process's own allocation held to a small multiple of
// the cap, nowhere near the repository's real, uncapped content size. See
// plugins/vcs's own version of this test for the full argument on why that
// second half is the one CLAUDE.md's house rule actually insists on.
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
// into one pathological object is still refused, but only after that one
// object has already been decompressed in full - so this process's own peak
// allocation tracks that object's real size, not the cap. See
// plugins/vcs's own version of this test for the full argument.
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

	if used < uint64(totalBytes)/2 {
		t.Fatalf("clone allocated only %d bytes before refusing a single %d byte object - "+
			"if a single object no longer costs this much memory before the bound catches it, "+
			"packbound.go's documented residual is stale and should be corrected, not this test",
			used, totalBytes)
	}
}
