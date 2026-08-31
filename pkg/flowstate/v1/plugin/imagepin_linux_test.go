//go:build linux

package plugin

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The digest is what a run is pinned to, so the one question worth asking of it
// is whether it describes the bytes the kernel executed. Hashing a path and then
// executing that path cannot answer yes: it is two lookups of a mutable name
// with a window between them, and an atomic rename — the ordinary way software
// on disk replaces itself — is exactly what lands in that window.
//
// These tests force the window open rather than wait for it. The replacement is
// driven from [Config.beforeExec], a seam that runs after the digest is taken and
// before the process is started, so the race happens on every run of the test
// instead of on an unlucky one. A sleep-based version would prove nothing on a
// loaded machine, and worse, would pass on a fast one whether or not the defect
// was fixed.

// TestPinnedImageSurvivesExecDescriptorShuffle reproduces the descriptor
// collision behind #1230 without depending on process-wide launch timing.
//
// os/exec starts its child-side scratch descriptors one above the largest file
// it must inherit. Put that source at N-1 and the executable at N: before the
// fix, os/exec moves its low exec-error pipe onto N and execve finds a pipe at
// /proc/self/fd/N, returning EACCES. prepareForExec must move the same open file
// description above the complete shuffle range, after which the launch works.
// The setup runs in a subprocess because assigning exact descriptor numbers is
// deliberately process-global.
func TestPinnedImageSurvivesExecDescriptorShuffle(t *testing.T) {
	const helperEnv = "FLOWSTATE_TEST_EXEC_SHUFFLE"
	if os.Getenv(helperEnv) != "" {
		runExecShuffleHelper(t)
		return
	}

	self, err := os.Executable()
	if err != nil {
		t.Fatalf("finding the test binary: %v", err)
	}
	cmd := exec.Command(self, "-test.run=^TestPinnedImageSurvivesExecDescriptorShuffle$")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("descriptor-shuffle helper: %v\n%s", err, output)
	}
}

func runExecShuffleHelper(t *testing.T) {
	t.Helper()

	image, err := openExecImage("/bin/true", testLogger(t))
	if err != nil {
		t.Fatalf("opening executable image: %v", err)
	}
	defer image.close()
	if !image.pinned {
		t.Skip("/bin/true is not descriptor-pinnable on this host")
	}

	const imageFD = 1000
	if err := syscall.Dup3(int(image.file.Fd()), imageFD, syscall.O_CLOEXEC); err != nil {
		t.Fatalf("placing the image on descriptor %d: %v", imageFD, err)
	}
	name := image.file.Name()
	image.file.Close()
	image.file = os.NewFile(imageFD, name)
	image.execPath = "/proc/self/fd/1000"

	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("opening %s: %v", os.DevNull, err)
	}
	defer null.Close()
	const childFileFD = imageFD - 1
	if err := syscall.Dup3(int(null.Fd()), childFileFD, syscall.O_CLOEXEC); err != nil {
		t.Fatalf("placing the child file on descriptor %d: %v", childFileFD, err)
	}
	childFile := os.NewFile(childFileFD, os.DevNull)
	defer childFile.Close()
	files := []*os.File{childFile, childFile, childFile, childFile}

	run := func() error {
		cmd := exec.Command(image.execPath)
		cmd.Stdin = childFile
		cmd.Stdout = childFile
		cmd.Stderr = childFile
		cmd.ExtraFiles = []*os.File{childFile}
		return cmd.Run()
	}

	if err := run(); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("unprepared launch error = %v, want EACCES proving descriptor %d was overwritten", err, imageFD)
	}
	if err := image.prepareForExec(files); err != nil {
		t.Fatalf("preparing image: %v", err)
	}
	if image.file.Fd() <= imageFD {
		t.Fatalf("prepared image descriptor = %d, want it above the shuffle rooted at %d", image.file.Fd(), imageFD)
	}
	if err := run(); err != nil {
		t.Fatalf("prepared launch: %v", err)
	}
}

// TestTheDigestIsOfTheImageThatRanWhenTheBinaryIsSwappedAtExec is the race
// itself.
//
// A fake plugin that reports the digest of its own running inode is the oracle:
// /proc/self/exe names what is executing rather than what is at a path, so the
// plugin's answer is the truth about which bytes served, and the host's recorded
// digest is a claim about the same thing. The binary is replaced between the two
// steps of the launch, and the two have to still agree.
//
// Executed by path, they do not: the host records the binary it hashed and the
// kernel runs the binary that replaced it, which is the recorded provenance
// describing a process that never existed.
func TestTheDigestIsOfTheImageThatRanWhenTheBinaryIsSwappedAtExec(t *testing.T) {
	t.Parallel()

	dir, path := copiedPlugin(t, "self-digest")

	original, err := flowstatev1.ContentDigestOf(mustOpen(t, path))
	if err != nil {
		t.Fatalf("hashing the plugin binary: %v", err)
	}

	cfg := testConfig(t, dir)

	// Errors rather than fatals, and counted: this runs on whichever goroutine
	// is launching the plugin, where a t.Fatalf would end the wrong goroutine.
	var swaps atomic.Int64
	var swapErr atomic.Pointer[error]
	cfg.beforeExec = func(string) {
		swaps.Add(1)
		if err := replaceBinary(path); err != nil {
			swapErr.Store(&err)
		}
	}

	host := openHost(t, cfg)

	if err := swapErr.Load(); err != nil {
		t.Fatalf("replacing the plugin binary mid-launch: %v", *err)
	}
	if swaps.Load() == 0 {
		t.Fatal("the launch never reached the seam, so no window was forced open and this test proves nothing")
	}

	replacement, err := flowstatev1.ContentDigestOf(mustOpen(t, path))
	if err != nil {
		t.Fatalf("hashing the replacement: %v", err)
	}
	if replacement == original {
		t.Fatal("the replacement has the same digest, so this test proves nothing")
	}

	p, ok := host.Lookup("self-digest")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	recorded := p.DistributionDigest()
	ran := p.Manifest().GetDescription()

	if ran != recorded {
		t.Errorf("the plugin process is running %s and the host recorded %s; "+
			"a run pinned to the recorded digest would be pinned to bytes that never served it", ran, recorded)
	}
	if recorded != original {
		t.Errorf("recorded digest = %s, want the binary that was hashed and executed, %s", recorded, original)
	}
	if ran == replacement {
		t.Errorf("the replacement ran: the binary swapped in between the digest and the exec is the one serving")
	}
}

// TestExecImagePointsAtTheDescriptorRatherThanThePath is the primitive under
// that test, checked directly: once the image is open, everything taken from it
// is of the same inode however the path is rebound afterwards.
func TestExecImagePointsAtTheDescriptorRatherThanThePath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, BinaryPrefix+"pinned")

	// A header this host's own ELF loader would claim, because only a natively
	// executable image is pinned (see [nativeELFRefusal]) and a script or
	// a foreign-architecture image here would be asserting the opposite of
	// what this file is about. Nothing executes these bytes — the pin is the
	// whole subject — so the header is all that has to be real.
	if err := os.WriteFile(path, append(nativeELFHeader(t), "the rest does not matter here\n"...), 0o755); err != nil {
		t.Fatalf("writing the binary: %v", err)
	}

	image, err := openExecImage(path, testLogger(t))
	if err != nil {
		t.Fatalf("openExecImage: %v", err)
	}
	t.Cleanup(image.close)

	if !image.pinned {
		t.Fatalf("image is not pinned to its descriptor on linux; exec path = %q", image.execPath)
	}
	if image.execPath == path {
		t.Fatalf("exec path = %q, want the descriptor's own name rather than the path", image.execPath)
	}

	before, err := image.digest()
	if err != nil {
		t.Fatalf("digesting the image: %v", err)
	}

	opened, err := os.Stat(image.execPath)
	if err != nil {
		t.Fatalf("resolving %s: %v", image.execPath, err)
	}

	if err := replaceBinary(path); err != nil {
		t.Fatalf("replacing the binary: %v", err)
	}

	// The name now means something else, and the handle does not.
	after, err := image.digest()
	if err != nil {
		t.Fatalf("digesting the image after the swap: %v", err)
	}
	if after != before {
		t.Errorf("digest = %s after the path was rebound, want the opened image's %s", after, before)
	}

	swapped, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if os.SameFile(opened, swapped) {
		t.Fatal("the replacement is the same inode, so this test proves nothing")
	}

	still, err := os.Stat(image.execPath)
	if err != nil {
		t.Fatalf("resolving %s after the swap: %v", image.execPath, err)
	}
	if !os.SameFile(still, opened) {
		t.Errorf("%s resolves to a different file after the path was rebound; "+
			"exec through it would run bytes that were never hashed", image.execPath)
	}
}

// TestExecImageRefusesWhatIsNotARegularFile covers the check that has to happen
// on the descriptor: hashing a fifo is a read with no end, and this runs on the
// path a worker's startup is waiting on.
func TestExecImageRefusesWhatIsNotARegularFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	_, err := openExecImage(dir, testLogger(t))
	if err == nil {
		t.Fatal("openExecImage accepted a directory")
	}
	if !strings.Contains(err.Error(), "not a regular file") {
		t.Errorf("error = %q, want it to say what is wrong", err.Error())
	}
}
