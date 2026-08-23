//go:build linux

package plugin

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `binfmt_misc` is the reason the ELF header cannot be the whole answer, and it
// is also the thing a test cannot create: registering a format wants privilege
// and is host-global, so a test that tried would be editing the machine every
// other test on it is running on.
//
// So the registry is read through a directory argument, and these tests write
// the directory. What is being covered is the decision — which registration
// would claim this image, and whether the kernel would hand its interpreter a
// path — over exactly the bytes the kernel prints.

// writeRegistration puts one registration in a fake registry directory.
func writeRegistration(t *testing.T, dir, name string, lines ...string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("writing the registration %s: %v", name, err)
	}
}

// qemuStyle is a registration shaped like the ones that make this necessary: it
// matches the leading bytes of an image, and it has no O flag, so the kernel
// hands its interpreter a path to reopen.
func qemuStyle(t *testing.T, image []byte, flags string) []string {
	t.Helper()

	magic := image[:8]

	return []string{
		"enabled",
		"interpreter /usr/bin/qemu-something-static",
		"flags: " + flags,
		"offset 0",
		"magic " + hex.EncodeToString(magic),
		"mask " + strings.Repeat("ff", len(magic)),
	}
}

// TestARegisteredInterpreterIsNotHandedAPinnedDescriptor is #741: an image that
// is a perfectly good native ELF, and a registration that claims it first.
//
// `binfmt_misc` inserts itself at the head of the kernel's format list, so it is
// asked before `binfmt_elf` — which means no amount of checking the ELF header
// can establish that the native loader is the one that runs the file. The only
// place that answer exists is the registry, so it is read.
func TestARegisteredInterpreterIsNotHandedAPinnedDescriptor(t *testing.T) {
	t.Parallel()

	image := nativeELFHeader(t)

	// Refused: a registration matching this image with no O flag. Its
	// interpreter would be started with a `/proc/self/fd/N` path, and the
	// descriptor naming it is closed by the time the interpreter looks.
	refused := map[string][]string{
		"no flags at all":                  qemuStyle(t, image, ""),
		"flags that are not O":             qemuStyle(t, image, "PCF"),
		"an O somewhere else in the value": {"enabled", "interpreter /usr/bin/other", "flags: PC", "offset 0", "magic " + hex.EncodeToString(image[:4])},
	}

	for name, lines := range refused {
		dir := t.TempDir()
		writeRegistration(t, dir, "claimant", lines...)

		claim, err := binfmtMiscClaim(dir, kernelExecBuffer(image), "/opt/flowstate/flowstate-plugin-thing")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if claim == "" {
			t.Errorf("%s: the registration was not seen to claim the image, so its descriptor would be "+
				"pinned and the interpreter handed a path it cannot reopen", name)
			continue
		}
		if !strings.Contains(claim, "claimant") {
			t.Errorf("%s: the reason does not name the registration: %q", name, claim)
		}
	}

	// Allowed: the same registration is harmless with the open-binary flag,
	// because then the kernel passes the interpreter an already-open descriptor
	// rather than a name to reopen. Nothing about a pin can hurt it.
	//
	// And a registration that is registered but off claims nothing at all.
	allowed := map[string][]string{
		"the O flag is set":                qemuStyle(t, image, "OC"),
		"the registration is disabled":     append([]string{"disabled"}, qemuStyle(t, image, "")[1:]...),
		"the magic does not match":         {"enabled", "interpreter /usr/bin/other", "flags: ", "offset 0", "magic cafebabe", "mask ffffffff"},
		"the magic is masked to not match": {"enabled", "interpreter /usr/bin/other", "flags: ", "offset 0", "magic 00000000", "mask ffffffff"},
	}

	for name, lines := range allowed {
		dir := t.TempDir()
		writeRegistration(t, dir, "claimant", lines...)

		claim, err := binfmtMiscClaim(dir, kernelExecBuffer(image), "/opt/flowstate/flowstate-plugin-thing")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if claim != "" {
			t.Errorf("%s: the pin was refused anyway, which costs the digest guarantee for nothing: %s", name, claim)
		}
	}
}

// TestARegistrationIsMatchedTheWayTheKernelMatchesIt covers the parts of the
// match that are easy to get subtly wrong, each of which fails silently: an
// offset, a mask with holes in it, an extension registration, and a magic that
// reaches past the end of a short file into the zeroes the kernel's buffer is
// filled with.
func TestARegistrationIsMatchedTheWayTheKernelMatchesIt(t *testing.T) {
	t.Parallel()

	image := nativeELFHeader(t)

	for _, tc := range []struct {
		name   string
		lines  []string
		path   string
		claims bool
	}{
		{
			// The #741 shape said precisely: bytes matched from somewhere
			// after the identification prefix, which the ELF check reads and
			// approves.
			name: "magic at an offset inside the header",
			lines: []string{"enabled", "interpreter /usr/bin/thing", "flags: ", "offset 16",
				"magic " + hex.EncodeToString(image[16:20]), "mask ffffffff"},
			claims: true,
		},
		{
			name: "a mask that ignores the bytes that differ",
			lines: []string{"enabled", "interpreter /usr/bin/thing", "flags: ", "offset 0",
				"magic 7f45000000000000", "mask ffff000000000000"},
			claims: true,
		},
		{
			name: "a magic reaching into the zero fill past the end of the image",
			lines: []string{"enabled", "interpreter /usr/bin/thing", "flags: ", "offset 250",
				"magic 0000000000", "mask ffffffffff"},
			claims: true,
		},
		{
			name:   "an extension registration for this plugin's name",
			lines:  []string{"enabled", "interpreter /usr/bin/java", "flags: ", "extension jar"},
			path:   "/opt/flowstate/flowstate-plugin-thing.jar",
			claims: true,
		},
		{
			name:   "an extension registration for some other extension",
			lines:  []string{"enabled", "interpreter /usr/bin/java", "flags: ", "extension jar"},
			path:   "/opt/flowstate/flowstate-plugin-thing",
			claims: false,
		},
		{
			name: "a magic that runs past the buffer the kernel matches within",
			lines: []string{"enabled", "interpreter /usr/bin/thing", "flags: ", "offset 254",
				"magic 7f454c46", "mask ffffffff"},
			claims: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeRegistration(t, dir, "claimant", tc.lines...)

			path := tc.path
			if path == "" {
				path = "/opt/flowstate/flowstate-plugin-thing"
			}

			claim, err := binfmtMiscClaim(dir, kernelExecBuffer(image), path)
			if err != nil {
				t.Fatalf("binfmtMiscClaim: %v", err)
			}
			if got := claim != ""; got != tc.claims {
				t.Errorf("claimed = %v, want %v (reason: %q)", got, tc.claims, claim)
			}
		})
	}
}

// TestAnUnreadableRegistrationRefusesThePin is the fail-closed direction. A
// registration this cannot parse is one it cannot rule out, and an image whose
// loader is unknown is not one to make a provable-execution claim about — so the
// answer is the by-path fallback, not a pin taken on the strength of not having
// understood the obstacle.
func TestAnUnreadableRegistrationRefusesThePin(t *testing.T) {
	t.Parallel()

	for name, lines := range map[string][]string{
		"a magic that is not hex":            {"enabled", "interpreter /usr/bin/thing", "flags: ", "offset 0", "magic nothexatall"},
		"a magic with an odd length":         {"enabled", "interpreter /usr/bin/thing", "flags: ", "offset 0", "magic 7f454c4"},
		"an offset that is not a number":     {"enabled", "interpreter /usr/bin/thing", "flags: ", "offset later", "magic 7f454c46"},
		"a registration with no interpreter": {"enabled", "flags: ", "offset 0", "magic 7f454c46"},
	} {
		dir := t.TempDir()
		writeRegistration(t, dir, "unreadable", lines...)

		_, err := binfmtMiscClaim(dir, make([]byte, binprmBufSize), "/opt/flowstate/flowstate-plugin-thing")
		if err == nil {
			t.Errorf("%s was read as a registration that claims nothing; an entry that cannot be understood "+
				"has not been ruled out, and reading it as harmless is the unsafe direction", name)
			continue
		}
		if !strings.Contains(err.Error(), "unreadable") {
			t.Errorf("%s: the error does not name the registration file: %v", name, err)
		}
	}

	// A file too large to be a registration is the same answer: it is not
	// parsed, so it is not ruled out.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "enormous"), make([]byte, binfmtMiscEntrySize+1), 0o644); err != nil {
		t.Fatalf("writing the oversized registration: %v", err)
	}
	if _, err := binfmtMiscClaim(dir, make([]byte, binprmBufSize), "plugin"); err == nil {
		t.Error("a registration larger than the read bound was treated as claiming nothing")
	}
}

// TestTheRegistryIsReadTheWayTheKernelPresentsIt covers the directory itself:
// the two files that are not registrations, the subsystem's own switch, and the
// registry not being there at all.
func TestTheRegistryIsReadTheWayTheKernelPresentsIt(t *testing.T) {
	t.Parallel()

	image := nativeELFHeader(t)

	t.Run("register and status are not registrations", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		// `register` is write-only in procfs and reads as nothing here; the
		// point is that neither is parsed as an entry.
		writeRegistration(t, dir, "register")
		writeRegistration(t, dir, "status", "enabled")

		claim, err := binfmtMiscClaim(dir, kernelExecBuffer(image), "plugin")
		if err != nil {
			t.Fatalf("binfmtMiscClaim: %v", err)
		}
		if claim != "" {
			t.Errorf("something in an empty registry claimed the image: %s", claim)
		}
	})

	t.Run("the subsystem is switched off", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeRegistration(t, dir, "status", "disabled")
		writeRegistration(t, dir, "claimant", qemuStyle(t, image, "")...)

		claim, err := binfmtMiscClaim(dir, kernelExecBuffer(image), "plugin")
		if err != nil {
			t.Fatalf("binfmtMiscClaim: %v", err)
		}
		if claim != "" {
			t.Errorf("a registration in a disabled binfmt_misc claimed the image; the kernel skips all of "+
				"them, so this costs the digest guarantee for nothing: %s", claim)
		}
	})

	t.Run("the registry is not mounted", func(t *testing.T) {
		t.Parallel()

		claim, err := binfmtMiscClaim(filepath.Join(t.TempDir(), "not-mounted"), kernelExecBuffer(image), "plugin")
		if err != nil {
			t.Fatalf("an absent registry is the ordinary state of a container and must not fail a launch: %v", err)
		}
		if claim != "" {
			t.Errorf("an absent registry produced a claim: %s", claim)
		}
	})
}

// TestAClaimedImageIsExecutedByPath is the wiring, end to end through the gate:
// the refusal reaches [refuseUnlessExecutedDirectly], and it names both what was
// refused and why.
//
// The pure functions above are where the shapes are covered; this is the join,
// which is the part a package of well-tested halves still gets wrong.
func TestAClaimedImageIsExecutedByPath(t *testing.T) {
	t.Parallel()

	image := nativeELFHeader(t)
	path := writeImage(t, "claimed", image)

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening the image: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	empty := t.TempDir()
	if err := refuseUnlessExecutedDirectly(f, info, empty); err != nil {
		t.Fatalf("a native image with no registration claiming it was refused: %v", err)
	}

	registry := t.TempDir()
	writeRegistration(t, registry, "qemu-something", qemuStyle(t, image, "PC")...)

	err = refuseUnlessExecutedDirectly(f, info, registry)
	if err == nil {
		t.Fatal("a native image claimed by a non-O binfmt_misc registration was pinned to its descriptor; " +
			"the interpreter the kernel starts is handed a close-on-exec path it cannot reopen")
	}
	for _, want := range []string{"qemu-something", "/usr/bin/qemu-something-static", "O flag"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q, so an operator cannot tell what refused the image: %v", want, err)
		}
	}
}
