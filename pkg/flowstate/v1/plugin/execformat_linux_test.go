//go:build linux

package plugin

import (
	"debug/elf"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// otherClass and otherData return the ELF class and byte order this host's
// loader does *not* accept, so a header can be built that is wrong in exactly
// one field.
func otherClass(t *testing.T) elf.Class {
	t.Helper()

	if hostELF[runtime.GOARCH].class == elf.ELFCLASS64 {
		return elf.ELFCLASS32
	}
	return elf.ELFCLASS64
}

func otherData(t *testing.T) elf.Data {
	t.Helper()

	if hostELF[runtime.GOARCH].data == elf.ELFDATA2LSB {
		return elf.ELFDATA2MSB
	}
	return elf.ELFDATA2LSB
}

// Which images may be pinned to a descriptor is a question about executable
// *format*, and it is asked as an allowlist: an image is pinned when it is
// certainly executed directly, and takes the documented by-path fallback
// otherwise.
//
// The reason it cannot be a list of known-bad markers is `binfmt_misc`. A
// registration without the open-binary (`O`) flag makes the kernel hand its
// interpreter the *path* rather than an open descriptor, exactly as `#!` does —
// for whatever format that registration claims, which a host can add at any time
// and this process cannot enumerate. `#!` is the case people hit; it is not the
// shape of the problem.

// writeImage puts content at a plugin-shaped path and returns it.
func writeImage(t *testing.T, name string, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), BinaryPrefix+name)
	if err := os.WriteFile(path, content, 0o755); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// elfHeaderFor builds a 20-byte ELF header prefix — everything through
// e_machine, which is all [isDirectlyExecutable] reads.
func elfHeaderFor(class elf.Class, data elf.Data, machine elf.Machine) []byte {
	header := make([]byte, elfIdentSize)
	copy(header, elf.ELFMAG)
	header[elf.EI_CLASS] = byte(class)
	header[elf.EI_DATA] = byte(data)
	header[elf.EI_VERSION] = byte(elf.EV_CURRENT)

	switch data {
	case elf.ELFDATA2MSB:
		binary.BigEndian.PutUint16(header[18:20], uint16(machine))
	default:
		binary.LittleEndian.PutUint16(header[18:20], uint16(machine))
	}

	return header
}

// nativeELFHeader is a header this host's own loader would claim.
func nativeELFHeader(t *testing.T) []byte {
	t.Helper()

	host, known := hostELF[runtime.GOARCH]
	if !known {
		t.Skipf("no native ELF header is known for GOARCH %s", runtime.GOARCH)
	}
	return elfHeaderFor(host.class, host.data, host.machine)
}

// foreignELFHeader is a header for some architecture that is definitely not
// this one — the qemu-user/binfmt_misc shape.
func foreignELFHeader(t *testing.T) []byte {
	t.Helper()

	host := hostELF[runtime.GOARCH]

	// Any entry whose machine differs from this host's will do; s390x is the
	// usual pick and x86-64 stands in when this *is* an s390x.
	foreign := hostELF["s390x"]
	if foreign.machine == host.machine {
		foreign = hostELF["amd64"]
	}

	return elfHeaderFor(foreign.class, foreign.data, foreign.machine)
}

// TestOnlyANativeBinaryIsPinnedToItsDescriptor is the allowlist, in both
// directions: ELF is pinned, and every other shape falls back to the path with
// the weaker guarantee said out loud.
func TestOnlyANativeBinaryIsPinnedToItsDescriptor(t *testing.T) {
	t.Parallel()

	// Not pinnable, and each for the same underlying reason: the kernel would
	// hand an interpreter a path to reopen.
	interpreted := map[string][]byte{
		// The familiar case.
		"shebang": []byte("#!/bin/sh\nexit 0\n"),

		// The case that makes this an allowlist. Nothing here can know which
		// binfmt_misc registrations a host has, so an image that is not
		// certainly direct is treated as interpreted — a Java class file's
		// magic stands in for the whole open-ended set.
		"binfmt-misc": {0xca, 0xfe, 0xba, 0xbe, 0x00, 0x00, 0x00, 0x34},

		// Too short to carry any magic at all.
		"empty":    {},
		"one-byte": {'#'},

		// Shares a prefix with ELF without being it: the check must compare
		// every byte of the magic, not merely the first.
		"almost-elf": {0x7f, 'E', 'L', 'X', 'x'},

		// A real ELF, and still not one this host's loader claims: the
		// foreign-architecture case that qemu-user registers binfmt_misc for.
		// Four magic bytes call this native; the header says otherwise.
		"foreign-arch-elf": foreignELFHeader(t),

		// The same question asked through the other two header fields the
		// loader decides on.
		"wrong-elf-class":     elfHeaderFor(otherClass(t), hostELF[runtime.GOARCH].data, hostELF[runtime.GOARCH].machine),
		"wrong-elf-byteorder": elfHeaderFor(hostELF[runtime.GOARCH].class, otherData(t), hostELF[runtime.GOARCH].machine),
	}

	for name, content := range interpreted {
		path := writeImage(t, name, content)

		var logged strings.Builder
		image, err := openExecImage(path, newCapturingLogger(t, &logged))
		if err != nil {
			t.Fatalf("openExecImage(%s): %v", name, err)
		}

		if image.pinned {
			t.Errorf("%s was pinned to its descriptor; the kernel runs it through an interpreter, "+
				"which is handed a path it can no longer open", name)
		}
		if image.execPath != path {
			t.Errorf("%s: exec path = %q, want the installed path %q", name, image.execPath, path)
		}

		// The fallback is allowed to be weaker and is not allowed to be quiet:
		// the digest then describes what the path held rather than what ran.
		if !strings.Contains(logged.String(), "rather than proving what ran") {
			t.Errorf("%s: nothing was logged about the weaker guarantee; log was:\n%s", name, logged.String())
		}

		image.close()
	}

	// And the one shape that is certainly executed directly.
	native := writeImage(t, "native", append(nativeELFHeader(t), "the rest does not matter here"...))

	image, err := openExecImage(native, testLogger(t))
	if err != nil {
		t.Fatalf("openExecImage(native): %v", err)
	}
	t.Cleanup(image.close)

	if !image.pinned {
		t.Fatalf("a native ELF image was not pinned; exec path = %q", image.execPath)
	}
	if image.execPath == native {
		t.Error("a native ELF image was executed by path rather than by its descriptor")
	}
}

// TestTheInterpreterFallbackNamesTheLimitation checks the reason an operator
// reads, rather than only that some reason was logged. A line saying the digest
// is weaker without saying why sends whoever reads it to the wrong place —
// their filesystem, or their platform — for a property of the image.
func TestTheInterpreterFallbackNamesTheLimitation(t *testing.T) {
	t.Parallel()

	path := writeImage(t, "scripted", []byte("#!/bin/sh\nexit 0\n"))

	var logged strings.Builder
	image, err := openExecImage(path, newCapturingLogger(t, &logged))
	if err != nil {
		t.Fatalf("openExecImage: %v", err)
	}
	t.Cleanup(image.close)

	if !strings.Contains(logged.String(), "interpreter") {
		t.Errorf("the logged reason does not name the interpreter limitation; log was:\n%s", logged.String())
	}
}
