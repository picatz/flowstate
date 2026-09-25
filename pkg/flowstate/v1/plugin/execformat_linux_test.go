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
//
// Nor is "it is an ELF for this machine" the shape of the answer, which is what
// #732 and #741 are about. `binfmt_elf` declines a file — `-ENOEXEC`, which
// offers it to the next registered format rather than failing the exec — over
// several header fields past the architecture triple, so an image can be
// host-shaped in every byte this used to read and still arrive at somebody's
// interpreter. The table below is that set, field by field.

// writeImage puts content at a plugin-shaped path and returns it.
func writeImage(t *testing.T, name string, content []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), BinaryPrefix+name)
	if err := os.WriteFile(path, content, 0o755); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// orderFor is the byte order an ELF with this data encoding is written in.
func orderFor(data elf.Data) binary.ByteOrder {
	if data == elf.ELFDATA2MSB {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

// elfHeaderFor builds a complete, well-formed ELF image for a
// class/byte-order/machine triple: the ehdr, followed immediately by a single
// zeroed program header, which is the smallest shape `binfmt_elf` commits to.
//
// It is a whole header rather than the 20-byte prefix this used to build,
// because the check it feeds now reads through `e_phnum` — and a test handing it
// twenty bytes would be asserting that a truncated header is pinnable, which is
// the defect rather than the fixture.
func elfHeaderFor(class elf.Class, data elf.Data, machine elf.Machine) []byte {
	layout := elfLayout[class]
	order := orderFor(data)

	image := make([]byte, layout.headerSize+layout.phdrSize)
	copy(image, elf.ELFMAG)
	image[elf.EI_CLASS] = byte(class)
	image[elf.EI_DATA] = byte(data)
	image[elf.EI_VERSION] = byte(elf.EV_CURRENT)

	order.PutUint16(image[elfTypeOffset:], uint16(elf.ET_EXEC))
	order.PutUint16(image[elfMachineOffset:], uint16(machine))
	order.PutUint32(image[layout.version:], uint32(elf.EV_CURRENT))
	order.PutUint16(image[layout.ehsize:], uint16(layout.headerSize))
	order.PutUint16(image[layout.phentsize:], uint16(layout.phdrSize))
	order.PutUint16(image[layout.phnum:], 1)

	if layout.offsetWidth == 4 {
		order.PutUint32(image[layout.phoff:], uint32(layout.headerSize))
	} else {
		order.PutUint64(image[layout.phoff:], uint64(layout.headerSize))
	}

	return image
}

// hostLayout is what this host's loader accepts and where its class puts the
// fields a test wants to break.
func hostLayout(t *testing.T) (nativeELF, elfHeaderLayout, binary.ByteOrder) {
	t.Helper()

	host, known := hostELF[runtime.GOARCH]
	if !known {
		t.Skipf("no native ELF header is known for GOARCH %s", runtime.GOARCH)
	}
	return host, elfLayout[host.class], orderFor(host.data)
}

// nativeELFHeader is an image this host's own loader would claim.
func nativeELFHeader(t *testing.T) []byte {
	t.Helper()

	host, _, _ := hostLayout(t)
	return elfHeaderFor(host.class, host.data, host.machine)
}

// brokenNativeELF starts from an image this host's loader would claim and
// breaks one field, which is the only way to attribute a refusal to that field.
func brokenNativeELF(t *testing.T, corrupt func(image []byte, layout elfHeaderLayout, order binary.ByteOrder)) []byte {
	t.Helper()

	_, layout, order := hostLayout(t)

	image := nativeELFHeader(t)
	corrupt(image, layout, order)
	return image
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
// directions: an image `binfmt_elf` certainly claims is pinned, and every other
// shape falls back to the path with the weaker guarantee said out loud.
func TestOnlyANativeBinaryIsPinnedToItsDescriptor(t *testing.T) {
	t.Parallel()

	host, layout, _ := hostLayout(t)

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
		"wrong-elf-class":     elfHeaderFor(otherClass(t), host.data, host.machine),
		"wrong-elf-byteorder": elfHeaderFor(host.class, otherData(t), host.machine),

		// #732 and #741: host-shaped in class, byte order and machine, and
		// still declined by `binfmt_elf` — so still somebody's interpreter's
		// problem. Every field is covered in
		// TestNativeELFRefusalNamesTheFieldTheLoaderDeclinesOn; two stand here
		// to show the refusal travels all the way out to [openExecImage].
		"host-shaped-but-not-executable": brokenNativeELF(t, func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
			order.PutUint16(image[elfTypeOffset:], uint16(elf.ET_REL))
		}),
		"host-shaped-but-truncated": nativeELFHeader(t)[:layout.headerSize-1],
	}

	for name, content := range interpreted {
		path := writeImage(t, name, content)

		var logged capturedLogs
		image, err := openExecImage(path, newCapturingLogger(t, &logged))
		if err != nil {
			t.Fatalf("openExecImage(%s): %v", name, err)
		}

		if image.pinned {
			t.Errorf("%s was pinned to its descriptor; the kernel does not certainly execute it directly, "+
				"and an interpreter that claims it is handed a path it can no longer open", name)
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
	native := writeImage(t, "native", nativeELFHeader(t))

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

// TestARealHostBinaryIsPinned is the happy path against a binary the kernel
// actually runs, rather than one this test wrote.
//
// A synthetic header proves what the check reads; only a real one proves the
// check has not become so strict that nothing a toolchain emits gets through —
// which is how an under-approximation fails, and which a table of hand-built
// headers cannot see, because the same misunderstanding writes both the fixture
// and the check. The test binary itself is the sample: this host's own ELF,
// linked by this host's toolchain, and running.
func TestARealHostBinaryIsPinned(t *testing.T) {
	t.Parallel()

	self, err := os.Executable()
	if err != nil {
		t.Fatalf("finding the test binary: %v", err)
	}

	content, err := os.ReadFile(self)
	if err != nil {
		t.Fatalf("reading the test binary: %v", err)
	}

	var logged capturedLogs
	image, err := openExecImage(writeImage(t, "real", content), newCapturingLogger(t, &logged))
	if err != nil {
		t.Fatalf("openExecImage: %v", err)
	}
	t.Cleanup(image.close)

	if !image.pinned {
		t.Fatalf("a real binary built for this host was not pinned to its descriptor, so the digest stopped "+
			"proving what ran for the ordinary case; the refusal was:\n%s", logged.String())
	}
}

// TestNativeELFRefusalNamesTheFieldTheLoaderDeclinesOn is the negative
// direction, field by field, over the pure function.
//
// It is a pure function precisely so this can exist: a `binfmt_misc`
// registration cannot be made from a test — it wants privilege and it is
// host-global — so the only way to cover what happens when the native loader
// declines is to make the decision itself answerable from bytes. Each case
// breaks exactly one field of an image that is otherwise pinnable, and asserts
// both that it is refused and that the refusal names the field, because a
// diagnostic saying only "not pinned" sends an operator to look for the defect
// in their filesystem rather than in their binary.
func TestNativeELFRefusalNamesTheFieldTheLoaderDeclinesOn(t *testing.T) {
	t.Parallel()

	host, _, _ := hostLayout(t)

	for _, tc := range []struct {
		name    string
		corrupt func(image []byte, layout elfHeaderLayout, order binary.ByteOrder)
		names   string
	}{
		{
			name: "e_type is not executable",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint16(image[elfTypeOffset:], uint16(elf.ET_REL))
			},
			names: "type is ET_REL",
		},
		{
			name: "e_type is none",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint16(image[elfTypeOffset:], uint16(elf.ET_NONE))
			},
			names: "type is ET_NONE",
		},
		{
			name: "e_machine is another architecture",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				machine := elf.EM_S390
				if host.machine == machine {
					machine = elf.EM_X86_64
				}
				order.PutUint16(image[elfMachineOffset:], uint16(machine))
			},
			names: "machine is",
		},
		{
			name: "EI_VERSION is not current",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				image[elf.EI_VERSION] = 0
			},
			names: "identification version is",
		},
		{
			name: "e_version is not current",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint32(image[layout.version:], 2)
			},
			names: "object file version is",
		},
		{
			name: "e_ehsize disagrees with the class",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint16(image[layout.ehsize:], uint16(layout.headerSize-1))
			},
			names: "byte header",
		},
		{
			name: "e_phentsize is not this class's program header",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint16(image[layout.phentsize:], uint16(layout.phdrSize+1))
			},
			names: "byte program headers",
		},
		{
			name: "there are no program headers",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint16(image[layout.phnum:], 0)
			},
			names: "no program headers",
		},
		{
			name: "the program header table is larger than the kernel reads",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				order.PutUint16(image[layout.phnum:], 0xffff)
			},
			names: "exceed the 65536 bytes",
		},
		{
			name: "the program header table is at offset zero",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				if layout.offsetWidth == 4 {
					order.PutUint32(image[layout.phoff:], 0)
				} else {
					order.PutUint64(image[layout.phoff:], 0)
				}
			},
			names: "program header table runs from offset 0",
		},
		{
			name: "the program header table is past the end of the file",
			corrupt: func(image []byte, layout elfHeaderLayout, order binary.ByteOrder) {
				if layout.offsetWidth == 4 {
					order.PutUint32(image[layout.phoff:], 0x7fffffff)
				} else {
					order.PutUint64(image[layout.phoff:], 0x7fffffff)
				}
			},
			names: "past the end of a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			image := brokenNativeELF(t, tc.corrupt)

			err := nativeELFRefusal(image, int64(len(image)))
			if err == nil {
				t.Fatalf("%s was accepted as certainly executed by binfmt_elf; the kernel declines it, and "+
					"whatever binfmt_misc registration claims it next is handed a close-on-exec path", tc.name)
			}
			if !strings.Contains(err.Error(), tc.names) {
				t.Errorf("refusal = %q, want it to name the field: %q", err.Error(), tc.names)
			}
		})
	}

	// The prefix the old check read in full, and accepted: twenty bytes
	// carrying the right class, byte order and machine and nothing else. It is
	// the regression #732 and #741 name, so it is asserted on its own.
	t.Run("only the identification prefix", func(t *testing.T) {
		t.Parallel()

		if err := nativeELFRefusal(nativeELFHeader(t)[:elfMachineOffset+2], elfMachineOffset+2); err == nil {
			t.Fatal("a 20-byte image with a host-shaped identification prefix was accepted as certainly " +
				"executed directly; nothing past e_machine was even present to check")
		}
	})

	// And the shapes that must keep working, so the table above cannot pass by
	// refusing everything.
	t.Run("a whole native header", func(t *testing.T) {
		t.Parallel()

		image := nativeELFHeader(t)
		if err := nativeELFRefusal(image, int64(len(image))); err != nil {
			t.Fatalf("a well-formed native image was refused: %v", err)
		}
	})

	t.Run("a native header inside a larger file", func(t *testing.T) {
		t.Parallel()

		image := append(nativeELFHeader(t), make([]byte, 4096)...)
		if err := nativeELFRefusal(image[:binprmBufSize], int64(len(image))); err != nil {
			t.Fatalf("a well-formed native image in a larger file was refused: %v", err)
		}
	})
}

// TestTheInterpreterFallbackNamesTheLimitation checks the reason an operator
// reads, rather than only that some reason was logged. A line saying the digest
// is weaker without saying why sends whoever reads it to the wrong place —
// their filesystem, or their platform — for a property of the image.
func TestTheInterpreterFallbackNamesTheLimitation(t *testing.T) {
	t.Parallel()

	path := writeImage(t, "scripted", []byte("#!/bin/sh\nexit 0\n"))

	var logged capturedLogs
	image, err := openExecImage(path, newCapturingLogger(t, &logged))
	if err != nil {
		t.Fatalf("openExecImage: %v", err)
	}
	t.Cleanup(image.close)

	if !strings.Contains(logged.String(), "interpreter") {
		t.Errorf("the logged reason does not name the interpreter limitation; log was:\n%s", logged.String())
	}
}
