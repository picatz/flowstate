//go:build linux

package plugin

import (
	"debug/elf"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"
)

// binprmBufSize is how much of an image the kernel itself looks at when it
// decides which format claims the file: BINPRM_BUF_SIZE, the buffer
// `search_binary_handler` hands every registered handler.
//
// It is the right amount to read here for the same reason: every question this
// package asks about an image — the ELF header, and whether a `binfmt_misc`
// registration's magic matches — is a question the kernel answers out of these
// bytes and no others. A registration whose offset plus magic would run past
// this buffer is rejected at registration time, so nothing that matters lives
// further in.
const binprmBufSize = 256

// execPrefix reads the leading [binprmBufSize] bytes of an image.
//
// Read from the descriptor rather than the path, like everything else here, and
// with [os.File.ReadAt] so it does not disturb the offset [execImage.digest]
// rewinds anyway. A file shorter than the buffer is not an error: the kernel
// zero-fills its own, and a short image simply fails more of the checks below.
func execPrefix(f *os.File) ([]byte, error) {
	prefix := make([]byte, binprmBufSize)

	n, err := f.ReadAt(prefix, 0)
	switch {
	case errors.Is(err, io.EOF):
	case err != nil:
		return nil, fmt.Errorf("reading the first bytes of %s: %w", f.Name(), err)
	}

	return prefix[:n], nil
}

// refuseUnlessExecutedDirectly reports why this image must not be pinned to its
// descriptor, and nil when the kernel certainly executes it itself.
//
// It is an allowlist, and the burden of proof runs one way: a refusal costs a
// pin and keeps the launch, so the answer for anything unproven is a refusal
// with a reason an operator can read. The refusal is the diagnostic — see
// [openExecImage], which logs it beside the weaker guarantee it is taking.
func refuseUnlessExecutedDirectly(f *os.File, info fs.FileInfo) error {
	prefix, err := execPrefix(f)
	if err != nil {
		return err
	}

	return nativeELFRefusal(prefix, info.Size())
}

// errImageIsRunThroughAnInterpreter is why an image the kernel does not execute
// directly is executed by path instead, and the digest describes the path rather
// than proving what ran.
//
// An interpreter-backed format is not executed the way a native binary is. The
// kernel recognizes it, execs some *other* program, and hands that program this
// image's path as an argument to reopen for itself — from inside the new program,
// by which time the close-on-exec descriptor is gone. `/proc/self/fd/N` then names
// nothing — or, worse, names whatever the *interpreter* happens to hold on that
// descriptor number, which is a file nobody hashed — and the launch fails before
// the handshake with an error about a path no operator ever wrote down.
//
// `#!` is the familiar case and not the only one. A `binfmt_misc` registration
// without the open-binary (`O`) flag behaves identically for whatever format it
// claims — a foreign-architecture ELF under qemu-user, a `.jar`, a Mono
// executable — because `O` is precisely the flag that makes the kernel pass an
// open descriptor instead of a path. So this is an allowlist rather than a list
// of known-bad markers: only what is certainly executed directly is pinned, and
// everything else takes the documented fallback. Enumerating the ways an image
// can be interpreted means reading the registry, keeping up with it, and being
// wrong by default for anything new; requiring proof of the one case that works
// is wrong in the safe direction, which here means a launch that succeeds with a
// weaker recorded guarantee.
//
// Clearing close-on-exec would make the path survive into the interpreter, and is
// not done. It leaves every plugin process, and everything it spawns, holding a
// readable descriptor on the image, and it still would not pin what runs: the
// bytes that execute an interpreted image are the *interpreter's*, resolved at
// launch, and nothing here hashes those.
//
// Refusing was the other option and is rejected. [Discover] accepts any
// executable regular file and always has, which makes these supported ways to
// ship a plugin; a hardening change that turns a working deployment into a
// start-up failure has traded something real for something marginal. An operator
// who wants the strong guarantee ships a native binary. What is not defensible is
// the third option, which is what this replaces: breaking them silently.
var errImageIsRunThroughAnInterpreter = errors.New(
	"the image is not a native ELF binary, so the kernel runs it through an interpreter that is handed " +
		"the path and must reopen it after the close-on-exec descriptor naming it is gone")

// refuseNonNativeELF wraps the specific reason `binfmt_elf` will not claim an
// image that nevertheless begins like one this host runs.
//
// The reason is carried through to the operator because it is the whole content
// of the diagnostic: "not pinned" says nothing anyone can act on, and "e_phnum
// is 0, and binfmt_elf requires at least one program header" names the image's
// actual defect. See [nativeELFRefusal].
func refuseNonNativeELF(because string, args ...any) error {
	return fmt.Errorf(
		"the image begins like an ELF this host executes but %s, so the native loader refuses it and a "+
			"binfmt_misc registration can claim it instead and be handed a path its interpreter cannot reopen",
		fmt.Sprintf(because, args...))
}

// nativeELFRefusal reports why `binfmt_elf` would not claim this image, and nil
// when it certainly will.
//
// This is the part four magic bytes, and then a class/byte-order/machine triple,
// each got wrong in turn (#711, #719, #732, #741). Matching the identification
// prefix says the file *starts* like a native binary. It does not say the native
// loader accepts it — and on Linux the difference is not academic, because of how
// `search_binary_handler` iterates. A handler that returns `-ENOEXEC` does not
// fail the exec; it declines, and the next registered format is offered the same
// buffer. `binfmt_elf` returns `-ENOEXEC` from exactly the header checks below,
// so an image that fails one of them is not a failed launch — it is a file handed
// on to whatever else claims it, which on a host running qemu-user or a `.jar`
// registration is a `binfmt_misc` interpreter, handed a `/proc/self/fd/N` path
// that is closed by the time it looks.
//
// So the question this answers is narrower and more useful than "is this an ELF":
// *will `binfmt_elf` commit to this image rather than decline it.* Past the
// checks below, `binfmt_elf` has committed — a bad program header or an
// unmappable segment is a hard error that fails the exec loudly, with nothing
// else claiming the file — which is why the list stops where it does rather than
// growing into a userspace reimplementation of a loader.
//
// It is deliberately an *under*-approximation, and the direction is the whole
// safety argument: every image it accepts, `binfmt_elf` accepts, but not every
// image `binfmt_elf` accepts is accepted here. Being wrong in that direction
// costs a pin and keeps a launch — the by-path fallback, with the weaker
// guarantee said out loud. Being wrong in the other direction is the defect this
// closes. That is also why drift with the kernel is tolerable here where a
// faithful copy of its rules would not be: a rule the kernel relaxes leaves this
// merely conservative, and a rule it tightens is one this does not have to know
// about, because anything it newly declines was already refused here or is a
// hard error rather than a hand-off.
//
// Three of the fields are checked more strictly than the kernel does, on
// purpose — `EI_VERSION`,
// `e_version` and `e_ehsize` are fields every real toolchain fills in and
// `binfmt_elf` does not consult — for the same reason: an image disagreeing with
// its own declared shape is not one to make a provable-execution claim about.
//
// size is the file's size, taken from the same descriptor the prefix is read
// from, and is used only to check that the program header table the header
// promises is actually inside the file.
func nativeELFRefusal(prefix []byte, size int64) error {
	if len(prefix) < len(elf.ELFMAG) || string(prefix[:len(elf.ELFMAG)]) != elf.ELFMAG {
		return errImageIsRunThroughAnInterpreter
	}

	host, known := hostELF[runtime.GOARCH]
	if !known {
		// Not a defect in the image: this build simply has no entry saying what
		// its own loader accepts, so it cannot prove anything. See [hostELF].
		return fmt.Errorf("no native ELF header is known for GOARCH %s, so it cannot be shown that "+
			"this host's own loader claims the image rather than an interpreter", runtime.GOARCH)
	}

	layout, ok := elfLayout[host.class]
	if !ok {
		return refuseNonNativeELF("no header layout is known for ELF class %v", host.class)
	}

	if len(prefix) < layout.headerSize {
		return refuseNonNativeELF("it is %d bytes long, shorter than the %d-byte header it would need",
			len(prefix), layout.headerSize)
	}

	// The identification bytes, which is where the kernel starts too: a
	// disagreement on any of these means some *other* loader's binfmt — the
	// 32-bit compat one, a foreign-architecture qemu registration — and not this
	// host's own.
	if class := elf.Class(prefix[elf.EI_CLASS]); class != host.class {
		return refuseNonNativeELF("its class is %v rather than this host's %v", class, host.class)
	}
	if data := elf.Data(prefix[elf.EI_DATA]); data != host.data {
		return refuseNonNativeELF("its byte order is %v rather than this host's %v", data, host.data)
	}
	if version := elf.Version(prefix[elf.EI_VERSION]); version != elf.EV_CURRENT {
		return refuseNonNativeELF("its identification version is %v rather than %v", version, elf.EV_CURRENT)
	}

	order := binary.ByteOrder(binary.LittleEndian)
	if host.data == elf.ELFDATA2MSB {
		order = binary.BigEndian
	}

	u16 := func(at int) uint16 { return order.Uint16(prefix[at : at+2]) }

	// e_machine and e_type are the two fields `binfmt_elf` itself declines on,
	// and therefore the two that hand the image to whatever is registered next.
	if machine := elf.Machine(u16(elfMachineOffset)); machine != host.machine {
		return refuseNonNativeELF("its machine is %v rather than this host's %v", machine, host.machine)
	}
	switch kind := elf.Type(u16(elfTypeOffset)); kind {
	case elf.ET_EXEC, elf.ET_DYN:
	default:
		return refuseNonNativeELF("its type is %v, and binfmt_elf claims only %v and %v",
			kind, elf.ET_EXEC, elf.ET_DYN)
	}

	if version := elf.Version(order.Uint32(prefix[layout.version : layout.version+4])); version != elf.EV_CURRENT {
		return refuseNonNativeELF("its object file version is %v rather than %v", version, elf.EV_CURRENT)
	}
	if declared := u16(layout.ehsize); int(declared) != layout.headerSize {
		return refuseNonNativeELF("it declares a %d-byte header, and a %v ELF header is %d bytes",
			declared, host.class, layout.headerSize)
	}

	// The program header table, which is the other half of what `binfmt_elf`
	// declines on: `load_elf_phdrs` refuses an entry size that is not this
	// class's, and refuses a count of zero or one that would not fit in the
	// 64KiB it is willing to read.
	phentsize := u16(layout.phentsize)
	if int(phentsize) != layout.phdrSize {
		return refuseNonNativeELF("it declares %d-byte program headers, and a %v program header is %d bytes",
			phentsize, host.class, layout.phdrSize)
	}

	phnum := u16(layout.phnum)
	switch {
	case phnum < 1:
		return refuseNonNativeELF("it declares no program headers, and binfmt_elf requires at least one")
	case uint32(phnum)*uint32(phentsize) > elfPhdrTableLimit:
		return refuseNonNativeELF("its %d program headers exceed the %d bytes binfmt_elf reads of the table",
			phnum, elfPhdrTableLimit)
	}

	// And that the table the header promises is inside the file. The kernel
	// fails this one loudly rather than declining, so it is not a hand-off — it
	// is simply not an image to claim a provable execution for.
	phoff := layout.readOffset(order, prefix)
	if end := phoff + uint64(phnum)*uint64(phentsize); phoff == 0 || end > uint64(size) {
		return refuseNonNativeELF("its program header table runs from offset %d to %d, past the end of a "+
			"%d-byte file", phoff, end, size)
	}

	return nil
}

// elfPhdrTableLimit is how much of a program header table `load_elf_phdrs`
// will read: 64KiB, which is where its `e_phnum` bound comes from.
const elfPhdrTableLimit = 65536

// The offsets of the two fields that sit at the same place in both ELF classes,
// because everything before e_version is class-independent.
const (
	elfTypeOffset    = 16
	elfMachineOffset = 18
)

// elfHeaderLayout is where the class-dependent header fields sit.
//
// Written out rather than reached through debug/elf's reader, because that
// package parses an image the way a linker wants it — seeking, allocating,
// following section tables — and the question here is decided by a fixed number
// of bytes at fixed offsets in a buffer already read. A parser that can allocate
// on an attacker's say-so does not belong on the path a launch waits on.
type elfHeaderLayout struct {
	headerSize int // sizeof(Elf_Ehdr)
	phdrSize   int // sizeof(Elf_Phdr)
	version    int // offset of e_version
	phoff      int // offset of e_phoff
	ehsize     int // offset of e_ehsize
	phentsize  int // offset of e_phentsize
	phnum      int // offset of e_phnum

	// offsetWidth is the width of a file offset in this class, so that a 32-bit
	// header's 4-byte e_phoff is not read together with the e_shoff that
	// follows it — which reading eight bytes would do, and which no mask fixes
	// on a big-endian host, where those four bytes are the high half.
	offsetWidth int
}

// readOffset reads e_phoff at this class's width.
func (l elfHeaderLayout) readOffset(order binary.ByteOrder, prefix []byte) uint64 {
	if l.offsetWidth == 4 {
		return uint64(order.Uint32(prefix[l.phoff : l.phoff+4]))
	}
	return order.Uint64(prefix[l.phoff : l.phoff+8])
}

var elfLayout = map[elf.Class]elfHeaderLayout{
	elf.ELFCLASS32: {
		headerSize: 52, phdrSize: 32,
		version: 20, phoff: 28, ehsize: 40, phentsize: 42, phnum: 44,
		offsetWidth: 4,
	},
	elf.ELFCLASS64: {
		headerSize: 64, phdrSize: 56,
		version: 20, phoff: 32, ehsize: 52, phentsize: 54, phnum: 56,
		offsetWidth: 8,
	},
}

// nativeELF is what an ELF header has to say for `binfmt_elf` on this host to be
// the loader that claims it.
type nativeELF struct {
	class   elf.Class
	data    elf.Data
	machine elf.Machine
}

// hostELF maps GOARCH to the header this host's native loader accepts.
//
// Keyed by GOARCH rather than probed, because the question is which loader the
// *kernel* picks for a file, and the answer is fixed by the architecture this
// binary was built for. An entry missing here is not an error: see
// [nativeELFRefusal] on why an unknown GOARCH declines to pin.
var hostELF = map[string]nativeELF{
	"386":      {elf.ELFCLASS32, elf.ELFDATA2LSB, elf.EM_386},
	"amd64":    {elf.ELFCLASS64, elf.ELFDATA2LSB, elf.EM_X86_64},
	"arm":      {elf.ELFCLASS32, elf.ELFDATA2LSB, elf.EM_ARM},
	"arm64":    {elf.ELFCLASS64, elf.ELFDATA2LSB, elf.EM_AARCH64},
	"loong64":  {elf.ELFCLASS64, elf.ELFDATA2LSB, elf.EM_LOONGARCH},
	"mips":     {elf.ELFCLASS32, elf.ELFDATA2MSB, elf.EM_MIPS},
	"mipsle":   {elf.ELFCLASS32, elf.ELFDATA2LSB, elf.EM_MIPS},
	"mips64":   {elf.ELFCLASS64, elf.ELFDATA2MSB, elf.EM_MIPS},
	"mips64le": {elf.ELFCLASS64, elf.ELFDATA2LSB, elf.EM_MIPS},
	"ppc64":    {elf.ELFCLASS64, elf.ELFDATA2MSB, elf.EM_PPC64},
	"ppc64le":  {elf.ELFCLASS64, elf.ELFDATA2LSB, elf.EM_PPC64},
	"riscv64":  {elf.ELFCLASS64, elf.ELFDATA2LSB, elf.EM_RISCV},
	"s390x":    {elf.ELFCLASS64, elf.ELFDATA2MSB, elf.EM_S390},
}
