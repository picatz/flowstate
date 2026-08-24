//go:build linux

package plugin

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// binfmtMiscRegistry is where the kernel exposes the `binfmt_misc` formats
// registered in this mount namespace, one file per registration.
//
// It is consulted rather than assumed absent because of the order the kernel
// asks in: `binfmt_misc` inserts itself at the *head* of the format list
// (`insert_binfmt`), so every registration is offered an image before
// `binfmt_elf` is. A registration is therefore able to claim a file that the
// native loader would have accepted perfectly well — matching bytes anywhere in
// the first BINPRM_BUF_SIZE, which includes every byte of an ELF header — and no
// amount of checking the header can see that coming. That is the hole #741 names,
// and reading the registry is the only thing that closes it: the question "which
// loader claims this image" has an answer on this host, and it is written down
// here.
const binfmtMiscRegistry = "/proc/sys/fs/binfmt_misc"

// binfmtMiscEntrySize bounds one registration file.
//
// procfs files are small and this one is written by the kernel, but the rule
// this repository keeps is that a reader has a bound rather than that its input
// is trustworthy today. An entry that does not fit is not parsed and therefore
// not ruled out, which refuses the pin — the safe direction.
const binfmtMiscEntrySize = 16 << 10

// binfmtMiscClaim reports how a `binfmt_misc` registration would claim this
// image before `binfmt_elf` is consulted, and "" when none would.
//
// dir is the registry directory, taken as an argument so that the whole of this
// is testable without privilege: registering a format needs root and is
// host-global, so the only honest way to cover the shapes a host can present is
// to read a directory a test wrote. name is the path the image was found at,
// which is what an `extension` registration matches on.
//
// The error return is not "could not tell, carry on". Every unreadable or
// unparseable thing here is a registration this cannot rule out, and the caller
// treats it as a refusal to pin, which costs the digest's stronger guarantee and
// keeps the launch.
//
// One case is deliberately not an error: a registry that is not there. Missing
// or unmounted means no registration can be read *in this namespace*, which is
// the ordinary state of a container. It is worth being exact about what that
// buys, because it is the residual limitation of this fix: binfmt_misc
// registrations made on the host still apply to execs inside a container that
// does not mount the filesystem — that is precisely how multi-architecture
// container images run — so an absent registry is not proof of an empty one.
// What remains uncovered after this is that case, and it is narrower than what
// it replaces: refusing to pin whenever the registry is invisible would drop the
// digest guarantee #711 exists for on most container deployments, in order to
// close a case that also requires a registration whose magic matches a
// well-formed host ELF.
func binfmtMiscClaim(dir string, prefix []byte, name string) (string, error) {
	entries, err := os.ReadDir(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("reading the binfmt_misc registry %s: %w", dir, err)
	}

	// The subsystem's own switch. With it off the kernel skips every
	// registration, so there is nothing here to be claimed by.
	switch status, err := readBinfmtMiscFile(filepath.Join(dir, "status")); {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return "", fmt.Errorf("reading the binfmt_misc status: %w", err)
	case strings.TrimSpace(status) == "disabled":
		return "", nil
	}

	for _, entry := range entries {
		// Everything the kernel puts here that is not a registration.
		if entry.IsDir() || entry.Name() == "register" || entry.Name() == "status" {
			continue
		}

		path := filepath.Join(dir, entry.Name())

		text, err := readBinfmtMiscFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			// A registration removed while this was walking is one that cannot
			// claim the image being launched either.
			continue
		}
		if err != nil {
			return "", fmt.Errorf("reading the binfmt_misc registration %s: %w", path, err)
		}

		registration, err := parseBinfmtMiscEntry(text)
		if err != nil {
			return "", fmt.Errorf("the binfmt_misc registration %s cannot be read, so it cannot be ruled "+
				"out as the format that claims this image: %w", path, err)
		}

		if registration.claims(prefix, name) {
			return fmt.Sprintf("the binfmt_misc registration %q claims it and runs %s, which is handed a "+
				"path rather than an open descriptor because the registration has no O flag",
				entry.Name(), registration.interpreter), nil
		}
	}

	return "", nil
}

// readBinfmtMiscFile reads one registry file under [binfmtMiscEntrySize].
func readBinfmtMiscFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// One byte past the bound, so that a file at exactly the bound is read
	// whole and one over it is visibly over rather than silently truncated into
	// something that parses.
	text, err := io.ReadAll(io.LimitReader(f, binfmtMiscEntrySize+1))
	if err != nil {
		return "", err
	}
	if len(text) > binfmtMiscEntrySize {
		return "", fmt.Errorf("%s is larger than the %d bytes a registration is read within", path, binfmtMiscEntrySize)
	}

	return string(text), nil
}

// binfmtMiscEntry is one `binfmt_misc` registration, as the kernel prints it.
type binfmtMiscEntry struct {
	enabled     bool
	interpreter string
	flags       string
	offset      int
	magic       []byte
	mask        []byte
	extension   string
}

// parseBinfmtMiscEntry reads the format `bm_entry_read` writes:
//
//	enabled
//	interpreter /usr/bin/qemu-aarch64-static
//	flags: OCF
//	offset 0
//	magic 7f454c460201010000000000000000000200b7
//	mask ffffffffffffff00fffffffffffffffffeffff
//
// or, for the other kind of registration, an `extension jar` line in place of
// the offset/magic/mask trio.
//
// A line it does not recognize is ignored and a malformed value is an error,
// which is the split that matters: a field the kernel adds later must not stop
// this from reading the fields it does know, and a magic that is not hex must
// not be read as an empty magic that matches nothing.
func parseBinfmtMiscEntry(text string) (binfmtMiscEntry, error) {
	var entry binfmtMiscEntry

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)

		field, value, _ := strings.Cut(line, " ")
		value = strings.TrimSpace(value)

		switch field {
		case "enabled":
			entry.enabled = true
		case "disabled":
			entry.enabled = false
		case "interpreter":
			entry.interpreter = value
		case "flags:":
			entry.flags = value
		case "extension":
			entry.extension = value
		case "offset":
			offset, err := strconv.Atoi(value)
			if err != nil || offset < 0 {
				return entry, fmt.Errorf("offset %q is not a byte offset", value)
			}
			entry.offset = offset
		case "magic", "mask":
			decoded, err := decodeBinfmtMiscHex(value)
			if err != nil {
				return entry, fmt.Errorf("%s %q: %w", field, value, err)
			}
			if field == "magic" {
				entry.magic = decoded
			} else {
				entry.mask = decoded
			}
		}
	}

	if entry.interpreter == "" && (len(entry.magic) > 0 || entry.extension != "") {
		return entry, errors.New("it names no interpreter")
	}

	return entry, nil
}

// decodeBinfmtMiscHex decodes the lowercase hex the kernel prints a magic and a
// mask as.
//
// encoding/hex is not used because the kernel prints an *unescaped* magic
// verbatim when it was registered that way — this is the same field, and a
// value that is not hex has to be an error rather than a partial decode.
func decodeBinfmtMiscHex(value string) ([]byte, error) {
	if len(value)%2 != 0 {
		return nil, errors.New("it has an odd number of hex digits")
	}

	decoded := make([]byte, 0, len(value)/2)
	for i := 0; i < len(value); i += 2 {
		b, err := strconv.ParseUint(value[i:i+2], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("%q is not a hex byte", value[i:i+2])
		}
		decoded = append(decoded, byte(b))
	}

	return decoded, nil
}

// claims reports whether this registration would take an image with these
// leading bytes, found at this path.
//
// The `O` flag is the whole reason a registration can be ignored: with it the
// kernel opens the image and passes the interpreter an already-open descriptor,
// so nothing has to reopen a path and pinning is harmless. Without it the
// interpreter is handed the name — which, for a pinned image, is a
// close-on-exec `/proc/self/fd/N` that no longer exists by the time it looks.
//
// An extension registration is matched against the path the image was installed
// at rather than against the name exec would be given, and case-insensitively
// where the kernel is exact. Both are deliberate over-matching: the cost is a
// refused pin, and the alternative is reasoning about which name the kernel sees
// in a case where being wrong hands an interpreter a dead path.
func (e binfmtMiscEntry) claims(prefix []byte, name string) bool {
	if !e.enabled || strings.ContainsRune(e.flags, 'O') {
		return false
	}

	if e.extension != "" {
		suffix := "." + e.extension
		return len(name) > len(suffix) && strings.EqualFold(filepath.Ext(name), suffix)
	}

	if len(e.magic) == 0 || e.offset+len(e.magic) > len(prefix) {
		return false
	}

	for i, want := range e.magic {
		got := prefix[e.offset+i]
		if i < len(e.mask) {
			got &= e.mask[i]
		}
		if got != want {
			return false
		}
	}

	return true
}
