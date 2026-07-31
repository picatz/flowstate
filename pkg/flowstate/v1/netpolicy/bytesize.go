package netpolicy

import (
	"fmt"
	"strconv"
	"strings"
)

// ByteSize is a byte count an operator can write the way they say it.
//
// `max_response_bytes: 1048576` is a number somebody has to decode with a
// calculator to review, and a policy field nobody can read at a glance is a
// policy field that gets approved without being read. So the file takes
// `1MiB` and `10MB` and plain `1048576` alike, and this type is the parser.
//
// Two families of suffix, both honoured, because both are in real use and
// silently treating one as the other is a 7% lie at the megabyte and worse
// above: kB/MB/GB/TB are decimal powers (1000), KiB/MiB/GiB/TiB are binary
// powers (1024). Suffixes are matched case-insensitively — the distinction
// that matters is the `i`, not the capitalisation. A bare number is bytes.
//
// Whole numbers only. `1.5GiB` is refused with the fix in the message
// (`1536MiB`), because fractional sizes invite the float arithmetic whose
// rounding someone eventually has to argue about in an incident review, and
// the smaller unit always says the same thing exactly.
type ByteSize int64

// byteSuffixes maps each accepted suffix, lowercased, to its multiplier.
// Longer suffixes are matched first, since `b` is a suffix of all of them.
var byteSuffixes = []struct {
	suffix     string
	multiplier int64
}{
	{"kib", 1 << 10},
	{"mib", 1 << 20},
	{"gib", 1 << 30},
	{"tib", 1 << 40},
	{"kb", 1_000},
	{"mb", 1_000_000},
	{"gb", 1_000_000_000},
	{"tb", 1_000_000_000_000},
	{"b", 1},
}

// UnmarshalText parses a byte size from the form it is written in.
//
// [encoding.TextUnmarshaler], so every YAML and JSON decoder that honours the
// standard interface reads the same forms — the point is one spelling across
// every file that holds a size, not a parser private to this policy.
func (s *ByteSize) UnmarshalText(text []byte) error {
	parsed, err := ParseByteSize(string(text))
	if err != nil {
		return err
	}

	*s = parsed

	return nil
}

// ParseByteSize reads a byte count such as "1MiB", "10MB", or "1048576".
func ParseByteSize(value string) (ByteSize, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("a byte size is empty; write a count such as 1MiB or 1048576")
	}

	number := trimmed
	multiplier := int64(1)

	lowered := strings.ToLower(trimmed)
	for _, entry := range byteSuffixes {
		if strings.HasSuffix(lowered, entry.suffix) {
			number = strings.TrimSpace(trimmed[:len(trimmed)-len(entry.suffix)])
			multiplier = entry.multiplier

			break
		}
	}

	if number == "" {
		return 0, fmt.Errorf("%q has a unit and no count; write the number the unit multiplies, such as 1MiB", value)
	}

	count, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		// The one mistake worth its own sentence: a fractional count. The fix
		// is always the smaller unit, and the message can do that arithmetic
		// for the person, because they are reading it mid-edit.
		if fraction, ferr := strconv.ParseFloat(number, 64); ferr == nil {
			return 0, fmt.Errorf(
				"%q is fractional; whole numbers only, so write the smaller unit — %s",
				value, wholeUnitFor(fraction, multiplier))
		}

		return 0, fmt.Errorf("%q is not a byte size; write a count such as 1MiB, 10MB, or 1048576", value)
	}

	if count < 0 {
		return 0, fmt.Errorf("%q is negative; a byte size counts bytes", value)
	}

	if multiplier > 1 && count > (1<<62)/multiplier {
		return 0, fmt.Errorf("%q overflows; that is more bytes than an int64 can count", value)
	}

	return ByteSize(count * multiplier), nil
}

// wholeUnitFor suggests the spelling of a fractional size one unit down, where
// it is whole — or in bytes, where it always is.
func wholeUnitFor(fraction float64, multiplier int64) string {
	for _, step := range []struct {
		threshold int64
		unit      string
		size      int64
	}{
		{1 << 20, "KiB", 1 << 10},
		{1 << 30, "MiB", 1 << 20},
		{1 << 40, "GiB", 1 << 30},
		{1_000_000, "kB", 1_000},
		{1_000_000_000, "MB", 1_000_000},
		{1_000_000_000_000, "GB", 1_000_000_000},
	} {
		if multiplier == step.threshold {
			smaller := fraction * float64(multiplier) / float64(step.size)
			if smaller == float64(int64(smaller)) {
				return fmt.Sprintf("%d%s", int64(smaller), step.unit)
			}
		}
	}

	return fmt.Sprintf("%d bytes", int64(fraction*float64(multiplier)))
}
