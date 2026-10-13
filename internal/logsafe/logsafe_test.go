package logsafe_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/picatz/flowstate/internal/logsafe"
)

func TestString(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, in, want string }{
		{"plain text is untouched", "/hooks/deploy?x=1 é世", "/hooks/deploy?x=1 é世"},
		{"line feed", "a\nb", `a\nb`},
		{"carriage return", "a\r\nb", `a\r\nb`},
		{"forged entry", "/x\nlevel=ERROR msg=forged", `/x\nlevel=ERROR msg=forged`},
		{"escape character", "a\x1b[2Jb", `a\x1b[2Jb`},
		{"nul and del", "a\x00b\x7fc", `a\x00b\x7fc`},
		{"c1 csi", "a\u009bb", `a\u009bb`},
		{"bidi override", "a\u202eb", `a\u202eb`},
		{"line separator", "a\u2028b", `a\u2028b`},
		{"invalid utf-8", "a\xffb", `a\xffb`},
		{"backslash is left alone", `a\nb`, `a\nb`},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := logsafe.String(tc.in); got != tc.want {
				t.Fatalf("String(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestStringNeverEmitsUnsafe(t *testing.T) {
	t.Parallel()

	var in strings.Builder
	for r := rune(0); r < 0x300; r++ {
		in.WriteRune(r)
	}

	in.WriteString("‮  \xff")

	got := logsafe.String(in.String())
	for _, r := range got {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) || r == '‮' || r == ' ' || r == ' ' {
			t.Fatalf("output still holds %U", r)
		}
	}
}

func TestArgsKeepsOneLine(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}

		return a
	}}))

	log.Warn("refused", logsafe.Args(
		"path", "/a\nlevel=ERROR forged",
		"error", errors.New("bad\r\nbody"),
		"limit", 7,
	)...)

	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Fatalf("log output has %d line breaks, want exactly the terminator: %q", got, buf.String())
	}

	if !strings.Contains(buf.String(), `limit=7`) {
		t.Fatalf("non-string values must pass through: %q", buf.String())
	}
}

func TestValueNilError(t *testing.T) {
	t.Parallel()

	var err error
	if got := logsafe.Value(err); got != nil {
		t.Fatalf("Value(nil error) = %v, want nil", got)
	}
}
