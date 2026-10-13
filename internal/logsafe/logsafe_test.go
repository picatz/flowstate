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

	in.WriteString("\u202e\u2028\u2029\xff")

	got := logsafe.String(in.String())
	for _, r := range got {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) || r == '\u202e' || r == '\u2028' || r == '\u2029' {
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

type nilErr struct{ msg string }

func (e *nilErr) Error() string { return e.msg }

func TestValueTypedNilError(t *testing.T) {
	t.Parallel()

	var err *nilErr

	if got := logsafe.Value(err); got != "<nil>" {
		t.Fatalf("Value(typed nil error) = %v, want <nil>", got)
	}
}

func TestArgsWalksLikeSlog(t *testing.T) {
	t.Parallel()

	got := logsafe.Args(slog.Int("n", 1), "path", "evil\x1b[2J", slog.String("s", "a\nb"), "k")

	want := []any{slog.Int("n", 1), "path", `evil\x1b[2J`, slog.String("s", `a\nb`), "k"}
	if len(got) != len(want) {
		t.Fatalf("Args = %v, want %v", got, want)
	}

	for i := range want {
		if a, ok := want[i].(slog.Attr); ok {
			if !a.Equal(got[i].(slog.Attr)) {
				t.Fatalf("Args[%d] = %v, want %v", i, got[i], a)
			}

			continue
		}

		if got[i] != want[i] {
			t.Fatalf("Args[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestArgsRewritesGroupsAnyAndBadKeys(t *testing.T) {
	t.Parallel()

	var nilPtr *nilErr

	got := logsafe.Args(
		slog.Group("g", slog.String("s", "a\nb"), slog.Any("e", errors.New("x\ny"))),
		slog.Any("n", nilPtr),
		errors.New("bad\nkey"),
	)

	g := got[0].(slog.Attr).Value.Group()
	if g[0].Value.String() != `a\nb` || g[1].Value.String() != `x\ny` {
		t.Fatalf("group members not rewritten: %v", g)
	}

	if v := got[1].(slog.Attr).Value.String(); v != "<nil>" {
		t.Fatalf("typed-nil error in an Attr = %q, want <nil>", v)
	}

	if got[2] != `bad\nkey` {
		t.Fatalf("bad key = %v, want rewritten", got[2])
	}
}

func TestArgsRewritesErrorsBeforeAnyHandler(t *testing.T) {
	t.Parallel()

	got := logsafe.Args("error", errors.New("bad\r\nbody"), "n", 7)

	if got[1] != `bad\r\nbody` {
		t.Fatalf("error value = %#v, want the escaped text, not the error", got[1])
	}

	if got[3] != 7 {
		t.Fatalf("non-text value = %#v, want it unchanged", got[3])
	}
}
