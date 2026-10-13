package logsafe_test

import (
	"errors"
	"fmt"
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

type nilErr struct{ msg string }

func (e *nilErr) Error() string { return e.msg }

func TestError(t *testing.T) {
	t.Parallel()

	var typedNil *nilErr

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"message", errors.New("bad\r\nbody"), `bad\r\nbody`},
		{"wrapped", fmt.Errorf("outer: %w", errors.New("in\x1bner")), `outer: in\x1bner`},
		{"nil", nil, "<nil>"},
		{"typed nil", typedNil, "<nil>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := logsafe.Error(tc.err); got != tc.want {
				t.Fatalf("Error(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
