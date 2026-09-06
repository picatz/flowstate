package textbound

import (
	"testing"
	"unicode/utf8"
)

// invalid is a valid prefix followed by a lone continuation byte, then more
// valid text: the shape a byte cut somewhere upstream leaves behind.
const invalid = "ab\x80cd"

func TestTruncate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		in    string
		limit int
		want  string
	}{
		{name: "empty", in: "", limit: 8, want: ""},
		{name: "empty at zero", in: "", limit: 0, want: ""},
		{name: "within limit", in: "abc", limit: 8, want: "abc"},
		{name: "exact limit", in: "abcdefgh", limit: 8, want: "abcdefgh"},
		{name: "one past limit", in: "abcdefghi", limit: 8, want: "abcdefgh..."},
		{name: "zero limit", in: "abc", limit: 0, want: "..."},
		{name: "negative limit", in: "abc", limit: -1, want: "..."},
		// U+1F600 is four bytes; a limit inside it backs up to its start.
		{name: "4-byte rune straddling the limit", in: "ab\U0001F600cd", limit: 4, want: "ab..."},
		{name: "4-byte rune straddling at its last byte", in: "ab\U0001F600cd", limit: 5, want: "ab..."},
		{name: "4-byte rune ending exactly at the limit", in: "ab\U0001F600cd", limit: 6, want: "ab\U0001F600..."},
		// U+00E9 is two bytes; the cut lands on its continuation byte.
		{name: "2-byte rune straddling the limit", in: "cafés", limit: 4, want: "caf..."},
		{name: "multibyte rune wider than the limit", in: "\U0001F600", limit: 2, want: "..."},
		// Input that is already invalid comes out valid, cut or not.
		{name: "invalid byte within the limit", in: invalid, limit: 8, want: "abcd"},
		{name: "invalid byte past the limit", in: invalid, limit: 4, want: "abc..."},
		{name: "invalid byte at the limit", in: invalid, limit: 2, want: "ab..."},
		{name: "only invalid bytes", in: "\x80\xff", limit: 8, want: ""},
		// A lead byte whose continuation lies past the limit is not a rune the
		// input holds; it goes the way of any other invalid byte.
		{name: "truncated sequence at the limit", in: "ab\xf0\x9f", limit: 3, want: "ab..."},
		{name: "stray continuation bytes before the limit", in: "a\x80\x80\x80bc", limit: 5, want: "ab..."},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := Truncate(test.in, test.limit)
			if got != test.want {
				t.Fatalf("Truncate(%q, %d) = %q, want %q", test.in, test.limit, got, test.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("Truncate(%q, %d) = %q is not valid UTF-8", test.in, test.limit, got)
			}
			if len(test.in) > test.limit && len(got) > max(test.limit, 0)+len("...") {
				t.Fatalf("Truncate(%q, %d) = %q carries %d bytes of input past the limit", test.in, test.limit, got, len(got)-len("..."))
			}
		})
	}
}

func TestCut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		in    string
		limit int
		want  string
	}{
		{name: "empty", in: "", limit: 4, want: ""},
		{name: "within limit", in: "abc", limit: 4, want: "abc"},
		{name: "exact limit", in: "abcd", limit: 4, want: "abcd"},
		{name: "past limit carries no marker", in: "abcde", limit: 4, want: "abcd"},
		{name: "negative limit", in: "abc", limit: -1, want: ""},
		{name: "4-byte rune straddling the limit", in: "ab\U0001F600cd", limit: 5, want: "ab"},
		{name: "invalid byte within the limit", in: invalid, limit: 8, want: "abcd"},
		{name: "invalid byte past the limit", in: invalid, limit: 4, want: "abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := Cut(test.in, test.limit)
			if got != test.want {
				t.Fatalf("Cut(%q, %d) = %q, want %q", test.in, test.limit, got, test.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("Cut(%q, %d) = %q is not valid UTF-8", test.in, test.limit, got)
			}
			if len(got) > max(test.limit, 0) {
				t.Fatalf("Cut(%q, %d) = %q is %d bytes, past the limit", test.in, test.limit, got, len(got))
			}
		})
	}
}
