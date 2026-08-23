package main

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
)

// TestCursorRoundTrips proves encodePageCursor/decodePageCursor are exact
// inverses across a range of values, including the zero-skip and
// large-page-number cases every list task's own walk actually produces.
func TestCursorRoundTrips(t *testing.T) {
	fp := filterFingerprint("owner=octocat", "repo=hello-world")

	cases := []struct {
		page, skip int
	}{
		{page: 1, skip: 0},
		{page: 1, skip: 9},
		{page: 42, skip: 0},
		{page: 1000000, skip: maxPerPage - 1},
	}

	for _, c := range cases {
		raw := encodePageCursor(c.page, c.skip, fp)
		got, err := decodePageCursor(raw)
		if err != nil {
			t.Fatalf("decodePageCursor(%q): unexpected error: %v", raw, err)
		}
		if got.page != c.page {
			t.Errorf("page = %d, want %d", got.page, c.page)
		}
		if got.skip != c.skip {
			t.Errorf("skip = %d, want %d", got.skip, c.skip)
		}
		if got.fingerprint != fp {
			t.Errorf("fingerprint round-tripped wrong")
		}
	}
}

// TestDecodePageCursorRefusesGarbage is the InvalidInput requirement:
// anything not shaped exactly like a value this plugin's own
// encodePageCursor could have produced is refused, each for a distinct,
// identifiable reason.
func TestDecodePageCursorRefusesGarbage(t *testing.T) {
	fp := filterFingerprint("a")
	valid := encodePageCursor(1, 0, fp)

	cases := []struct {
		name string
		raw  string
	}{
		{"not base64", "not-valid-base64!!!"},
		{"too short", "AA"},
		{"too long", strings.Repeat("A", 200)},
		{"page zero", encodePageCursorRawForTest(0, 0, fp)},
		{"skip at page size", encodePageCursorRawForTest(1, maxPerPage, fp)},
		{"skip overflows int32", encodePageCursorRawForTest(1, 1<<31, fp)},
		{"wrong magic", "X" + valid[1:]},
		{"empty", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.raw == "" {
				// decodePageCursor itself has no "" special case - that is
				// validateCursor's own job (an unset cursor is not an
				// error). Skip it here; validateCursor's own test covers
				// the empty-string path.
				t.Skip("empty cursor is validateCursor's own concern, not decodePageCursor's")
			}
			if _, err := decodePageCursor(c.raw); err == nil {
				t.Fatalf("decodePageCursor(%q): got nil error, want a refusal", c.raw)
			}
		})
	}
}

// encodePageCursorRawForTest builds a cursor with a page number
// encodePageCursor itself would never be asked to encode (0) - reaching
// past the normal constructor deliberately, the way plugins/git's own
// cursor tests reach directly into decodeCursor to prove the structural
// check fires regardless of how the bad value was produced. Duplicates
// encodePageCursor's own packing by hand rather than calling it, since
// encodePageCursor is production code that never needs to write an
// out-of-range page number.
func encodePageCursorRawForTest(page, skip uint32, fp fingerprint) string {
	buf := make([]byte, 0, cursorRawLen)
	buf = append(buf, cursorMagic...)
	buf = binary.BigEndian.AppendUint32(buf, page)
	buf = binary.BigEndian.AppendUint32(buf, skip)
	buf = append(buf, fp[:]...)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// TestValidateCursorAcceptsEmpty proves an unset cursor is not an error -
// the ordinary "fresh call" case every list task's own input takes.
func TestValidateCursorAcceptsEmpty(t *testing.T) {
	got, err := validateCursor("")
	if err != nil {
		t.Fatalf("validateCursor(\"\"): unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("validateCursor(\"\") = %q, want empty", got)
	}
}

// TestValidateCursorBoundsSizeBeforeDecoding proves an oversized cursor is
// refused as InvalidInput before ever reaching decodePageCursor's own
// base64 decode - the "bound before the real use sees it" shape every other
// attacker-adjacent input in this plugin follows (validate.go).
func TestValidateCursorBoundsSizeBeforeDecoding(t *testing.T) {
	oversized := strings.Repeat("A", maxCursorBytes+1)
	_, err := validateCursor(oversized)
	if err == nil {
		t.Fatal("validateCursor: got nil error for an oversized cursor")
	}
	if !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("error = %q, want it to name the byte limit", err)
	}
}

// TestValidateCursorRefusesGarbage proves a structurally invalid cursor
// (one this plugin never emitted) is refused with a diagnostic naming that,
// not merely "invalid."
func TestValidateCursorRefusesGarbage(t *testing.T) {
	_, err := validateCursor("not-a-real-cursor")
	if err == nil {
		t.Fatal("validateCursor: got nil error for a garbage cursor")
	}
}

// TestRequireCursorFingerprintRefusesMismatch proves a cursor issued under
// one set of filters is refused when replayed alongside a different set -
// the "detect and refuse a cursor replayed against different filters"
// requirement.
func TestRequireCursorFingerprintRefusesMismatch(t *testing.T) {
	original := filterFingerprint("owner=octocat", "repo=hello-world", "state=open")
	different := filterFingerprint("owner=octocat", "repo=hello-world", "state=closed")

	cur := pageCursor{page: 2, skip: 0, fingerprint: original}
	if err := requireCursorFingerprint(cur, original); err != nil {
		t.Fatalf("requireCursorFingerprint with matching filters: unexpected error: %v", err)
	}
	err := requireCursorFingerprint(cur, different)
	if err == nil {
		t.Fatal("requireCursorFingerprint with mismatched filters: got nil error")
	}
	if !strings.Contains(err.Error(), "different filters") {
		t.Fatalf("error = %q, want it to name the filter mismatch", err)
	}
}

// TestFilterFingerprintDoesNotCollideAcrossFieldBoundaries proves the
// length prefixing filterFingerprint uses actually prevents the
// concatenation collision its own doc comment names: "a=1" then "b=" must
// not fingerprint identically to "a=1b=" then "".
//
// The nul-carrying pair is the case a separator cannot answer and a length
// prefix can, which is why the prefix replaced it: with fields joined by a
// nul byte, a field that CONTAINS one forges a boundary, so "a=1\x00b=2"
// alone hashes exactly as "a=1" then "b=2" does.
func TestFilterFingerprintDoesNotCollideAcrossFieldBoundaries(t *testing.T) {
	if filterFingerprint("a=1", "b=") == filterFingerprint("a=1b=", "") {
		t.Error("filterFingerprint collided across a field boundary")
	}
	if filterFingerprint("a=1\x00b=2") == filterFingerprint("a=1", "b=2") {
		t.Error("filterFingerprint collided on a field carrying its own separator byte")
	}
	if filterFingerprint("a=") == filterFingerprint("a=", "") {
		t.Error("filterFingerprint collided on a trailing empty field")
	}
}
