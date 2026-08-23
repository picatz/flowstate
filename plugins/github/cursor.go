package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// This file is this plugin's counterpart to plugins/git/cursor.go, for
// issue #216's own layer 1: a truncated github.pull_request_list,
// github.issue_list, or github.pull_request_files call reports where to
// resume, the same "next_cursor in, cursor out" shape git.log's own cursor
// already gives a workflow.
//
// The mechanism is deliberately not git.log's. git's cursor packs a
// commit-DAG frontier because a commit graph is immutable: the position
// "everything reachable from these hashes, minus what was already
// returned" never moves once written. GitHub's REST listing endpoints are
// page-number pagination over a list that can mutate between two calls -
// an issue opened, a commit pushed to a pull request - so this plugin's
// cursor is a (page, skip-within-that-page) pair plus a fingerprint of the
// filters the walk was running under, not a frontier. See each list task's
// own doc comment (issue_list.go, pull_request_list.go,
// pull_request_files.go) for what guarantee that buys and what it does
// not - narrower than git.log's exactly-once for issue_list and
// pull_request_list (both require a stable sort to hold even that), and
// narrower still for pull_request_files, which GitHub gives this task no
// sort control over at all.
//
// # Why page-plus-skip, not page alone
//
// paginateBounded can stop mid-page: the item or byte budget can bind
// before a fetched page's own entries are exhausted (see its own doc
// comment). A cursor that only named the next page to fetch would either
// re-emit everything already returned from a partially-consumed page (page
// alone, no skip) or silently start the next page instead and lose
// whatever that partial page had left (page+1, guessing). skip records
// exactly how many of that page's entries a resumed call must discard
// before it resumes collecting - see paginateBounded's own use of it.
//
// # Why a fingerprint, not just page+skip
//
// A page number's meaning depends entirely on the query that produced it:
// page 3 of "state: open, sort: created" is a different set of entries
// than page 3 of "state: closed, sort: updated" - offset pagination has no
// invariant across differing filters the way a commit sha does. Replaying
// a cursor against different filters would silently walk the wrong
// sequence rather than fail - exactly the ambiguity CLAUDE.md's own
// "capability is not done until reachable" and cursor-design guidance both
// warn against leaving implicit. fingerprint hashes every filter the walk
// was running under (owner, repo, state, and so on - each list task's own
// doIssueList/doPullRequestList/doPullRequestFiles builds it) so a cursor
// replayed against a different filter set is refused outright, with a
// diagnostic naming the mismatch, rather than producing a page of results
// nobody asked for.
const cursorMagic = "G1"

// cursorRawLen is the exact decoded byte length every cursor this plugin
// ever emits has: 2 bytes of magic, a 4-byte big-endian page number, a
// 4-byte big-endian skip count, and a 32-byte sha-256 fingerprint. Fixed
// rather than variable-length on purpose: an incoming cursor's shape is
// checked by exact length alone before a single field is parsed out of it,
// the cheapest possible refusal of anything a caller did not get from this
// task's own next_cursor output, including anything oversized.
const cursorRawLen = len(cursorMagic) + 4 + 4 + sha256.Size

// maxCursorInputBytes bounds the raw (still base64-encoded) cursor string
// before this task ever base64-decodes it - a cursor this task emits is
// always base64.RawURLEncoding of exactly cursorRawLen bytes (56
// characters), so anything past a small, generous margin over that is
// refused before the decode even runs, the same "bound before the real use
// sees it" reasoning every other attacker-adjacent input in this plugin
// follows (see validate.go).
const maxCursorInputBytes = 128

// fingerprint identifies the filters a list task's walk is running under -
// see filterFingerprint below. A named type, not a bare [32]byte, so every
// signature that passes one around (paginateBounded's callers, cursor.go's
// own functions) reads as "a filter fingerprint," not "some 32 bytes."
type fingerprint [sha256.Size]byte

// pageCursor is a decoded github-plugin list cursor.
type pageCursor struct {
	page        int
	skip        int
	fingerprint fingerprint
}

// encodePageCursor packs page, skip, and fingerprint into the opaque string
// a list task's own next_cursor output carries. Every value this plugin
// ever writes here round-trips through decodePageCursor unchanged - see
// TestCursorRoundTrips.
func encodePageCursor(page, skip int, fp fingerprint) string {
	buf := make([]byte, 0, cursorRawLen)
	buf = append(buf, cursorMagic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(page))
	buf = binary.BigEndian.AppendUint32(buf, uint32(skip))
	buf = append(buf, fp[:]...)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodePageCursor is encodePageCursor's inverse, and the one place that
// decides whether a string is shaped like a value this plugin could ever
// have emitted - checked structurally (bounded length, valid base64,
// exactly cursorRawLen decoded bytes, this task's own magic prefix, a page
// number that fits this worker and a skip smaller than the largest page
// this task requests) before a single byte of it is trusted for anything
// else. It does not check the fingerprint against a caller's
// current filters - that comparison needs the caller's own filters in
// hand, so it is done by whichever list task calls this (see
// requireCursorFingerprint below), the same "decode structurally here,
// compare meaning at the call site" split plugins/git's own decodeCursor
// and doLogWithBounds use.
func decodePageCursor(raw string) (pageCursor, error) {
	if len(raw) > maxCursorInputBytes {
		return pageCursor{}, fmt.Errorf(
			"cursor is %d bytes, over the %d byte limit this task enforces before it is even decoded",
			len(raw), maxCursorInputBytes)
	}
	buf, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return pageCursor{}, fmt.Errorf("cursor is not valid base64: not a value this task ever emitted: %w", err)
	}
	if len(buf) != cursorRawLen {
		return pageCursor{}, fmt.Errorf(
			"cursor decodes to %d bytes, want exactly %d: not a value this task ever emitted",
			len(buf), cursorRawLen)
	}
	if string(buf[:len(cursorMagic)]) != cursorMagic {
		return pageCursor{}, fmt.Errorf("cursor does not carry this task's own magic prefix: not a value this task ever emitted")
	}
	rest := buf[len(cursorMagic):]
	page := binary.BigEndian.Uint32(rest[0:4])
	skip := binary.BigEndian.Uint32(rest[4:8])
	if page < 1 {
		return pageCursor{}, fmt.Errorf("cursor names page %d, which this task never emits", page)
	}
	// A page number this worker's own int cannot hold is refused rather
	// than silently truncated into one it can. Unreachable on a 64-bit
	// worker - page is a uint32, so every value it can carry fits - and
	// load-bearing only on a 32-bit one (GOARCH=386, arm), where a page at
	// or past 1<<31 becomes a negative int. That is not a crash:
	// paginateBounded's own "if page < 1 { page = 1 }" would clamp it. It is
	// the worse thing, a wrong answer wearing a working call's clothes -
	// a walk quietly restarted from page one under a cursor that named a
	// different page. Refusing it is the fail-closed reading, and the
	// diagnostic says which value was impossible.
	if uint64(page) > uint64(math.MaxInt) {
		return pageCursor{}, fmt.Errorf("cursor names page %d, which does not fit this worker's integer size", page)
	}
	// skip is an index into a page this task itself asked for, and no page
	// this task asks for is larger than maxPerPage (perPage is always
	// min(maxPerPage, max_results+1); paginateBounded clamps an
	// over-delivering peer's response to the perPage it requested precisely
	// so this stays true - see its own doc comment). So a skip at or past
	// maxPerPage is not a position this plugin ever emitted, whoever
	// produced it, and it is refused here rather than carried into
	// paginateBounded as a slice index: the fingerprint is an unkeyed hash
	// over filters a caller already knows, so a cursor is forgeable and
	// every field of one is checked structurally before it is trusted, not
	// only the fields a cooperative caller could get wrong by accident.
	if skip >= maxPerPage {
		return pageCursor{}, fmt.Errorf(
			"cursor names within-page skip %d, want less than the maximum page size %d: not a value this task ever emitted",
			skip, maxPerPage)
	}
	var fp fingerprint
	copy(fp[:], rest[8:])
	return pageCursor{page: int(page), skip: int(skip), fingerprint: fp}, nil
}

// filterFingerprint hashes an ordered list of "key=value" filter strings a
// list task built its walk from - order matters (callers always pass the
// same fields in the same order), and each field is hashed length-prefixed:
// an 8-byte big-endian length, then the field's bytes, with the field count
// itself prefixed the same way ahead of all of them.
//
// Length-prefixed rather than delimiter-separated, per CLAUDE.md's own "no
// separator fixes an ambiguous encoding": a nul byte between fields keeps
// "a=1" then "b=" apart from "a=1b=" then "" only for as long as no field
// can itself contain a nul, and nothing in this plugin's validation
// promises that of every value it fingerprints - issue_list's labels are
// bounded by count and per-entry length, not by character class (see
// validateLabels). A length prefix needs no such promise: the encoding of
// a field list is unique whatever bytes the fields hold, so two different
// filter sets cannot hash alike however a caller spells them.
func filterFingerprint(fields ...string) fingerprint {
	h := sha256.New()
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(fields)))
	h.Write(n[:])
	for _, f := range fields {
		binary.BigEndian.PutUint64(n[:], uint64(len(f)))
		h.Write(n[:])
		h.Write([]byte(f))
	}
	var out fingerprint
	copy(out[:], h.Sum(nil))
	return out
}

// requireCursorFingerprint compares a decoded cursor's own fingerprint
// against the one this call's current filters produce, refusing a mismatch
// with a diagnostic that names the problem rather than silently walking
// whatever page the mismatched cursor happens to name against the new
// filters. See this file's own doc comment, "why a fingerprint."
func requireCursorFingerprint(cur pageCursor, current fingerprint) error {
	if cur.fingerprint != current {
		return fmt.Errorf(
			"cursor was issued under different filters than this call is using - a cursor only " +
				"resumes the exact walk (same owner, repo, and every other filter this task takes) " +
				"that produced it; start a fresh call (cursor unset) to change filters")
	}
	return nil
}

// canonicalAPIBase is the one spelling of an API base this plugin uses
// wherever two spellings must compare equal - effectiveAPIBase's own
// comparison against the operator-configured base, and the api_base field
// of every list task's cursor fingerprint.
//
// Two differences mean nothing and are erased here. A trailing slash is the
// variation newClient itself introduces (it appends one back before use, so
// "https://x" and "https://x/" already name the identical API endpoint).
// And the empty string is github.com: an unset base_url reaches
// api.github.com, so it must canonicalize to the same value an explicitly
// spelled "https://api.github.com" does, or one endpoint would fingerprint
// two ways depending on how a caller happened to write it.
//
// Deliberately not a fuller normalization (case-folding the host, resolving
// "..", and so on): base_url is checked as a valid URL by newClient itself
// before a request is ever made, so this only needs to erase the
// differences that mean nothing, not defend against a malformed value -
// that is newClient's job, not the fingerprint's.
func canonicalAPIBase(raw string) string {
	base := strings.TrimSuffix(raw, "/")
	if base == "" {
		return defaultAPIBaseURL
	}
	return base
}

// cursorHasResumePosition reports whether (nextPage, nextSkip) - what a
// list task's own paginateBounded call just returned - is an actual,
// different position from where this call started (startPage, startSkip),
// or whether at least one item was collected. Either is a real place a
// resumed call can continue from.
//
// Why "position changed" has to be checked independently of "collected an
// item": a peer that answers every page with zero items and a non-zero
// NextPage - CLAUDE.md's own List lesson, and paginateBounded's own
// TestPaginateBoundedStopsAgainstAPeerThatPagesForever - drives
// paginateBounded to its request bound having collected nothing at all,
// yet it DID advance from page 1 to some later page over those requests:
// that later page is exactly where a resumed call needs to pick up, and a
// cursor gate that only asked "did this call return anything" would
// discard that position and hand the caller a dead end - `truncated: true`
// with no way to continue, the exact failure #216 exists to close. Checked
// as "the position moved" rather than "requests were spent," so it stays
// correct if paginateBounded's own retry shape ever changes: what matters
// is where a next call would resume, not how many requests it took to get
// there.
//
// The one case genuinely left with nothing to resume from is the
// mirror-image byte-budget wall: a single item, on the very first page at
// the very first index, too large to fit even alone - collected count 0,
// position unchanged from where this call started. There is nothing this
// call reached that a resumed call could start after; see
// PullRequestListOutputs.next_cursor's own doc comment for what a caller
// does in that position (the same "narrow the walk" remedy git.log's own
// NextCursor gives for its own narrower empty cases).
func cursorHasResumePosition(startPage, startSkip, nextPage, nextSkip, collected int) bool {
	return collected > 0 || nextPage != startPage || nextSkip != startSkip
}
