package main

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestBlobReturnsTheVerifiedDocument is the ordinary path: the bytes come back,
// they hash to what was asked for, and parse_json decodes the attestation this
// task exists to read.
func TestBlobReturnsTheVerifiedDocument(t *testing.T) {
	registry := newFakeRegistry(t)
	document := []byte(`{"_type":"https://in-toto.io/Statement/v1","predicateType":"https://slsa.dev/provenance/v1"}`)
	digest := registry.addBlob("app", "application/vnd.in-toto+json", document)

	out, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), defaultBlobBytes, true)
	if err != nil {
		t.Fatalf("fetchBlob: %v", err)
	}
	if out.GetDigest() != digest {
		t.Errorf("digest = %q, want %q", out.GetDigest(), digest)
	}
	if out.GetSize() != int64(len(document)) {
		t.Errorf("size = %d, want %d", out.GetSize(), len(document))
	}
	if !strings.Contains(out.GetContent(), "in-toto") {
		t.Errorf("content = %q", out.GetContent())
	}

	entries := out.GetJson().GetMapValue().GetEntries()
	if len(entries) != 2 {
		t.Fatalf("json decoded to %d entries, want the document's two", len(entries))
	}
}

// TestBlobRefusesBytesThatDoNotHashToTheirAddress is the property that makes
// this task worth having: a registry serving different bytes under a digest is
// caught rather than believed, and the failure is permanent because another
// attempt fetches the same wrong bytes.
func TestBlobRefusesBytesThatDoNotHashToTheirAddress(t *testing.T) {
	registry := newFakeRegistry(t)
	claimed := "sha256:" + strings.Repeat("b", 64)
	registry.addCorruptBlob("app", claimed, []byte(`{"tampered":true}`))

	_, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", claimed)), defaultBlobBytes, false)
	if err == nil {
		t.Fatal("bytes that do not hash to their address were returned")
	}
	if !strings.Contains(err.Error(), "does not match its address") {
		t.Errorf("the refusal is %v, want one naming the mismatch", err)
	}
	if sdk.IsUnavailable(err) {
		t.Error("a digest mismatch was classified as retryable; the same bytes come back next time")
	}
}

// TestBlobRefusesADocumentLongerThanTheCallAllowed: a truncated document read
// as a complete one is how a workflow decides on evidence it did not receive.
func TestBlobRefusesADocumentLongerThanTheCallAllowed(t *testing.T) {
	registry := newFakeRegistry(t)
	digest := registry.addBlob("app", "application/json", []byte(`{"padding":"`+strings.Repeat("x", 4096)+`"}`))

	_, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), 128, false)
	if err == nil {
		t.Fatal("an oversized blob was truncated into a result")
	}
	if !sdk.IsInvalidInput(err) {
		t.Errorf("the refusal is %v, want invalid input naming max_bytes", err)
	}
	if !strings.Contains(err.Error(), "max_bytes") {
		t.Errorf("the refusal does not name the input that fixes it: %v", err)
	}
}

// TestBlobRefusesToCallInvalidJsonAnEmptyParse keeps parse_json honest: a
// document that is not JSON is a refusal, not a null a workflow reads as "no
// predicate".
func TestBlobRefusesToCallInvalidJsonAnEmptyParse(t *testing.T) {
	registry := newFakeRegistry(t)
	digest := registry.addBlob("app", "application/json", []byte("not json at all"))

	_, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), defaultBlobBytes, true)
	if err == nil {
		t.Fatal("a blob that is not JSON parsed")
	}
	if !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("error = %v", err)
	}
}

// TestBlobMaxBytesIsRefusedRatherThanLowered, for the reason the referrers
// limit is.
func TestBlobMaxBytesIsRefusedRatherThanLowered(t *testing.T) {
	if _, err := boundedBlobBytes(maxBlobBytes + 1); err == nil {
		t.Error("a max_bytes over the ceiling was accepted")
	}
	if got, err := boundedBlobBytes(0); err != nil || got != defaultBlobBytes {
		t.Errorf("boundedBlobBytes(0) = %d, %v", got, err)
	}
}

// TestParsedJSONKeepsIntegersTheRegistryServed is why parse_json does not
// decode into `any` directly.
//
// encoding/json turns every number into a float64 when the destination is
// `any`, and a float64 holds integers exactly only to 2^53. A workflow
// branching on a build number or a nanosecond epoch out of an attestation would
// be branching on a value different from the bytes this task just verified
// against their digest.
func TestParsedJSONKeepsIntegersTheRegistryServed(t *testing.T) {
	registry := newFakeRegistry(t)
	// 2^53 + 1: the first integer a float64 cannot represent.
	document := []byte(`{"buildNumber":9007199254740993,"small":42,"ratio":0.5}`)
	digest := registry.addBlob("app", "application/vnd.in-toto+json", document)

	out, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), defaultBlobBytes, true)
	if err != nil {
		t.Fatalf("fetchBlob: %v", err)
	}

	entries := out.GetJson().GetMapValue().GetEntries()
	if len(entries) != 3 {
		t.Fatalf("json decoded to %d entries, want 3", len(entries))
	}

	for _, entry := range entries {
		switch entry.GetKey().GetStringValue() {
		case "buildNumber":
			if got := entry.GetValue().GetInt64Value(); got != 9007199254740993 {
				t.Errorf("buildNumber = %d (double %v), want the integer the registry served",
					got, entry.GetValue().GetDoubleValue())
			}
		case "small":
			if got := entry.GetValue().GetInt64Value(); got != 42 {
				t.Errorf("small = %d, want 42", got)
			}
		case "ratio":
			if got := entry.GetValue().GetDoubleValue(); got != 0.5 {
				t.Errorf("ratio = %v, want a float to stay a float", got)
			}
		}
	}
}

// TestParsedJSONAcceptsOrdinaryNumberSpellings is the other half of keeping
// integers exact: only integers are held to it.
//
// `1.0` and `1e3` are exact values written non-canonically, and `0.1` is not
// exactly representable in binary at all — it is also in most real documents.
// A representability check applied to floats would refuse all three, which is
// how a correctness fix becomes an availability defect.
func TestParsedJSONAcceptsOrdinaryNumberSpellings(t *testing.T) {
	registry := newFakeRegistry(t)
	document := []byte(`{"one":1.0,"thousand":1e3,"tenth":0.1,"negative":-17}`)
	digest := registry.addBlob("app", "application/json", document)

	out, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), defaultBlobBytes, true)
	if err != nil {
		t.Fatalf("an ordinary JSON document was refused: %v", err)
	}

	for _, entry := range out.GetJson().GetMapValue().GetEntries() {
		switch entry.GetKey().GetStringValue() {
		case "one":
			if got := entry.GetValue().GetDoubleValue(); got != 1 {
				t.Errorf("one = %v, want 1", got)
			}
		case "thousand":
			if got := entry.GetValue().GetDoubleValue(); got != 1000 {
				t.Errorf("thousand = %v, want 1000", got)
			}
		case "tenth":
			if got := entry.GetValue().GetDoubleValue(); got != 0.1 {
				t.Errorf("tenth = %v, want 0.1", got)
			}
		case "negative":
			if got := entry.GetValue().GetInt64Value(); got != -17 {
				t.Errorf("negative = %d, want -17", got)
			}
		}
	}
}

// TestParsedJSONRefusesAnIntegerItCannotCarry is the direction that must stay
// a refusal: an integer past int64 returned as a float would be a different
// value from the bytes verified against their digest.
func TestParsedJSONRefusesAnIntegerItCannotCarry(t *testing.T) {
	registry := newFakeRegistry(t)
	document := []byte(`{"huge":123456789012345678901234567890}`)
	digest := registry.addBlob("app", "application/json", document)

	_, err := fetchBlob(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), defaultBlobBytes, true)
	if err == nil {
		t.Fatal("an integer this plugin cannot represent was returned rather than refused")
	}
	if !sdk.IsFailed(err) {
		t.Errorf("error is %v, want the permanent failure classification", err)
	}
}
