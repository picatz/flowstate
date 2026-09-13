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
