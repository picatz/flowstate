package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// referrerIndex renders a referrers response holding the given artifact types.
func referrerIndex(artifactTypes ...string) []byte {
	manifests := make([]map[string]any, 0, len(artifactTypes))
	for i, artifactType := range artifactTypes {
		manifests = append(manifests, map[string]any{
			"mediaType":    "application/vnd.oci.image.manifest.v1+json",
			"artifactType": artifactType,
			"digest":       "sha256:" + strings.Repeat(string(rune('a'+i)), 64),
			"size":         int64(200 + i),
			"annotations": map[string]string{
				"org.opencontainers.image.created": "2026-09-01T00:00:00Z",
			},
		})
	}

	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     "application/vnd.oci.image.index.v1+json",
		"manifests":     manifests,
	})
	if err != nil {
		panic(err)
	}
	return body
}

// TestReferrersReturnsTheAttachmentsAsDescriptors covers the ordinary case: the
// descriptor fields the specification defines, and nothing invented beside
// them.
func TestReferrersReturnsTheAttachmentsAsDescriptors(t *testing.T) {
	registry := newFakeRegistry(t)
	digest := digestOfBytes(registry.manifests["app:1.0"].body)
	registry.addReferrers("app", digest, referrerIndex("application/vnd.in-toto+json", "application/vnd.dev.cosign.simplesigning.v1+json"))

	out, err := listReferrers(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), "", defaultReferrerLimit)
	if err != nil {
		t.Fatalf("listReferrers: %v", err)
	}
	if out.GetCount() != 2 || len(out.GetReferrers()) != 2 {
		t.Fatalf("count = %d with %d referrers, want 2", out.GetCount(), len(out.GetReferrers()))
	}
	if out.GetTruncated() {
		t.Error("truncated is true for a complete single page")
	}

	first := valueMap(t, out.GetReferrers()[0])
	if got := first["artifact_type"].GetStringValue(); got != "application/vnd.in-toto+json" {
		t.Errorf("artifact_type = %q", got)
	}
	if got := first["size"].GetInt64Value(); got != 200 {
		t.Errorf("size = %d", got)
	}
	if first["annotations"].GetMapValue() == nil {
		t.Error("annotations did not survive as a map")
	}
}

// TestReferrersFiltersEvenWhenTheRegistryDoesNot keeps the result meaning one
// thing: the specification lets a registry ignore the artifactType filter, and
// a workflow's condition must not depend on which registry it ran against.
func TestReferrersFiltersEvenWhenTheRegistryDoesNot(t *testing.T) {
	registry := newFakeRegistry(t)
	digest := digestOfBytes(registry.manifests["app:1.0"].body)
	registry.addReferrers("app", digest, referrerIndex("application/vnd.in-toto+json", "application/other"))

	out, err := listReferrers(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), "application/vnd.in-toto+json", defaultReferrerLimit)
	if err != nil {
		t.Fatalf("listReferrers: %v", err)
	}
	if out.GetCount() != 1 {
		t.Fatalf("count = %d, want the one matching artifact type", out.GetCount())
	}
	if got := valueMap(t, out.GetReferrers()[0])["artifact_type"].GetStringValue(); got != "application/vnd.in-toto+json" {
		t.Errorf("artifact_type = %q, want the filtered one", got)
	}
}

// TestReferrersMarksATruncatedPage is what a workflow gating on "nothing is
// attached" depends on: an empty answer and a bounded one must be
// distinguishable.
func TestReferrersMarksATruncatedPage(t *testing.T) {
	registry := newFakeRegistry(t)
	digest := digestOfBytes(registry.manifests["app:1.0"].body)
	registry.addReferrers("app", digest, referrerIndex("a/1", "a/2", "a/3"))

	out, err := listReferrers(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), "", 2)
	if err != nil {
		t.Fatalf("listReferrers: %v", err)
	}
	if out.GetCount() != 2 || !out.GetTruncated() {
		t.Errorf("count = %d truncated = %v, want 2 and true", out.GetCount(), out.GetTruncated())
	}
}

// TestReferrersReportsAnUnsupportedRegistryRatherThanAnEmptySet is the
// fail-closed direction: a 404 means the repository is missing or the registry
// does not implement referrers, and neither is "there are no attestations".
func TestReferrersReportsAnUnsupportedRegistryRatherThanAnEmptySet(t *testing.T) {
	registry := newFakeRegistry(t)
	digest := digestOfBytes(registry.manifests["app:1.0"].body)

	_, err := listReferrers(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", digest)), "", defaultReferrerLimit)
	if !errIsNotFound(err) {
		t.Fatalf("error = %v, want not-found rather than an empty set", err)
	}
	if !strings.Contains(err.Error(), "neither is the same as no attachments") {
		t.Errorf("the refusal does not explain the difference that matters: %v", err)
	}
}

// TestReferrersRefusesATag: the answer is about specific bytes, so the question
// has to be too.
func TestReferrersRefusesATag(t *testing.T) {
	registry := newFakeRegistry(t)

	_, err := parsePinnedReference(registry.reference("app", "1.0"), "oci.referrers")
	if err == nil {
		t.Fatal("a tagged reference was accepted for a question about specific bytes")
	}
	if !sdk.IsInvalidInput(err) {
		t.Errorf("the refusal is %v, want invalid input", err)
	}
	if !strings.Contains(err.Error(), "oci.resolve") {
		t.Errorf("the refusal does not name the task that produces a pinned reference: %v", err)
	}
}

// TestReferrersLimitIsRefusedRatherThanLowered: a silently clamped limit would
// answer "two hundred attachments" for an image with more, indistinguishably
// from one with exactly two hundred.
func TestReferrersLimitIsRefusedRatherThanLowered(t *testing.T) {
	if _, err := boundedLimit(maxReferrerLimit + 1); err == nil {
		t.Error("a limit over the ceiling was accepted")
	}
	if got, err := boundedLimit(0); err != nil || got != defaultReferrerLimit {
		t.Errorf("boundedLimit(0) = %d, %v", got, err)
	}
	if got, err := boundedLimit(10); err != nil || got != 10 {
		t.Errorf("boundedLimit(10) = %d, %v", got, err)
	}
}
