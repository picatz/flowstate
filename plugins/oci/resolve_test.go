package main

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestResolveAnswersTheDigestTheBytesActuallyHaveTo is the task's whole point:
// a tag names whatever it names today, and what comes back is the digest of the
// bytes this call received - computed here, not read out of the registry's own
// Docker-Content-Digest header, which is the same party asserting about itself.
func TestResolveAnswersTheDigestTheBytesActuallyHaveTo(t *testing.T) {
	registry := newFakeRegistry(t)
	manifest := registry.manifests["app:1.0"]
	want := digestOfBytes(manifest.body)

	out, err := resolve(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.reference("app", "1.0")), platform{})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if out.GetDigest() != want {
		t.Errorf("digest = %q, want the hash of the served bytes %q", out.GetDigest(), want)
	}
	if got, want := out.GetReference(), registry.host+"/app@"+want; got != want {
		t.Errorf("reference = %q, want the canonical pinned form %q", got, want)
	}
	if out.GetMediaType() != "application/vnd.oci.image.manifest.v1+json" {
		t.Errorf("media_type = %q", out.GetMediaType())
	}
	if out.GetSize() != int64(len(manifest.body)) {
		t.Errorf("size = %d, want %d", out.GetSize(), len(manifest.body))
	}
	if out.GetPlatformMatched() {
		t.Error("platform_matched is true for a call that asked for no platform")
	}
}

// TestResolveRefusesAManifestThatIsNotTheDigestItWasAskedFor is the negative
// direction of content addressing, and the reason this is a plugin rather than
// an http step: a registry that serves other bytes under a pinned digest is
// caught here, not trusted.
func TestResolveRefusesAManifestThatIsNotTheDigestItWasAskedFor(t *testing.T) {
	registry := newFakeRegistry(t)

	// A digest nothing served hashes to, pointed at bytes that exist.
	wrong := "sha256:" + strings.Repeat("a", 64)
	registry.manifests["app@"+wrong] = fakeContent{
		mediaType: "application/vnd.oci.image.manifest.v1+json",
		body:      []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`),
		corrupt:   true,
	}

	_, err := resolve(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.pinned("app", wrong)), platform{})
	if err == nil {
		t.Fatal("a manifest whose bytes do not hash to the requested digest was accepted")
	}
	if !strings.Contains(err.Error(), "does not match its address") {
		t.Errorf("the refusal is %v, want one naming the mismatch", err)
	}
}

// TestResolveSelectsOnePlatformOutOfAnIndex covers the index case, including
// the attestation entry real indexes carry: an unknown/unknown manifest is not
// an image and must never be returned as one.
func TestResolveSelectsOnePlatformOutOfAnIndex(t *testing.T) {
	registry := newFakeRegistry(t)
	registry.addManifest("app", "multi", "application/vnd.oci.image.index.v1+json", indexWith("linux/amd64", "linux/arm64/v8"))

	out, err := resolve(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.reference("app", "multi")), platform{os: "linux", arch: "arm64"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if !out.GetPlatformMatched() || out.GetPlatform() != "linux/arm64" {
		t.Errorf("platform = %q matched = %v", out.GetPlatform(), out.GetPlatformMatched())
	}
	if want := "sha256:" + strings.Repeat("2", 64); out.GetDigest() != want {
		t.Errorf("digest = %q, want the arm64 child %q", out.GetDigest(), want)
	}
	if out.GetSize() != 101 {
		t.Errorf("size = %d, want the child descriptor's own size", out.GetSize())
	}
}

// TestResolveRefusesAPlatformAnIndexDoesNotHold refuses rather than returning
// the index's own digest with matched=false: a workflow that asked for a
// platform and was handed an index would deploy the wrong bytes while believing
// it had pinned the right ones.
func TestResolveRefusesAPlatformAnIndexDoesNotHold(t *testing.T) {
	registry := newFakeRegistry(t)
	registry.addManifest("app", "multi", "application/vnd.oci.image.index.v1+json", indexWith("linux/amd64"))

	_, err := resolve(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.reference("app", "multi")), platform{os: "windows", arch: "amd64"})
	if err == nil {
		t.Fatal("an unmatched platform resolved instead of refusing")
	}
	if !errIsNotFound(err) {
		t.Errorf("the refusal is %v, want not-found", err)
	}
	// The refusal says what the index does hold, so the fix does not need a
	// second tool.
	if !strings.Contains(err.Error(), "linux/amd64") {
		t.Errorf("the refusal does not name the platforms that are there: %v", err)
	}
}

// TestResolveRefusesAPlatformOnASinglePlatformManifest keeps the same promise
// for the other shape: there is nothing to match against, so nothing is
// silently returned as a match.
func TestResolveRefusesAPlatformOnASinglePlatformManifest(t *testing.T) {
	registry := newFakeRegistry(t)

	_, err := resolve(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.reference("app", "1.0")), platform{os: "linux", arch: "amd64"})
	if err == nil {
		t.Fatal("a platform request against a single manifest was accepted")
	}
	if !sdk.IsInvalidInput(err) {
		t.Errorf("the refusal is %v, want invalid input", err)
	}
}

// TestResolveCompletesTheTokenChallengeOnceAndReusesTheToken covers the
// protocol an http step cannot: the 401, the realm, the exchange, the retry -
// and that the credential is exchanged once rather than per request.
func TestResolveCompletesTheTokenChallengeOnceAndReusesTheToken(t *testing.T) {
	registry := newFakeRegistry(t)
	registry.requireAuth = true
	registry.username = "robot"
	registry.password = "not-a-real-credential"
	registry.addManifest("app", "multi", "application/vnd.oci.image.index.v1+json", indexWith("linux/amd64"))

	client := registry.client(t, credentials{username: "robot", password: "not-a-real-credential"})

	for _, tag := range []string{"1.0", "multi"} {
		if _, err := resolve(t.Context(), client, mustParse(t, registry.reference("app", tag)), platform{}); err != nil {
			t.Fatalf("resolve %s: %v", tag, err)
		}
	}

	if registry.tokenRequests != 1 {
		t.Errorf("the token endpoint was called %d times across two reads on one client, want 1", registry.tokenRequests)
	}
}

// TestResolveRefusesWhenTheCredentialIsWrong: a refused credential is permanent
// however many times it is retried, so it must not be classified as a backend
// that could not be reached.
func TestResolveRefusesWhenTheCredentialIsWrong(t *testing.T) {
	registry := newFakeRegistry(t)
	registry.requireAuth = true
	registry.username = "robot"
	registry.password = "not-a-real-credential"

	_, err := resolve(t.Context(), registry.client(t, credentials{username: "robot", password: "wrong"}), mustParse(t, registry.reference("app", "1.0")), platform{})
	if err == nil {
		t.Fatal("a wrong credential resolved")
	}
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("the refusal is %v, want permission denied rather than a retryable failure", err)
	}
}

// TestResolveReportsAMissingRepositoryAsNotFound keeps a registry's own 404
// distinguishable from every other failure, because a workflow gating on "this
// image exists" needs that difference.
func TestResolveReportsAMissingRepositoryAsNotFound(t *testing.T) {
	registry := newFakeRegistry(t)

	_, err := resolve(t.Context(), registry.client(t, credentials{}), mustParse(t, registry.reference("app", "9.9")), platform{})
	if !errIsNotFound(err) {
		t.Errorf("error = %v, want not-found", err)
	}
}

// mustParse is the parser, for tests whose subject is what happens after it.
func mustParse(t *testing.T, raw string) reference {
	t.Helper()

	ref, err := parseReference(raw)
	if err != nil {
		t.Fatalf("parsing %q: %v", raw, err)
	}
	return ref
}
