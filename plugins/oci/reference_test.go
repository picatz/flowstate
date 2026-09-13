package main

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestReferenceParsesTheFormsAWorkflowWrites covers what an author types, and
// what each part becomes: the registry that will be dialed, the repository, and
// which of tag or digest decides.
func TestReferenceParsesTheFormsAWorkflowWrites(t *testing.T) {
	t.Parallel()

	pinned := "sha256:" + strings.Repeat("a", 64)

	for _, test := range []struct {
		raw        string
		registry   string
		endpoint   string
		repository string
		tag        string
		digest     string
		canonical  string
	}{
		{
			raw: "ghcr.io/owner/image:1.2.3", registry: "ghcr.io", endpoint: "ghcr.io",
			repository: "owner/image", tag: "1.2.3", canonical: "ghcr.io/owner/image:1.2.3",
		},
		{
			raw: "ghcr.io/owner/image@" + pinned, registry: "ghcr.io", endpoint: "ghcr.io",
			repository: "owner/image", digest: pinned, canonical: "ghcr.io/owner/image@" + pinned,
		},
		{
			// Both: the digest decides and the tag stays readable, which is what
			// a pinned reference written for humans looks like everywhere else.
			raw: "ghcr.io/owner/image:1.2.3@" + pinned, registry: "ghcr.io", endpoint: "ghcr.io",
			repository: "owner/image", tag: "1.2.3", digest: pinned, canonical: "ghcr.io/owner/image@" + pinned,
		},
		{
			// The one host rewrite, written down rather than hidden in a dialer:
			// Hub is spelled docker.io and served from registry-1.docker.io, and
			// an operator's egress policy has to name what is dialed.
			raw: "docker.io/library/alpine:3.20", registry: "docker.io", endpoint: "registry-1.docker.io",
			repository: "library/alpine", tag: "3.20", canonical: "docker.io/library/alpine:3.20",
		},
		{
			raw: "registry.example.com:5000/team/app:main", registry: "registry.example.com:5000",
			endpoint: "registry.example.com:5000", repository: "team/app", tag: "main",
			canonical: "registry.example.com:5000/team/app:main",
		},
		{
			raw: "localhost:5000/app:dev", registry: "localhost:5000", endpoint: "localhost:5000",
			repository: "app", tag: "dev", canonical: "localhost:5000/app:dev",
		},
	} {
		ref, err := parseReference(test.raw)
		if err != nil {
			t.Errorf("parsing %q: %v", test.raw, err)
			continue
		}
		if ref.Registry != test.registry || ref.Endpoint != test.endpoint {
			t.Errorf("%q: registry = %q endpoint = %q, want %q and %q", test.raw, ref.Registry, ref.Endpoint, test.registry, test.endpoint)
		}
		if ref.Repository != test.repository || ref.Tag != test.tag || ref.Digest != test.digest {
			t.Errorf("%q: repository = %q tag = %q digest = %q", test.raw, ref.Repository, ref.Tag, ref.Digest)
		}
		if got := ref.String(); got != test.canonical {
			t.Errorf("%q renders as %q, want %q", test.raw, got, test.canonical)
		}
	}
}

// TestReferenceRefusesWhatWouldBeGuessedAt is the parser's real job. Every case
// here is one some other tool accepts by inferring something - a registry, a
// tag - and inference is how a workflow ends up pulling from a host nobody
// reviewed.
func TestReferenceRefusesWhatWouldBeGuessedAt(t *testing.T) {
	t.Parallel()

	for name, raw := range map[string]string{
		"no registry at all":         "alpine:3",
		"a bare repository path":     "library/alpine:3",
		"no tag and no digest":       "ghcr.io/owner/image",
		"an upper-case repository":   "ghcr.io/Owner/Image:1.0",
		"a tag beginning with a -":   "ghcr.io/owner/image:-bad",
		"a digest that is not one":   "ghcr.io/owner/image@sha256:short",
		"an unsupported algorithm":   "ghcr.io/owner/image@md5:" + strings.Repeat("a", 32),
		"a registry with a scheme":   "https://ghcr.io/owner/image:1.0",
		"a registry with userinfo":   "user@ghcr.io/owner/image:1.0",
		"an empty reference":         "",
		"a reference over the bound": strings.Repeat("x", maxReferenceBytes+1),
	} {
		if _, err := parseReference(raw); err == nil {
			t.Errorf("%s was accepted: %q", name, truncate(raw, 64))
		} else if !sdk.IsInvalidInput(err) {
			t.Errorf("%s: error is %v, want invalid input", name, err)
		}
	}
}

// TestARefusalNamesTheFormThatWorks: an author who mistyped a reference should
// not have to read this plugin's source to learn the grammar.
func TestARefusalNamesTheFormThatWorks(t *testing.T) {
	t.Parallel()

	_, err := parseReference("alpine:3")
	if err == nil {
		t.Fatal("a registry-less reference was accepted")
	}
	if !strings.Contains(err.Error(), "registry/repository[:tag][@digest]") {
		t.Errorf("the refusal does not show the form: %v", err)
	}
	if !strings.Contains(err.Error(), "never infers a registry") {
		t.Errorf("the refusal does not say why it will not guess: %v", err)
	}
}

// TestPlatformParsing covers the small grammar and its refusals.
func TestPlatformParsing(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]platform{
		"":               {},
		"linux/amd64":    {os: "linux", arch: "amd64"},
		"linux/arm64/v8": {os: "linux", arch: "arm64", variant: "v8"},
		"windows/amd64":  {os: "windows", arch: "amd64"},
	} {
		got, err := parsePlatform(raw)
		if err != nil {
			t.Errorf("parsePlatform(%q): %v", raw, err)
			continue
		}
		if got != want {
			t.Errorf("parsePlatform(%q) = %+v, want %+v", raw, got, want)
		}
	}

	for _, raw := range []string{"linux", "linux/", "/amd64", "Linux/AMD64", "linux/amd64/v8/extra"} {
		if _, err := parsePlatform(raw); err == nil {
			t.Errorf("platform %q was accepted", raw)
		}
	}
}

// TestPlatformMatchingTreatsAnUnstatedVariantAsAnyAndAStatedOneAsExact: a
// caller who wrote arm64/v8 meant v8, and one who wrote arm64 did not mean to
// exclude it.
func TestPlatformMatchingTreatsAnUnstatedVariantAsAnyAndAStatedOneAsExact(t *testing.T) {
	t.Parallel()

	any := platform{os: "linux", arch: "arm64"}
	if !any.matches("linux", "arm64", "v8") || !any.matches("linux", "arm64", "") {
		t.Error("a platform with no variant should match any variant")
	}

	exact := platform{os: "linux", arch: "arm64", variant: "v8"}
	if !exact.matches("linux", "arm64", "v8") {
		t.Error("a stated variant should match itself")
	}
	if exact.matches("linux", "arm64", "v7") || exact.matches("linux", "arm64", "") {
		t.Error("a stated variant should not match another")
	}
}

// TestACredentialHalfGivenIsRefused: a username with no password reads as an
// authenticated call and makes an anonymous one, which is the kind of silent
// downgrade that shows up as a 404 on a private repository.
func TestACredentialHalfGivenIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := credentialsFrom("robot", nil); err == nil {
		t.Error("a username with no password was accepted")
	}
	if creds, err := credentialsFrom("", nil); err != nil || creds.set() {
		t.Errorf("an anonymous call was refused: %+v %v", creds, err)
	}
}
