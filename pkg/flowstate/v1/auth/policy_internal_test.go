package auth

import (
	"strings"
	"testing"
)

// TestValidateComposedHTTPSURL is #2038's finding 3 under the rule that
// closed it: [ValidateHTTPSURL] refuses any `@` or `%40` past the scheme's
// slashes, and gcpExchanger.impersonate composes
// `.../serviceAccounts/<email>:generateAccessToken` — a real resource
// identifier, not a credential — under an operator-configured iam_endpoint.
// validateComposedHTTPSURL is the one exemption, and it is structural: the
// base passes ValidateHTTPSURL in full, the composed URL is that base plus a
// "/", and what follows opens no query, fragment or empty segment.
func TestValidateComposedHTTPSURL(t *testing.T) {
	const (
		base     = "https://iam.corp.example:8443/v1"
		composed = base + "/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken"
	)

	t.Run("accepted under a validated base", func(t *testing.T) {
		parsed, err := validateComposedHTTPSURL(composed, base, "endpoint")
		if err != nil {
			t.Fatalf("validateComposedHTTPSURL(%q, %q) = %v, want no error", composed, base, err)
		}
		if parsed == nil {
			t.Fatalf("validateComposedHTTPSURL(%q, %q) returned a nil URL alongside a nil error", composed, base)
		}
	})

	t.Run("ValidateHTTPSURL itself still refuses the identical shape", func(t *testing.T) {
		// Pinned so the two functions disagreeing is a decision on the
		// record, not an oversight: an operator could write this URL by hand,
		// and every such URL carrying an `@` is refused.
		if _, err := ValidateHTTPSURL(composed, "endpoint"); err == nil {
			t.Fatalf("ValidateHTTPSURL(%q) accepted a shape validateComposedHTTPSURL exists to exempt", composed)
		}
	})

	t.Run("a loopback base is accepted the same way", func(t *testing.T) {
		const loopbackBase = "https://127.0.0.1:8443/v1"
		const loopback = loopbackBase + "/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken"
		if _, err := validateComposedHTTPSURL(loopback, loopbackBase, "endpoint"); err != nil {
			t.Fatalf("validateComposedHTTPSURL(%q, %q) = %v, want no error", loopback, loopbackBase, err)
		}
	})

	t.Run("refuses what is not a path under a validated base", func(t *testing.T) {
		tests := []struct {
			name string
			url  string
			base string
		}{
			{
				// The misread #2038 is about, handed in with an unrelated
				// base: the exemption is for text appended to the base, so a
				// URL that does not start with it gets nothing.
				name: "the base is not a prefix",
				url:  "https://acct9:2024/s3cr3t@iam.corp.example/v1/projects/-/serviceAccounts/sa",
				base: base,
			},
			{
				// A prefix that stops mid-authority: without the "/" after
				// the base, `:2024/s3cr3t@...` would extend base's host.
				name: "the base is a prefix but not followed by a slash",
				url:  "https://iam.corp.example:2024/s3cr3t@evil.example/x",
				base: "https://iam.corp.example",
			},
			{
				// The base carries the delimiter itself, so ValidateHTTPSURL
				// refuses it and nothing composed on it is exempt.
				name: "the base carries an at sign",
				url:  "https://acct9:2024/s3cr3t@iam.corp.example/projects/-/serviceAccounts/sa@p",
				base: "https://acct9:2024/s3cr3t@iam.corp.example",
			},
			{
				name: "the base carries an encoded at sign",
				url:  "https://iam.corp.example/a%40b/projects/-/serviceAccounts/sa@p",
				base: "https://iam.corp.example/a%40b",
			},
			{
				name: "the remainder opens a query",
				url:  base + "/projects/-/serviceAccounts/sa?x=a@b",
				base: base,
			},
			{
				name: "the remainder opens a fragment",
				url:  base + "/projects/-/serviceAccounts/sa#a@b",
				base: base,
			},
			{
				name: "the remainder opens an empty segment",
				url:  base + "//a@b",
				base: base,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := validateComposedHTTPSURL(tt.url, tt.base, "endpoint")
				if err == nil {
					t.Fatalf("validateComposedHTTPSURL(%q, %q) accepted a URL that is not a path under a validated base", tt.url, tt.base)
				}
				if strings.Contains(err.Error(), "s3cr3t") {
					t.Fatalf("validateComposedHTTPSURL(%q, %q) = %v, which repeats the credential", tt.url, tt.base, err)
				}
			})
		}
	})

	t.Run("still refuses what ValidateHTTPSURL refuses for every other reason", func(t *testing.T) {
		tests := []struct {
			name string
			url  string
			base string
		}{
			{name: "plain http against a non-loopback host", url: "http://iam.corp.example:8443/x", base: "http://iam.corp.example:8443"},
			{name: "an unsupported scheme", url: "ftp://iam.corp.example/x", base: "ftp://iam.corp.example"},
			{name: "no host", url: "https:///x/y", base: "https:///x"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := validateComposedHTTPSURL(tt.url, tt.base, "endpoint"); err == nil {
					t.Fatalf("validateComposedHTTPSURL(%q, %q) accepted a URL that fails a check the exemption does not touch", tt.url, tt.base)
				}
			})
		}
	})
}
