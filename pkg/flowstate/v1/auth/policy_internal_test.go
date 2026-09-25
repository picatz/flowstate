package auth

import "testing"

// TestValidateComposedHTTPSURL is #2038's finding 3: an iam_endpoint on a
// non-loopback port loaded, then failed every impersonation request, because
// gcpExchanger.impersonate composes
// `.../serviceAccounts/<email>:generateAccessToken` — a real resource
// identifier, not a credential — and post() re-validated that composed URL
// through the same ambiguous-authority search a hand-written policy value
// needs. validateComposedHTTPSURL is the fix: every check ValidateHTTPSURL
// runs except that one search, for a URL this package built itself from an
// already-validated base plus a url.PathEscape'd field.
func TestValidateComposedHTTPSURL(t *testing.T) {
	const composed = "https://iam.corp.example:8443/v1/projects/-/serviceAccounts/" +
		"sa%40p.iam.gserviceaccount.com:generateAccessToken"

	t.Run("accepted despite the ambiguous authority ValidateHTTPSURL would refuse", func(t *testing.T) {
		parsed, err := validateComposedHTTPSURL(composed, "endpoint")
		if err != nil {
			t.Fatalf("validateComposedHTTPSURL(%q) = %v, want no error", composed, err)
		}
		if parsed == nil {
			t.Fatalf("validateComposedHTTPSURL(%q) returned a nil URL alongside a nil error", composed)
		}
	})

	t.Run("ValidateHTTPSURL itself still refuses the identical shape", func(t *testing.T) {
		// Pinned so the two functions disagreeing is a decision on the
		// record, not an oversight: this is exactly the URL an operator
		// could write by hand, and it is exactly the shape
		// picatz/flowstate#2038 closes.
		_, err := ValidateHTTPSURL(composed, "endpoint")
		if err == nil {
			t.Fatalf("ValidateHTTPSURL(%q) accepted a shape validateComposedHTTPSURL exists to bypass", composed)
		}
	})

	t.Run("still refuses what ValidateHTTPSURL refuses for every other reason", func(t *testing.T) {
		tests := []struct {
			name string
			url  string
		}{
			{name: "not a URL at all", url: "://not a url"},
			{name: "no host", url: "https:///x"},
			{name: "plain http against a non-loopback host", url: "http://iam.corp.example:8443/x"},
			{name: "an unsupported scheme", url: "ftp://iam.corp.example/x"},
			{name: "literal userinfo, not a composed field", url: "https://acct9:s3cr3t@iam.corp.example:8443/x"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := validateComposedHTTPSURL(tt.url, "endpoint"); err == nil {
					t.Fatalf("validateComposedHTTPSURL(%q) accepted a URL that fails a check the composed skip does not touch", tt.url)
				}
			})
		}
	})

	t.Run("a loopback ported endpoint is accepted either way", func(t *testing.T) {
		const loopback = "https://127.0.0.1:8443/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken"
		if _, err := validateComposedHTTPSURL(loopback, "endpoint"); err != nil {
			t.Fatalf("validateComposedHTTPSURL(%q) = %v, want no error", loopback, err)
		}
		if _, err := ValidateHTTPSURL(loopback, "endpoint"); err != nil {
			t.Fatalf("ValidateHTTPSURL(%q) = %v, want no error (the loopback exemption already covers this)", loopback, err)
		}
	})
}

// TestHostCarriesPortDelimiter is the direct unit test for the helper the
// ambiguous-authority credential check in ValidateHTTPSURL keys on, covering
// the shapes urlprobe found ValidateHTTPSURL's earlier Port() != "" gate
// missing.
func TestHostCarriesPortDelimiter(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{host: "issuer.example.com", want: false},
		{host: "acct9:2024", want: true},
		{host: "acct9:", want: true}, // the empty-port shape urlprobe found.
		{host: "[::1]", want: false},
		{host: "[::1]:2024", want: true},
		{host: "[2001:db8::1]", want: false},
		{host: "[2001:db8::1]:8443", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := hostCarriesPortDelimiter(tt.host); got != tt.want {
				t.Errorf("hostCarriesPortDelimiter(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}
