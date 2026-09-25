package flowstatev1

import (
	"strings"
	"testing"
)

func TestValidateContentDigest(t *testing.T) {
	t.Parallel()

	valid := "sha256:" + strings.Repeat("a1", 32)
	if err := ValidateContentDigest(valid); err != nil {
		t.Fatalf("ValidateContentDigest(%q): %v", valid, err)
	}

	tests := []struct {
		name   string
		digest string
		want   string
	}{
		{name: "empty", want: `does not begin with "sha256:"`},
		{name: "tag is not content", digest: "registry.example/app:latest", want: `does not begin with "sha256:"`},
		{name: "wrong algorithm", digest: "sha512:" + strings.Repeat("a", 128), want: `does not begin with "sha256:"`},
		{name: "short", digest: "sha256:abc", want: "carries 3 hex characters, want 64"},
		{name: "upper case", digest: "sha256:" + strings.Repeat("A", 64), want: "not lower-case hexadecimal"},
		{name: "not hex", digest: "sha256:" + strings.Repeat("z", 64), want: "not lower-case hexadecimal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateContentDigest(tt.digest)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateContentDigest(%q) error = %v, want containing %q", tt.digest, err, tt.want)
			}
		})
	}
}
