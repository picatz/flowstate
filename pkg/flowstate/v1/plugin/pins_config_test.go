package plugin

import (
	"errors"
	"testing"
)

// TestParsePinsConfigReadsAPin covers the ordinary case: a well-formed pins
// file decodes into the map [Config.PinnedDigests] compares against.
func TestParsePinsConfigReadsAPin(t *testing.T) {
	cfg, err := ParsePinsConfig([]byte(`
pins:
  github: sha256:` + hex64('a') + `
`))
	if err != nil {
		t.Fatalf("ParsePinsConfig: %v", err)
	}

	if got := cfg.Pins["github"]; got != "sha256:"+hex64('a') {
		t.Fatalf("pin for github = %q", got)
	}
}

// TestParsePinsConfigRefusesAnUnknownKey covers the fail-closed-on-config
// half of #1010: a misspelled top-level key is a startup error, exactly as
// [flowstatev1.ParseTaskPolicyConfig] refuses one in its own file, rather
// than a pin the operator believed was configured and never took effect.
func TestParsePinsConfigRefusesAnUnknownKey(t *testing.T) {
	_, err := ParsePinsConfig([]byte(`
pinns:
  github: sha256:` + hex64('a') + `
`))
	if err == nil {
		t.Fatal("ParsePinsConfig accepted an unknown top-level key")
	}
}

// TestParsePinsConfigRefusesADuplicateKey covers the file's own duplicate-key
// case, which YAML permits by default and this format does not — the same
// rule [flowstatev1.ParseTaskPolicyConfig] applies to its file.
func TestParsePinsConfigRefusesADuplicateKey(t *testing.T) {
	_, err := ParsePinsConfig([]byte(`
pins:
  github: sha256:` + hex64('a') + `
pins:
  slack: sha256:` + hex64('b') + `
`))
	if err == nil {
		t.Fatal("ParsePinsConfig accepted a duplicate top-level key")
	}
}

// TestParsePinsConfigRefusesMalformedYAML covers the document simply not
// being YAML at all.
func TestParsePinsConfigRefusesMalformedYAML(t *testing.T) {
	_, err := ParsePinsConfig([]byte("not: [valid: yaml"))
	if err == nil {
		t.Fatal("ParsePinsConfig accepted malformed YAML")
	}
	if !errors.Is(err, ErrDigestPin) {
		t.Fatalf("error = %v, want it to wrap ErrDigestPin", err)
	}
}

// TestParsePinsConfigDoesNotValidateDigestShape documents a boundary rather
// than asserting a feature: shape validation (a valid plugin name, a
// well-formed digest) is [Config.validate]'s job via [validateDigestPin], run
// once at host construction for every source of a pin. A malformed entry
// still parses here and is refused later, by the same code and with the same
// message regardless of whether the pin came from this file or a
// --plugin-pin flag — see TestMalformedPinsAreRefusedAtConfigLoad in
// admission_test.go for that refusal.
func TestParsePinsConfigDoesNotValidateDigestShape(t *testing.T) {
	cfg, err := ParsePinsConfig([]byte(`
pins:
  github: not-a-digest
`))
	if err != nil {
		t.Fatalf("ParsePinsConfig: %v", err)
	}

	if got := cfg.Pins["github"]; got != "not-a-digest" {
		t.Fatalf("pin for github = %q", got)
	}
}

// hex64 returns 64 hex characters, all the same one, for a syntactically
// valid (if meaningless) digest in a test fixture.
func hex64(c byte) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
