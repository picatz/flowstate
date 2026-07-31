package netpolicy

import (
	"strings"
	"testing"
)

// TestByteSizeForms is the whole point of the type: every spelling below is
// the same bound, and the reviewer reading the policy file gets to see the one
// the operator meant.
func TestByteSizeForms(t *testing.T) {
	t.Parallel()

	for value, want := range map[string]ByteSize{
		"1048576": 1 << 20,
		"1MiB":    1 << 20,
		"1mib":    1 << 20,
		"10MB":    10_000_000,
		"512KiB":  512 << 10,
		"2GiB":    2 << 30,
		"1TB":     1_000_000_000_000,
		"64B":     64,
		" 1MiB ":  1 << 20,
		"1 MiB":   1 << 20,
	} {
		got, err := ParseByteSize(value)
		if err != nil {
			t.Fatalf("%q: %v", value, err)
		}
		if got != want {
			t.Fatalf("%q = %d, want %d", value, got, want)
		}
	}
}

// TestByteSizeRefusals: each refusal names what to write instead, because the
// reader is mid-edit in a policy file when they see it.
func TestByteSizeRefusals(t *testing.T) {
	t.Parallel()

	for value, wantInMessage := range map[string]string{
		"1.5GiB": "1536MiB", // fractional, with the arithmetic done for the reader
		"-1MiB":  "negative",
		"MiB":    "no count",
		"lots":   "not a byte size",
		"":       "empty",
	} {
		_, err := ParseByteSize(value)
		if err == nil {
			t.Fatalf("%q parsed; it must be refused", value)
		}
		if !strings.Contains(err.Error(), wantInMessage) {
			t.Fatalf("%q: error %q does not carry %q", value, err, wantInMessage)
		}
	}
}

// TestByteSizeThroughThePolicyFile pins the YAML path end to end, in both the
// readable form and the bare-count compatibility form — and that the parsed
// bound actually reaches the built policy, not only the config struct.
func TestByteSizeThroughThePolicyFile(t *testing.T) {
	t.Parallel()

	for doc, want := range map[string]int64{
		"egress:\n  max_response_bytes: 1MiB\n":    1 << 20,
		"egress:\n  max_response_bytes: \"2MB\"\n": 2_000_000,
		"egress:\n  max_response_bytes: 1048576\n": 1 << 20,
	} {
		cfg, err := ParseConfig([]byte(doc))
		if err != nil {
			t.Fatalf("parsing %q: %v", doc, err)
		}

		p, err := cfg.Policy()
		if err != nil {
			t.Fatalf("building %q: %v", doc, err)
		}

		if got := p.MaxResponseBytes(); got != want {
			t.Fatalf("%q: policy bound = %d, want %d", doc, got, want)
		}
	}

	// A fractional size in the file is refused with the suggestion, at load,
	// where every other policy mistake is refused.
	if _, err := ParseConfig([]byte("egress:\n  max_response_bytes: 2.5MiB\n")); err == nil {
		t.Fatal("a fractional size in a policy file parsed; it must refuse at load")
	}
}
