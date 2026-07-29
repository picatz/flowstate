package protocol

import (
	"strings"
	"testing"
)

// TestHandshakeRoundTrip checks that what a plugin writes is what the host
// reads. Both sides are generated from this one definition, so a change that
// broke the pairing would break here rather than at a worker's startup.
func TestHandshakeRoundTrip(t *testing.T) {
	t.Parallel()

	want := Handshake{
		HandshakeVersion: HandshakeVersion,
		ProtocolVersion:  Version2,
		Network:          NetworkUnix,
		Address:          "/var/folders/abc/fsplug123/s",
	}

	got, err := ParseHandshake(want.String() + "\n")
	if err != nil {
		t.Fatalf("ParseHandshake(%q): %v", want.String(), err)
	}
	if got != want {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

// TestParseHandshake covers what the host must refuse. It is the first thing an
// untrusted process says, so every field is checked rather than interpreted
// generously.
func TestParseHandshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		line        string
		wantMessage string
	}{
		{
			name:        "empty",
			line:        "",
			wantMessage: "empty handshake line",
		},
		{
			name:        "a program that is not a plugin",
			line:        "usage: mytool [options]",
			wantMessage: "is this a Flowstate plugin?",
		},
		{
			name:        "the wrong sentinel",
			line:        "OTHER-PLUGIN|1|1|unix|/tmp/s",
			wantMessage: "is this a Flowstate plugin?",
		},
		{
			name:        "too few fields",
			line:        "FLOWSTATE-PLUGIN|1|1|unix",
			wantMessage: "has 4 fields",
		},
		{
			name:        "too many fields",
			line:        "FLOWSTATE-PLUGIN|1|1|unix|/tmp/s|extra",
			wantMessage: "has 6 fields",
		},
		{
			name:        "a handshake version that is not a number",
			line:        "FLOWSTATE-PLUGIN|one|1|unix|/tmp/s",
			wantMessage: "handshake format version",
		},
		{
			name:        "a zero protocol version",
			line:        "FLOWSTATE-PLUGIN|1|0|unix|/tmp/s",
			wantMessage: "not a positive version",
		},
		{
			name:        "a negative protocol version",
			line:        "FLOWSTATE-PLUGIN|1|-1|unix|/tmp/s",
			wantMessage: "not a positive version",
		},
		{
			// Accepting "+1" or "01" would mean two spellings of one version,
			// which is a difference that eventually matters somewhere.
			name:        "a non-canonical version",
			line:        "FLOWSTATE-PLUGIN|1|+1|unix|/tmp/s",
			wantMessage: "not a canonical number",
		},
		{
			name:        "no network",
			line:        "FLOWSTATE-PLUGIN|1|1||/tmp/s",
			wantMessage: "names no network",
		},
		{
			name:        "no address",
			line:        "FLOWSTATE-PLUGIN|1|1|unix|",
			wantMessage: "names no address",
		},
		{
			name:        "a relative socket path",
			line:        "FLOWSTATE-PLUGIN|1|1|unix|relative/s",
			wantMessage: "is not absolute",
		},
		{
			name:        "longer than the bound",
			line:        "FLOWSTATE-PLUGIN|1|1|unix|/" + strings.Repeat("x", MaxHandshakeLine),
			wantMessage: "longer than",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseHandshake(test.line)
			if err == nil {
				t.Fatalf("ParseHandshake(%q) succeeded, want a refusal", truncate(test.line, 64))
			}
			if !strings.Contains(err.Error(), test.wantMessage) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), test.wantMessage)
			}
		})
	}
}

// TestParseHandshakeBoundsErrorText checks that an error about a hostile line
// cannot itself carry a megabyte of that process's choosing.
func TestParseHandshakeBoundsErrorText(t *testing.T) {
	t.Parallel()

	_, err := ParseHandshake(strings.Repeat("A", 2000))
	if err == nil {
		t.Fatal("a long line was accepted")
	}
	if len(err.Error()) > 256 {
		t.Errorf("the error is %d bytes, want a bounded one", len(err.Error()))
	}
}

// TestVersions checks the version list the host offers and the plugin parses.
func TestVersions(t *testing.T) {
	t.Parallel()

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()

		got, err := ParseVersions(FormatVersions([]int{1, 2, 5}))
		if err != nil {
			t.Fatalf("ParseVersions: %v", err)
		}
		if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 5 {
			t.Errorf("versions = %v, want [1 2 5]", got)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		t.Parallel()

		for _, input := range []string{"", "   ", "one", "1,", "1,,2", "0", "-1", strings.Repeat("1,", MaxOfferedVersions+1) + "1"} {
			if _, err := ParseVersions(input); err == nil {
				t.Errorf("ParseVersions(%q) succeeded, want a refusal", truncate(input, 32))
			}
		}
	})
}

// TestNegotiate checks that the highest common version wins, and that no common
// version is reported rather than guessed at.
func TestNegotiate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		offered   []int
		supported []int
		want      int
		wantOK    bool
	}{
		{name: "one in common", offered: []int{1}, supported: []int{1}, want: 1, wantOK: true},
		{name: "the highest in common", offered: []int{1, 2, 3}, supported: []int{2, 3, 4}, want: 3, wantOK: true},
		{name: "order does not matter", offered: []int{3, 1, 2}, supported: []int{2, 1}, want: 2, wantOK: true},
		{name: "nothing in common", offered: []int{1}, supported: []int{2}, wantOK: false},
		{name: "the plugin speaks nothing", offered: []int{1}, supported: nil, wantOK: false},
		{name: "the host offers nothing", offered: nil, supported: []int{1}, wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := Negotiate(test.offered, test.supported)
			if ok != test.wantOK {
				t.Fatalf("Negotiate ok = %v, want %v", ok, test.wantOK)
			}
			if ok && got != test.want {
				t.Errorf("Negotiate = %d, want %d", got, test.want)
			}
		})
	}
}
