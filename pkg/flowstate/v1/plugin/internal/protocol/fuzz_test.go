package protocol

import (
	"bytes"
	"testing"
)

// FuzzPluginHandshake fuzzes the three parsers that read bytes a plugin
// process, or the environment a plugin was launched with, hands the other
// side of the protocol: the handshake line, the offered-versions value and
// the token descriptor (#1721). Each is deliberately strict, and the property
// under fuzz is that strictness never becomes a panic: every input is an
// error or a value, never both.
func FuzzPluginHandshake(f *testing.F) {
	f.Add([]byte("flowstate-plugin|1|example|1.0.0|127.0.0.1:1234\n"))
	f.Add([]byte("flowstate-plugin|1|||\n"))
	f.Add([]byte("1,2,3"))
	f.Add([]byte("token-bytes\n"))
	f.Add([]byte("two\nlines\n"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		line := string(data)

		handshake, err := ParseHandshake(line)
		if err != nil && handshake != (Handshake{}) {
			t.Fatalf("ParseHandshake returned both an error and a handshake: %v", err)
		}

		versions, err := ParseVersions(line)
		if err != nil && versions != nil {
			t.Fatalf("ParseVersions returned both an error and versions: %v", err)
		}
		if err == nil && len(versions) == 0 {
			t.Fatal("ParseVersions returned no versions and no error")
		}

		token, err := ReadToken(bytes.NewReader(data))
		if err != nil && token != "" {
			t.Fatalf("ReadToken returned both an error and a token: %v", err)
		}
		if err == nil && token == "" {
			t.Fatal("ReadToken returned an empty token and no error")
		}
	})
}
