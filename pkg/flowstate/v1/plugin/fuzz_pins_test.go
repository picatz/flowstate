package plugin

import (
	"reflect"
	"testing"
)

// FuzzParsePinsConfig fuzzes the plugin pins decoder, an operator file that
// decides which binaries a host will launch (#1721). Every input is an error
// or a value, never both, and never a panic.
func FuzzParsePinsConfig(f *testing.F) {
	f.Add([]byte("pins:\n  github: sha256:" + hex64('a') + "\n"))
	f.Add([]byte("pinns:\n  github: sha256:" + hex64('a') + "\n"))
	f.Add([]byte(`{"pins": {"git": "sha256:` + hex64('b') + `"}}`))
	f.Add([]byte("pins: {}\n"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := ParsePinsConfig(data)
		if err != nil && !reflect.DeepEqual(cfg, PinsConfig{}) {
			t.Fatalf("ParsePinsConfig returned both an error and a config: %v", err)
		}
	})
}
