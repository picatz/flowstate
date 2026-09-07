package netpolicy

import (
	"reflect"
	"testing"
)

// FuzzParseConfig fuzzes the egress policy decoder — the SSRF boundary's own
// configuration, which an operator writes and every worker reads at startup
// (#1721) — and the byte-size parser its fields use. A config that parses is
// compiled to a policy as well, since a rule the decoder accepted and the
// compiler cannot hold is the second half of the same boundary. Every input
// is an error or a value, never both, and never a panic.
func FuzzParseConfig(f *testing.F) {
	f.Add([]byte("egress:\n  schemes: [https]\n  allow_ports: [443]\n"))
	f.Add([]byte("egress:\n  allow_loopback: true\n  deny:\n    - 'port != 443'\n"))
	f.Add([]byte("egress:\n  max_response_bytes: 4MiB\n  allow_private: false\n"))
	f.Add([]byte("egress:\n  deny:\n    - 'port !='\n"))
	f.Add([]byte("egres:\n  schemes: [https]\n"))
	f.Add([]byte("1KiB"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := ParseConfig(data)
		if err != nil && !reflect.DeepEqual(cfg, Config{}) {
			t.Fatalf("ParseConfig returned both an error and a config: %v", err)
		}
		if err == nil {
			policy, err := cfg.Policy()
			if err != nil && policy != nil {
				t.Fatalf("Config.Policy returned both an error and a policy: %v", err)
			}
			if err == nil && policy == nil {
				t.Fatal("Config.Policy returned neither a policy nor an error")
			}
		}

		size, err := ParseByteSize(string(data))
		if err != nil && size != 0 {
			t.Fatalf("ParseByteSize returned both an error and a size: %v", err)
		}
		var text ByteSize
		if err := text.UnmarshalText(data); err != nil && text != 0 {
			t.Fatalf("ByteSize.UnmarshalText returned both an error and a size: %v", err)
		}
	})
}
