package plugin

import (
	"reflect"
	"testing"
)

// FuzzParseEnvConfig fuzzes the per-plugin environment decoder, an operator
// file that reaches a worker before anything else this package does: what it
// accepts becomes a launched plugin's environment, and what it mis-parses
// becomes one plugin holding another's configuration.
func FuzzParseEnvConfig(f *testing.F) {
	f.Add([]byte("env:\n  github:\n    GITHUB_BASE_URL: https://github.example\n"))
	f.Add([]byte("env:\n  docker:\n    FLOWSTATE_DOCKER_GRANTS: /etc/flowstate/docker.yaml\n"))
	f.Add([]byte(`{"env": {"ssh": {"FLOWSTATE_SSH_GRANTS": "/etc/flowstate/ssh.yaml"}}}`))
	f.Add([]byte("env:\n  GitHub:\n    KEY: value\n"))
	f.Add([]byte("env:\n  github:\n    : value\n"))
	f.Add([]byte("envv:\n  github: {}\n"))
	f.Add([]byte("env: {}\n"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := ParseEnvConfig(data)
		if err != nil && !reflect.DeepEqual(cfg, EnvConfig{}) {
			t.Fatalf("ParseEnvConfig returned both an error and a config: %v", err)
		}

		// ByPlugin is what a caller actually spends, so the accepted half is
		// exercised too: it must not panic and must not invent a plugin.
		for name, entries := range cfg.ByPlugin() {
			if name == "" {
				t.Fatal("ByPlugin returned an entry under the empty plugin name")
			}
			_ = entries
		}
	})
}
