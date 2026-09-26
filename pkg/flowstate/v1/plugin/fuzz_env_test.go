package plugin

import (
	"errors"
	"reflect"
	"strings"
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
	// A flow mapping can use "" as a key like any other (picatz/flowstate
	// #2105's crasher, in shape rather than the reported bytes): a document,
	// not the crasher itself.
	f.Add([]byte("env:\n  \"\": {}\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := ParseEnvConfig(data)
		if err != nil && !reflect.DeepEqual(cfg, EnvConfig{}) {
			t.Fatalf("ParseEnvConfig returned both an error and a config: %v", err)
		}

		// ByPlugin is what a caller actually spends, so the accepted half is
		// exercised too: it must not panic, and rendering a name faithfully
		// is not itself the property — refusing an invalid one is. Each name
		// is checked alone, under an EnvByPlugin holding nothing else: that
		// is the only way an error from Config.validate can be attributed to
		// this name's own validity rather than a malformed KEY=VALUE entry
		// elsewhere in the map, or an earlier-sorting name's problem — both
		// of which validate would also report as a non-nil error, without
		// this name ever having been refused for what makes it invalid.
		for name := range cfg.ByPlugin() {
			if validPluginName(name) {
				continue
			}
			verr := (Config{EnvByPlugin: map[string][]string{name: nil}}).validate()
			if verr == nil || !errors.Is(verr, ErrPluginEnv) || !strings.Contains(verr.Error(), "not a valid plugin name") {
				t.Fatalf("ByPlugin rendered %q, which no plugin could ever answer to, and Config.validate did not refuse it as such: %v", name, verr)
			}
		}
	})
}
