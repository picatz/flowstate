package plugin

import (
	"fmt"
	"maps"
	"slices"

	"github.com/picatz/flowstate/internal/strictyaml"
)

// EnvConfig is the file form of [Config.EnvByPlugin]: what an operator writes
// and hands to `flow worker --plugin-env-file`, the same way [PinsConfig] is
// the file form of [Config.PinnedDigests].
//
// A file rather than only a repeatable flag for the reason the pins file
// exists: a deployment configuring several plugins is describing its own
// shape, and that description is an artifact to review, diff and roll back
// rather than a command line to reconstruct.
//
// What belongs in it is configuration — an endpoint, a region, a path to a
// document the operator wrote. What does not is a secret value. A plugin's
// environment is copied by execve(2) into /proc/<pid>/environ, where anything
// running as this user can read it for as long as the plugin runs; that is why
// the handshake token travels on a descriptor instead. Name a *path* here and
// let the plugin read the file, or use the secret provider the deployment
// already has.
type EnvConfig struct {
	// Env maps a plugin name to the variables that plugin's processes are
	// launched with. Merged into [Config.EnvByPlugin]; see that field for what
	// reaches which plugin, and what the entries may not redefine.
	Env map[string]map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
}

// ParseEnvConfig decodes an environment file from YAML or JSON, a subset of
// YAML. Unknown and duplicate fields are errors, so a misspelled key, or one
// variable set twice for the same plugin in the same file, fails loudly at
// startup rather than silently launching a plugin with fewer variables — or a
// different value — than the file's author wrote.
//
// This checks the document's shape only. Whether each key is a name a plugin
// could answer to, and whether the whole grant fits the bound a launch
// environment has, is [Config.validate]'s job — run once at host construction
// however the entries arrived, so a file and a flag cannot come to mean two
// different things.
func ParseEnvConfig(data []byte) (EnvConfig, error) {
	var cfg EnvConfig

	if err := strictyaml.UnmarshalStrict(data, &cfg); err != nil {
		return EnvConfig{}, fmt.Errorf("%w: %w", ErrPluginEnv, err)
	}

	return cfg, nil
}

// ByPlugin renders the file as [Config.EnvByPlugin] expects it: "KEY=VALUE"
// entries, keyed by plugin name.
//
// Entries are sorted by variable name. A map has no order, and the launch
// environment built from one would otherwise differ run to run — which matters
// less for what a plugin reads (os.Getenv takes the last of a repeated key
// either way) than for what an operator sees when comparing two launches.
func (c EnvConfig) ByPlugin() map[string][]string {
	if len(c.Env) == 0 {
		return nil
	}

	byPlugin := make(map[string][]string, len(c.Env))
	for name, vars := range c.Env {
		entries := make([]string, 0, len(vars))
		for _, key := range slices.Sorted(maps.Keys(vars)) {
			entries = append(entries, key+"="+vars[key])
		}
		byPlugin[name] = entries
	}
	return byPlugin
}
