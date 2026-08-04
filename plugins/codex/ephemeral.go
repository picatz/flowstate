package main

import (
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// buildEphemeralHome creates a fresh, per-run directory to use as the
// child's CODEX_HOME, so codex reads only what this call put there - never
// the worker user's own ~/.codex/config.toml or auth.json.
//
// This is the "never ambient" half of the config design (see policy.go's
// own doc comment for the three-layer narrowing this pairs with): codex-rs's
// own find_codex_home falls back to the invoking user's home directory
// whenever CODEX_HOME is unset, which - inherited through a plugin process
// that copied its own environment into the child, the way
// github.com/picatz/openai/codex's buildEnvironment does by default (see
// doc.go and exec.go's own note on why this plugin does not use that
// function) - would let whatever the worker's own user account happens to
// have configured leak into every run, silently and differently on every
// machine. A directory this call creates, populated with nothing but what
// operatorPolicy.RawConfig says (see policy.go), and destroyed when the run
// ends, is what makes a run's configuration a property of the request
// rather than of whichever machine happened to execute it.
//
// No auth.json is ever written here: the API key reaches codex as the
// CODEX_API_KEY environment variable this plugin sets directly on the
// child (see exec.go's childEnv), which codex's own auth resolution
// consults before ever looking for a stored credential in CODEX_HOME - so
// there is nothing for this directory to hold that could leak the key
// through a file left on disk.
func buildEphemeralHome(policy operatorPolicy) (dir string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", "flowstate-codex-home-*")
	if err != nil {
		return "", func() {}, sdk.Failed("creating an ephemeral CODEX_HOME: %v", err)
	}
	cleanup = func() { os.RemoveAll(dir) }

	if err := os.Chmod(dir, 0o700); err != nil {
		cleanup()
		return "", func() {}, sdk.Failed("securing the ephemeral CODEX_HOME: %v", err)
	}

	if len(policy.RawConfig) > 0 {
		configPath := dir + "/config.toml"
		if err := os.WriteFile(configPath, policy.RawConfig, 0o600); err != nil {
			cleanup()
			return "", func() {}, sdk.Failed("writing the ephemeral CODEX_HOME's config.toml: %v", err)
		}
	}

	return dir, cleanup, nil
}
