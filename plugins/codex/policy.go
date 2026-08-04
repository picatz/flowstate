package main

import (
	"os"

	"github.com/BurntSushi/toml"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

// policyEnv names the environment variable pointing at an operator-provided
// base config for codex - a real codex config.toml, or a fragment of one -
// the enterprise-shaped layer this plugin sits between a Flowfile's own
// narrower request and the codex CLI's much wider configuration surface.
//
// # The three layers, and why narrowing is the only direction a task moves
//
//  1. This plugin's own fail-closed baseline: sandbox READ_ONLY, no network -
//     what a deployment gets with nothing configured at all.
//  2. The operator's base config, named by this variable: raised (or left)
//     by whoever runs the worker, the same way a GitHub Actions org sets a
//     workflow permission ceiling a repository's own workflow file cannot
//     exceed - see CLAUDE.md's own framing of issue #172's lesson, applied
//     here to an agent's own sandbox rather than a token's scopes.
//  3. A task's own sandbox_mode and allow_network inputs, which may only
//     select *within* what layer 2 permits. A task asking for more than the
//     operator's ceiling is refused as [sdk.InvalidInput] naming the field -
//     never silently downgraded to what is allowed, which would make a
//     workflow author's request appear to succeed while quietly doing less
//     than asked, the same silent-clamping mistake bounds.go's own
//     clampMaxOutputBytes refuses to make.
//
// Only two of codex's own config keys are read out of the operator's file -
// sandbox_mode (the ceiling for layer 3's own sandbox_mode input) and
// sandbox_workspace_write.network_access (the ceiling for allow_network).
// Everything else in the file is opaque to this plugin: it is copied
// byte-for-byte into the ephemeral CODEX_HOME this plugin builds per run
// (see ephemeral.go), so an operator can still pin whatever else codex's
// config surface offers - a model provider allowlist, MCP servers, and so
// on - without this plugin needing to understand it. See doc.go's "Codex
// configuration: what this plugin covers, and what it does not" for why
// this slice (two keys) is deliberate rather than an oversight.
const policyEnv = "FLOWSTATE_CODEX_BASE_CONFIG"

// maxPolicyBytes bounds the operator's own config file - large by ordinary
// config.toml standards, but still a real cap: this file is read into
// memory once per plugin process launch's worth of task calls, and an
// operator's own misconfiguration (pointing this at something enormous)
// should not be able to make this plugin allocate without limit.
const maxPolicyBytes = 512 << 10 // 512 KiB

// operatorPolicy is the ceiling a task's own inputs may narrow within, plus
// the raw bytes of whatever else the operator's file said.
type operatorPolicy struct {
	MaxSandbox   codexv1.SandboxMode
	AllowNetwork bool
	RawConfig    []byte
}

// defaultOperatorPolicy is what a deployment gets with policyEnv unset -
// this plugin's own fail-closed baseline, layer 1 above: the most
// restricted sandbox, no network, and nothing to copy into the ephemeral
// CODEX_HOME (see ephemeral.go), so codex falls back to its own built-in
// defaults for everything this plugin does not itself set - never an
// operator's or the worker user's own ambient config.toml.
func defaultOperatorPolicy() operatorPolicy {
	return operatorPolicy{MaxSandbox: codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY}
}

// operatorConfigToml is the subset of codex's own config.toml schema this
// plugin reads - two keys, named and typed exactly as upstream
// (codex-rs/config/src/config_toml.rs) spells them, so an operator can hand
// this plugin a real codex config.toml (or a fragment naming just these two
// keys) rather than a schema of this plugin's own invention.
type operatorConfigToml struct {
	SandboxMode           string `toml:"sandbox_mode"`
	SandboxWorkspaceWrite struct {
		NetworkAccess bool `toml:"network_access"`
	} `toml:"sandbox_workspace_write"`
}

// loadOperatorPolicy reads and parses the operator's base config, or
// returns [defaultOperatorPolicy] when none is configured.
func loadOperatorPolicy() (operatorPolicy, error) {
	path := os.Getenv(policyEnv)
	if path == "" {
		return defaultOperatorPolicy(), nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return operatorPolicy{}, sdk.Failed("%s (%q): %v", policyEnv, truncatePath(path), err)
	}
	if info.IsDir() {
		return operatorPolicy{}, sdk.Failed("%s (%q) is a directory, not a config.toml file", policyEnv, truncatePath(path))
	}
	if info.Size() > maxPolicyBytes {
		return operatorPolicy{}, sdk.Failed(
			"%s (%q) is %d bytes, over the %d byte limit this plugin reads", policyEnv, truncatePath(path), info.Size(), maxPolicyBytes)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return operatorPolicy{}, sdk.Failed("%s (%q): %v", policyEnv, truncatePath(path), err)
	}

	var parsed operatorConfigToml
	if _, err := toml.Decode(string(raw), &parsed); err != nil {
		return operatorPolicy{}, sdk.Failed("%s (%q) does not parse as TOML: %v", policyEnv, truncatePath(path), err)
	}

	maxSandbox, err := sandboxModeFromConfigValue(parsed.SandboxMode)
	if err != nil {
		return operatorPolicy{}, sdk.Failed("%s (%q): sandbox_mode: %v", policyEnv, truncatePath(path), err)
	}

	return operatorPolicy{
		MaxSandbox:   maxSandbox,
		AllowNetwork: parsed.SandboxWorkspaceWrite.NetworkAccess,
		RawConfig:    raw,
	}, nil
}

// sandboxModeFromConfigValue maps a codex config.toml sandbox_mode string
// onto this task's own enum, matching upstream's exact spellings
// (codex-rs/protocol/src/config_types.rs's SandboxMode, kebab-case) - the
// same strings sandboxCLIValue in exec.go maps the other direction. An
// empty value is codex's own documented default, read-only
// (codex-rs/config/src/config_toml.rs), so this plugin's ceiling matches
// what codex itself would do with no sandbox_mode key at all.
func sandboxModeFromConfigValue(raw string) (codexv1.SandboxMode, error) {
	switch raw {
	case "", "read-only":
		return codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY, nil
	case "workspace-write":
		return codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE, nil
	case "danger-full-access":
		return codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS, nil
	default:
		return 0, sdk.Failed("%q is not a sandbox_mode value codex recognizes "+
			"(read-only, workspace-write, danger-full-access)", raw)
	}
}

// sandboxRank orders the three sandbox levels from least to most permissive,
// so a ceiling comparison is a plain integer comparison rather than a
// hand-written switch repeated at every call site.
func sandboxRank(mode codexv1.SandboxMode) int {
	switch mode {
	case codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY:
		return 0
	case codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE:
		return 1
	case codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS:
		return 2
	default:
		// Unreachable once sandboxCLIValue has already refused anything
		// else; ranked most restrictive so an impossible value never
		// accidentally compares as more permissive than it should.
		return 0
	}
}

// narrowSandbox refuses a requested sandbox_mode over the operator's own
// ceiling, rather than silently downgrading it - see policyEnv's own doc
// comment, "monotonic narrowing."
func narrowSandbox(requested codexv1.SandboxMode, policy operatorPolicy) error {
	if sandboxRank(requested) > sandboxRank(policy.MaxSandbox) {
		return sdk.InvalidInput(
			"sandbox_mode %s exceeds this worker's configured ceiling (%s); an operator raises the "+
				"ceiling via %s, a Flowfile cannot", sandboxModeConfigValue(requested), sandboxModeConfigValue(policy.MaxSandbox), policyEnv)
	}
	return nil
}

// narrowNetwork refuses allow_network=true when the operator's policy has
// not granted it. Meaningless outside WORKSPACE_WRITE (see codex.proto's
// own comment on allow_network), so this only ever refuses within that
// mode - a READ_ONLY or DANGER_FULL_ACCESS run never reaches this check at
// all (see exec.go).
func narrowNetwork(requested bool, policy operatorPolicy) error {
	if requested && !policy.AllowNetwork {
		return sdk.InvalidInput(
			"allow_network=true exceeds this worker's configured policy (network access for " +
				"sandbox_workspace_write is not granted); an operator grants it via " + policyEnv + ", a Flowfile cannot")
	}
	return nil
}

// sandboxModeConfigValue renders this task's enum back to codex's own
// config.toml/CLI spelling, for an error message a Flowfile author or an
// operator can search codex's own documentation for verbatim.
func sandboxModeConfigValue(mode codexv1.SandboxMode) string {
	switch mode {
	case codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY:
		return "read-only"
	case codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE:
		return "workspace-write"
	case codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS:
		return "danger-full-access"
	default:
		return "unspecified"
	}
}
