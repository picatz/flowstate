// Command flowstate-plugin-codex provides one task, codex.exec: a single
// bounded run of OpenAI Codex, over the real `codex` CLI binary - not a
// client of any codex network protocol.
//
// # Sources of truth, pinned
//
// Two upstream projects informed this plugin, and they disagree in places,
// so both are named here with the exact revision read rather than left to
// "whatever the latest version does" - the area moves fast enough that this
// pin is what makes a future drift diagnosable instead of mysterious:
//
//   - github.com/picatz/openai/codex at commit 02ace0a229c75a724ede668ab405ae71405e406d
//     (2025-11-25) is the Go SDK this plugin depends on for its module
//     graph: Exec.Run's command-line construction (adapted, not used
//     directly - see "Why this plugin does not use Exec.Run for the process
//     launch" below), and the ThreadEvent/ThreadItem decoding types this
//     plugin's readRun still uses as-is.
//   - github.com/openai/codex (the Codex CLI/app-server Rust sources) at
//     commit 64bb8094ba3b2c77becea8281a4b070e05e6c758 (2026-08-04) is the
//     actual protocol: codex-rs/exec/src/exec_events.rs for the JSON event
//     schema `codex exec --json` emits, codex-rs/protocol/src/config_types.rs
//     and codex-rs/config/src/config_toml.rs for the sandbox and config.toml
//     vocabulary, and codex-rs/utils/cli/src/{sandbox_mode_cli_arg,
//     config_override}.rs for what the CLI's own flags actually accept. This
//     is the one that is source of truth for the wire format: the Go SDK is
//     a convenience wrapper a third party maintains, not itself where the
//     protocol is defined.
//
// # Where the two disagree, and which one this plugin coded against
//
// Reading both, not just the Go SDK, found real drift:
//
//   - The Go SDK's Args.ConfigFile becomes a single `--config <value>` flag
//     with one string argument. Upstream's actual `--config` (`-c`) is a
//     *repeatable* `key=value` override (codex-rs/utils/cli/src/
//     config_override.rs's CliConfigOverrides), parsed as a dotted path into
//     the config tree - "a config file path" was never a thing `--config`
//     accepted upstream. This plugin does not use Args.ConfigFile at all for
//     that reason; see "Codex configuration" below for what it does instead
//     (an isolated CODEX_HOME directory plus explicit `-c` overrides for the
//     one key this task's inputs narrow).
//   - Upstream's event schema (exec_events.rs) has grown a `CollabToolCall`
//     item kind and a `Declined` CommandExecutionStatus value the Go SDK's
//     copy (items.go in that module) does not know about, and
//     TurnCompletedEvent's Usage carries `cache_write_input_tokens` and
//     `reasoning_output_tokens` fields the Go SDK's own Usage struct has no
//     field for. None of this breaks decoding - see "Defensive parsing"
//     below for why - but it does mean this task's usage output is known to
//     under-report those two token counts, and an event of either newer kind
//     summarizes as "unknown" (see events are handled in exec.go's
//     applyItemEvent) rather than with a name. Recorded here rather than
//     discovered by an operator comparing this task's numbers against an
//     OpenAI invoice.
//   - `--sandbox`'s three values (codex-rs/utils/cli/src/
//     sandbox_mode_cli_arg.rs) and config.toml's `sandbox_mode` key
//     (config_types.rs) agree with each other and with the Go SDK's
//     SandboxMode constants - kebab-case read-only/workspace-write/
//     danger-full-access, all three - so codex.proto's own enum and
//     sandboxCLIValue/sandboxModeFromConfigValue in exec.go and policy.go
//     needed no correction here.
//
// # Defensive parsing: an unrecognized event is counted, never fatal
//
// Given how fast this surface changes, readRun (exec.go) and applyItemEvent
// treat every event and item kind this build does not specifically
// recognize as data, not as a decode failure: the Go SDK's own
// UnmarshalThreadItem already falls back to UnknownThreadItem for an
// unrecognized `type` discriminator rather than erroring (items.go in that
// module), and a line that fails to parse as JSON at all becomes a single
// "unparsed" EventSummary rather than aborting the run - see readRun's own
// comment. A future codex release adding a tenth item kind or a fourth
// sandbox level should make this task report something less specific about
// that one event, never make the run fail outright over a shape neither
// this plugin nor its dependency has seen yet.
//
// # What the library actually is, and what that means here
//
// The library does not speak a wire protocol to a running codex server, and
// it does not call the OpenAI API directly. Exec.Run in the library's own
// exec.go builds a command line - `codex exec --json [--model ...]
// [--sandbox ...] [--cd ...] ...` - starts it with os/exec, writes the
// prompt to its stdin, and decodes one JSON event per line from its stdout
// until the process exits. Every input this plugin's own schema
// (proto/codex/v1/codex.proto) exposes - model, sandbox_mode,
// working_context - is a flag on that command line, not a field in a
// request body. There is no way to ask the library to do anything without
// executing that binary.
//
// # Why this plugin execs a subprocess, when plugins/vcs and plugins/github
// # refuse to
//
// plugins/vcs/doc.go's "No subprocesses, ever" is not a rule this repository
// applies uniformly for its own sake; it is a rule about what a *version
// control* task needs to do, and the whole argument there is that go-git
// gives that plugin a pure-Go alternative with the same result. There is no
// such alternative here: running an agent binary is not incidental to what
// this task does, it is the task. Codex's own execution engine - its
// sandboxing, its own tool-calling loop, its own handling of a model's
// output - lives entirely inside that binary and nowhere this plugin could
// reimplement in Go without becoming a second, worse copy of Codex itself.
// The three reasons plugins/vcs gives for avoiding exec (an ambient
// dependency on what happens to be installed; a larger injection surface;
// harder-to-contain environment and credentials) are all still real here,
// and this plugin answers each of them directly rather than accepting them
// as unavoidable:
//
//   - Ambient dependency: binary.go resolves the codex binary from a single
//     required environment variable, FLOWSTATE_CODEX_BIN, checked to be an
//     absolute path to a regular, executable file. It is never looked up on
//     $PATH - os/exec.LookPath is never called - so a worker cannot pick up
//     whatever "codex" happens to resolve to in whatever shell launched it,
//     the exact ambient-trust failure plugins/vcs's own bullet describes for
//     git.
//   - Injection surface: every argument that reaches the subprocess is
//     either a fixed flag this plugin chose or a value validated in
//     bounds.go before it is ever placed in the argv slice the library
//     builds - never shell text, since exec.CommandContext never invokes a
//     shell to interpret it. The prompt itself travels over the child's
//     stdin, not as an argument, exactly as the library's own Args.Input
//     does it.
//   - Credentials: the API key is placed directly into the child's own
//     environment slice by process.go's childEnv (CODEX_API_KEY, and
//     nowhere else - see TestChildEnvPlacesTheApiKeyOnlyWhereRequired) -
//     never written into this plugin process's own os.Environ, so a core
//     dump, a debugger, or another library called later in this same
//     process cannot find it there. What this plugin controls beyond that
//     is keeping the value out of everything else that could leak it -
//     logs, errors, and this task's own outputs - which is what exec.go's
//     scrubber and errors.go's classifyRunError exist for.
//
// # Why this plugin does not use Exec.Run for the process launch
//
// Every other part of the library is used as designed; the subprocess
// launch itself is this plugin's own (process.go), for one reason: Exec.Run's
// buildEnvironment starts from a copy of *this plugin process's own*
// os.Environ() and only overrides OPENAI_BASE_URL, CODEX_API_KEY, and an
// originator string on top of it. Every other variable this plugin process
// happens to have - including CODEX_HOME, were it ever set on the worker
// for any reason - would pass straight through to the child. The
// coordinator's design addendum named this directly: a subprocess that sees
// whatever its parent's environment happens to contain behaves differently
// depending on the machine it runs on, which is the same ambient-trust
// shape plugins/git's refusal of an ambient ssh-agent guards against, one
// layer further out. process.go's childEnv builds the child's entire
// environment from an explicit allowlist instead - PATH, HOME and
// CODEX_HOME pointed at a fresh per-run directory (see "Codex
// configuration" below), CODEX_API_KEY, and nothing this process did not
// choose to be there - and buildArgs mirrors Exec.Run's own flag-building
// closely enough to read the two side by side, so the only actual behavior
// change is the environment a run's subprocess sees.
//
// # Codex configuration: what this plugin covers, and what it does not
//
// codex's own config.toml surface is large - model provider allowlists, MCP
// servers, approval policies, shell environment policy, and more
// (codex-rs/config/src/config_toml.rs upstream) - and growing. This plugin
// does not attempt to expose or validate all of it; policy.go implements a
// deliberately narrow, three-layer slice covering exactly the two knobs
// this task's own inputs expose:
//
//  1. This plugin's own fail-closed baseline (defaultOperatorPolicy):
//     sandbox READ_ONLY, no network, nothing copied into the ephemeral
//     CODEX_HOME codex reads (see ephemeral.go) - what a deployment gets
//     with no operator configuration at all.
//  2. An operator's own base config, named by FLOWSTATE_CODEX_BASE_CONFIG -
//     a real codex config.toml, or a fragment of one. Only two keys are
//     read out of it (sandbox_mode, sandbox_workspace_write.network_access)
//     to compute the ceiling layer 3 may narrow within; everything else in
//     the file is opaque to this plugin and copied byte-for-byte into the
//     ephemeral CODEX_HOME, so an operator can still pin a model provider
//     allowlist, MCP servers, or anything else codex's config understands
//     without this plugin needing a Go type for every key codex has ever
//     added.
//  3. A task's own sandbox_mode and allow_network inputs, refused outright
//     (never silently downgraded) when they ask for more than layer 2
//     permits - see policy.go's own doc comment on policyEnv for the full
//     three-layer argument, which is the #172/GHA "a workflow can restrict
//     a grant but never widen one past what deployment configuration
//     allows" lesson applied to an agent's sandbox instead of a token's
//     scopes.
//
// This is a deliberate subset, not an oversight, and the rest is named
// rather than silently unsupported: approval_policy, MCP server
// configuration, model provider allowlisting, and shell environment policy
// are all real codex config surface this plugin does not narrow or
// validate today - an operator's FLOWSTATE_CODEX_BASE_CONFIG can still set
// them (they pass through in RawConfig), but no task input reaches them,
// and no ceiling check applies to them. Extending the narrowed set to cover
// one of those is the next slice, not a redesign of this one - policy.go's
// operatorConfigToml is exactly the place a new key gets added, following
// sandbox_mode and network_access as the worked example.
//
// # What still needs live testing
//
// Everything in this plugin below the subprocess boundary is exercised
// against testdata/fakecodex (see helper_test.go), a stand-in that emits
// scripted JSON events and exit codes - real os/exec, a real pipe, real
// exit codes, but not the real codex binary. Nothing here has been run
// against a live codex CLI talking to the real OpenAI API. Specifically
// unverified and worth a deliberate pass before this plugin is trusted in
// production:
//
//   - That `--skip-git-repo-check`, `--cd`, and the `-c
//     sandbox_workspace_write.network_access=...` override actually compose
//     the way codex-rs's own flag/config precedence (CLI flags > `-c`
//     overrides > config.toml > defaults, read from config_toml.rs and
//     config_override.rs) predicts, rather than the way this plugin's
//     reading of that precedence predicts.
//   - That CODEX_HOME pointed at a directory with no auth.json and no
//     config.toml (the default-policy case) does not make codex fall back
//     to an interactive login prompt or a different failure than "use
//     CODEX_API_KEY," since find_codex_home (codex-rs/utils/home-dir)
//     was read for its *path resolution* behavior, not for what codex does
//     once it has that path if the directory turns out to be otherwise
//     empty.
//   - The rate-limit and auth-failure text-match heuristics in
//     errors.go's classifyRunError, against real stderr wording from a
//     real 429 or 401 - this build has only ever seen invented stderr text
//     via fakecodex.
//   - Real token usage numbers, to confirm this task's InputTokens/
//     CachedInputTokens/OutputTokens match what OpenAI actually billed,
//     given the two additional Usage fields upstream added that this
//     plugin does not yet surface (see "Where the two disagree" above).
//
// # Secrets
//
// codex.exec declares api_key in its secret_inputs (see main.go), so a
// Flowfile writes `api_key: ${secret('...')}` and this task's Fn receives
// the resolved value directly - the host resolves the reference before this
// task ever runs (see pkg/flowstate/v1/plugin/task.go's
// resolvePluginSecretInputs), and this plugin process never holds a
// [flowstatev1.SecretRef] or a provider of its own. That is real and worth
// stating plainly: unlike plugins/vcs and plugins/github, which predate this
// mechanism and each stand up their own secret scheme resolved from the
// worker's environment, this plugin needs none of that machinery - it is
// the first task in this repository built entirely against secret_inputs
// rather than around its absence.
//
// What secret_inputs does not do - and what this plugin does on top of it,
// belt over suspenders - is stop a Flowfile author from writing
// `api_key: "sk-literal"` directly: by the time this task's Fn runs, a
// value that started as a literal and a value the host just resolved from a
// reference are the same shape, an already-resolved [flowstatev1.Value]
// with a literal string inside it, and nothing in the wire format lets this
// task tell them apart. Refusing a literal secret is a property this task
// cannot enforce for itself the way plugins/vcs's own tokenFromValue can
// (that plugin resolves its own scheme, and sees the SecretRef before
// resolving it) - it depends on wherever a future validator or policy
// surface checks that an input declared in secret_inputs was actually
// written as `${secret(...)}` in the Flowfile, which does not exist yet.
// Recorded here as the SDK gap the mission asked for, not silently worked
// around.
//
// # Bounds
//
// Every read from the subprocess is bounded below the library where
// possible, the same "the bound belongs on the transport, not on a library
// option" lesson CLAUDE.md draws from connect-go's non-200 unmarshaler gap:
// see exec.go's boundedReader for the byte cap on the child's combined
// stdout, and bounds.go for the event count, output size, and text-field
// caps applied above it. A timeout is not one of this task's own inputs -
// the step's own deadline (ctx) is what bounds how long a run may take,
// exactly as CLAUDE.md's "a step's own timeout: is still the primary
// bound" note describes for plugins/vcs's requestTimeout.
package main
