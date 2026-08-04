package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// maxStderrBytes bounds the codex CLI's stderr, captured only to fold into
// an error message when the process exits abnormally - never returned on
// its own, and never as large as maxSubprocessBytes bounds stdout, since
// stderr from a CLI tool is diagnostic text, not the run's actual output.
const maxStderrBytes = 64 << 10 // 64 KiB

// codexProcess is this plugin's own subprocess launch, replacing
// github.com/picatz/openai/codex's Exec.Run for the one thing that library
// does not give a caller control over: the child's environment.
//
// # Why this plugin does not use the library's own Exec.Run
//
// Exec.Run's buildEnvironment (in the library's exec.go) starts from
// os.Environ() - a copy of *this plugin process's own* environment - and
// only overrides OPENAI_BASE_URL, CODEX_API_KEY, and
// CODEX_INTERNAL_ORIGINATOR_OVERRIDE on top of it. Every other variable the
// worker process happens to have - including CODEX_HOME, were it ever set
// on the worker for any reason - passes straight through to the child. That
// is the ambient-inheritance problem the coordinator's design addendum
// named directly: a plugin that lets its subprocess see whatever the
// worker's own process environment happens to contain is not meaningfully
// different from a plugin that lets the subprocess read the worker user's
// dotfiles, since either way the run's behavior depends on the machine it
// happened to execute on rather than on this call's own inputs.
//
// This type builds argv the same way the library's Exec.Run does (see
// buildArgs below, which mirrors that function's flag mapping so the two
// stay obviously equivalent to read side by side) but constructs cmd.Env
// from an explicit allowlist (see childEnv) rather than a copy of anything.
// codex.ThreadEvent and the item types the library decodes events into are
// still used for everything downstream of the subprocess actually running -
// see exec.go's readRun - only the process launch itself is
// this plugin's own.
type codexProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser

	stderr   *boundedWriter
	waitOnce sync.Once
	waitErr  error
}

// buildArgs renders one run's flags, mirroring
// github.com/picatz/openai/codex's own Exec.Run flag mapping (exec.go in
// that module) for the subset of Args this plugin uses, plus this plugin's
// own -c override for sandbox_workspace_write.network_access - see
// policy.go for why that one flows through a `-c` override rather than a
// dedicated flag: upstream has no dedicated flag for it (see doc.go,
// "Codex configuration").
func buildArgs(model, sandboxArg, workDir string, allowNetwork bool, mutatingSandbox bool) []string {
	args := []string{"exec", "--json"}

	if model != "" {
		args = append(args, "--model", model)
	}

	args = append(args, "--sandbox", sandboxArg)

	if workDir != "" {
		args = append(args, "--cd", workDir)
	}

	// This plugin's own containment - an explicit binary path, a jailed
	// working directory, sandbox_mode defaulting closed, an ephemeral
	// CODEX_HOME - is what a run is protected by; codex's own additional
	// safety check that its working directory sits inside a git repository
	// is not a containment boundary this plugin depends on, and requiring
	// it would refuse a working_context that is deliberately not a repo.
	args = append(args, "--skip-git-repo-check")

	if mutatingSandbox {
		args = append(args, "-c", fmt.Sprintf("sandbox_workspace_write.network_access=%t", allowNetwork))
	}

	return args
}

// fakeCodexEnvPrefix is the one exception to childEnv's allowlist: this
// plugin's own tests drive testdata/fakecodex (a stand-in for the real
// codex CLI - see helper_test.go) entirely through environment variables
// under this prefix, and a subprocess launched with none of the calling
// process's own environment has no other channel to receive them through.
// Passing these specific names is not the ambient-inheritance hole
// childEnv otherwise exists to close: they carry no operator or worker
// secret, they are a fixed and narrow set this plugin's own source defines
// and reads (never anything an operator's deployment environment happens
// to contain), and they mean nothing to the real codex binary, which never
// looks for them.
const fakeCodexEnvPrefix = "FAKECODEX_"

// childEnv builds the subprocess's entire environment from an explicit
// allowlist plus the fakeCodexEnvPrefix exception above - never
// os.Environ() wholesale, and never this process's own env with entries
// merely added or removed. See codexProcess's own doc comment for why.
func childEnv(apiKey, codexHome string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + codexHome,
		"CODEX_HOME=" + codexHome,
		// Matches github.com/picatz/openai/codex's own default value for
		// this variable (goSDKOriginator in that module's exec.go), renamed
		// to identify this plugin specifically rather than the Go SDK in
		// general - useful to an operator reading OpenAI-side telemetry for
		// where a request actually came from.
		"CODEX_INTERNAL_ORIGINATOR_OVERRIDE=flowstate_plugin_codex",
	}
	if apiKey != "" {
		env = append(env, "CODEX_API_KEY="+apiKey)
	}

	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, fakeCodexEnvPrefix) {
			env = append(env, kv)
		}
	}

	return env
}

// startCodexProcess launches the codex subprocess, writes prompt to its
// stdin, and returns a handle whose Stdout can be read for the JSON event
// stream and whose Wait reports the terminal result.
func startCodexProcess(ctx context.Context, binPath, workDir, fallbackDir string, argv, env []string, prompt string) (*codexProcess, error) {
	cmd := exec.CommandContext(ctx, binPath, argv...)
	cmd.Env = env

	// Never inherited. An empty cmd.Dir means "this process's own directory",
	// which the host sets to the private plugin socket directory - so leaving
	// it empty would put host-managed files in the child's reach, whatever
	// --cd says. A writable run always has a workDir by the time it gets here
	// (exec.go refuses one without); a read-only run without one is pointed at
	// fallbackDir, this run's own ephemeral CODEX_HOME, which is torn down
	// with the run.
	cmd.Dir = workDir
	if cmd.Dir == "" {
		cmd.Dir = fallbackDir
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("open stdout pipe: %w", err)
	}

	stderr := &boundedWriter{max: maxStderrBytes}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start codex exec: %w", err)
	}

	go func() {
		defer stdin.Close()
		io.WriteString(stdin, prompt)
	}()

	return &codexProcess{cmd: cmd, stdout: stdout, stderr: stderr}, nil
}

// Wait blocks until the subprocess exits, returning a terminal error that
// - unlike the shape github.com/picatz/openai/codex's own Exec.Run produces
// on this same path - always keeps *exec.ExitError reachable with
// errors.As. That library's waitFn (exec.go in that module) builds its
// abnormal-exit message with `fmt.Errorf("codex exec failed: %s: %s",
// exitErr, stderrText)` - %s, not %w - whenever stderr is non-empty, which
// is the ordinary case for a failing CLI tool; only its empty-stderr
// fallback actually wraps. errors.go's classifyRunError depends on
// errors.As(err, &exitErr) to tell "the process ran and exited badly" apart
// from every other failure shape, so silently losing that type the moment
// stderr has anything in it would misclassify most real subprocess
// failures. This function always wraps with %w so that gap does not carry
// over into this plugin's own error path.
// Kill stops the subprocess without waiting for it to finish on its own.
//
// Called when this plugin has stopped reading the child's stdout — the
// output bound was reached — because at that moment the child is one
// full pipe buffer away from blocking forever on its next write, and
// [codexProcess.Wait] cannot return until it exits. Without this, a bound
// meant to stop a chatty run instead holds the activity open until the
// whole run timeout expires: the opposite of what bounding it was for.
//
// Safe to call more than once and safe to call on a process that has
// already exited; a kill that finds nothing to kill is not an error worth
// reporting, since the only thing this needs to guarantee is that nothing
// is left running.
func (p *codexProcess) Kill() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
}

func (p *codexProcess) Wait() error {
	p.waitOnce.Do(func() {
		err := p.cmd.Wait()
		if err == nil {
			return
		}

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderrText := strings.TrimSpace(string(p.stderr.buf))
			if stderrText != "" {
				p.waitErr = fmt.Errorf("codex exec failed: %w: %s", exitErr, stderrText)
				return
			}
			p.waitErr = fmt.Errorf("codex exec failed: %w", exitErr)
			return
		}

		p.waitErr = err
	})
	return p.waitErr
}
