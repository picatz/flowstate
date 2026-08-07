package secrets

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// Defaults for providers that read a secret from a command's output.
const (
	// DefaultCommandTimeout bounds one invocation. A local secret tool answers in
	// milliseconds; a tool that has not answered in this long is prompting for a
	// password, waiting on a network, or wedged, and a workflow step should not be
	// held open for it.
	DefaultCommandTimeout = 10 * time.Second

	// DefaultCommandMaxBytes bounds how much output is read. A credential is small;
	// the limit is what stops a wrong or hostile command from feeding a worker an
	// unbounded stream.
	DefaultCommandMaxBytes int64 = 1 << 20 // 1 MiB
)

// commandRunner runs one subprocess and returns its standard output.
//
// It is an interface so a provider's logic can be tested without the tool it
// wraps: a test that needs a keychain to run is a test that does not run in CI.
// The real implementation is [execRunner].
type commandRunner interface {
	// run executes name with args and returns its standard output. Errors are
	// already classified against this package's sentinels.
	run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// execRunner runs subprocesses with os/exec, under the constraints a secret tool
// invocation needs.
//
// There is no shell anywhere in this path. The executable and its arguments are
// passed separately, so a secret name containing a space, a quote, a semicolon, or
// a backtick is one argument rather than something a shell would re-interpret.
// That is the property that makes it safe to pass a workflow-supplied name to a
// local tool at all.
type execRunner struct {
	// timeout bounds one invocation, on top of any deadline the caller already has.
	timeout time.Duration

	// maxBytes bounds how much of the output is read.
	maxBytes int64

	// env is the child's environment. Nil inherits the parent's, which is the right
	// default here — a keychain or password-manager CLI needs HOME, the user's
	// session, and its own configuration to work at all.
	env []string
}

// run implements [commandRunner].
func (r execRunner) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	// Resolve the executable explicitly so a missing tool is reported as a missing
	// tool, and so a binary sitting in the working directory is never picked up:
	// LookPath refuses that since Go 1.19, and this surfaces the refusal clearly.
	path, err := exec.LookPath(name)
	if err != nil {
		if errors.Is(err, exec.ErrDot) {
			return nil, fmt.Errorf("%w: refusing to run %q from the current directory", ErrUnavailable, name)
		}
		return nil, fmt.Errorf("%w: %q is not installed or not on PATH", ErrUnavailable, name)
	}

	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = r.env

	// Nothing is written to the child, and leaving stdin attached to the worker's
	// would let a tool that decides to prompt block on a terminal that is not there.
	cmd.Stdin = nil

	// Bound the wait after cancellation, so a tool that ignores its kill signal
	// cannot hold the step open indefinitely.
	cmd.WaitDelay = 2 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{w: &stdout, limit: r.limit()}
	cmd.Stderr = &limitedWriter{w: &stderr, limit: 8 << 10}

	runErr := cmd.Run()

	// A deadline is reported as unavailable rather than as a failure of the tool:
	// it is the one condition here that another attempt might get past.
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: %s did not answer within %s", ErrUnavailable, name, r.timeout)
		}
		return nil, ctxErr
	}

	if errors.Is(runErr, errOutputTooLarge) {
		return nil, fmt.Errorf("%w: %s produced more than %d bytes", ErrTooLarge, name, r.limit())
	}

	if runErr != nil {
		var exit *exec.ExitError
		if errors.As(runErr, &exit) {
			// The tool ran and refused. Its stderr says why, and a secret tool puts
			// the reason there rather than the secret, so it is safe to include —
			// bounded, and with any trailing newline trimmed.
			return nil, fmt.Errorf("%w: %s exited %d: %s",
				ErrNotFound, name, exit.ExitCode(), summarize(stderr.String()))
		}

		return nil, fmt.Errorf("%w: running %s: %v", ErrUnavailable, name, runErr)
	}

	return stdout.Bytes(), nil
}

// limit returns the output cap, substituting the default.
func (r execRunner) limit() int64 {
	if r.maxBytes > 0 {
		return r.maxBytes
	}

	return DefaultCommandMaxBytes
}

// errOutputTooLarge signals that a child wrote past its byte budget.
var errOutputTooLarge = errors.New("output exceeded limit")

// limitedWriter bounds what a child process can make the worker buffer.
//
// It stops the copy rather than truncating quietly, because a truncated credential
// is worse than no credential: it would be presented, refused, and diagnosed as an
// authentication problem rather than as a size problem.
type limitedWriter struct {
	w       io.Writer
	limit   int64
	written int64
}

// Write implements [io.Writer].
func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.written+int64(len(p)) > w.limit {
		return 0, errOutputTooLarge
	}

	n, err := w.w.Write(p)
	w.written += int64(n)

	return n, err
}

// summarize renders a tool's diagnostic output for an error message: one line,
// bounded, and stripped of anything that could forge a log line.
func summarize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "no output"
	}

	s = strings.Map(func(r rune) rune {
		if isControl(r) {
			return ' '
		}
		return r
	}, s)

	if len(s) > 200 {
		s = s[:200] + "…"
	}

	return s
}

// hasCommand reports whether an executable is available, for a constructor that
// should fail on a machine without the tool rather than on the first workflow that
// needs a secret.
func hasCommand(name string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("%q is not installed or not on PATH: %w", name, err)
	}

	return nil
}

// Defaults for the command provider.
const (
	// DefaultCommandScheme is the reference scheme the provider handles.
	DefaultCommandScheme = "command"

	// namePlaceholder is replaced, literally, with the reference's name wherever it
	// appears in a configured argument.
	namePlaceholder = "{{name}}"

	// namespacePlaceholder is replaced, literally, with the resolved namespace
	// segment wherever it appears in a configured argument.
	namespacePlaceholder = "{{namespace}}"
)

// CommandProvider resolves secrets by running a configured external command and
// reading the secret from its standard output. It handles the "command" scheme by
// default.
//
// It exists as the escape hatch for every backend that does not get a provider of
// its own: "aws kms decrypt", "sops -d", "age -d",
// "aws secretsmanager get-secret-value", "doppler run", and anything else that is a
// command line away from a secret value is reachable through this without adding a
// dependency, an auth mode, or a review to this tree. A deployment that needs one of
// those is a deployment for which no in-tree provider is coming; this is the answer
// for all of them at once.
//
// A reference names the secret; the command is fixed at construction, by an
// operator, not by the workflow. The configured argument list may contain the
// literal token "{{name}}", replaced with the reference's name, and "{{namespace}}",
// replaced with the resolved namespace segment. Substitution is a literal string
// replace on one argv element — there is no shell anywhere in this path, so a name
// containing a space, a quote, a semicolon, or a backtick is exactly one argument to
// the child process and never something a shell reinterprets.
//
//	secrets.NewCommandProvider([]string{"sops", "-d", "--extract", `["{{name}}"]`, "/etc/flowstate/secrets.enc.yaml"})
//	secrets.NewCommandProvider([]string{"aws", "secretsmanager", "get-secret-value", "--secret-id", "{{name}}", "--query", "SecretString", "--output", "text"})
//
// The command's exit code and stderr classify the failure the same way the keychain
// and 1Password providers do: a non-zero exit is [ErrNotFound] with a bounded,
// control-character-stripped summary of stderr; a timeout or a missing executable is
// [ErrUnavailable]; empty output is [ErrEmpty].
//
// It is safe for concurrent use.
type CommandProvider struct {
	scheme string
	args   []string
	runner commandRunner

	// namespaced opts this provider into tenancy, off by default for the same
	// reason it is off in every other local provider: a worker must not become
	// multi-tenant because an identity happened to carry a namespace. When on,
	// "{{namespace}}" in a configured argument is replaced with the namespace, or
	// [DefaultNamespaceDir] for the unnamespaced tenant.
	namespaced bool
}

// CommandOption configures a [CommandProvider].
type CommandOption func(*CommandProvider)

// WithCommandNamespaced gives each tenant its own namespace segment, substituted
// wherever a configured argument spells "{{namespace}}".
//
// With it, a run in namespace "team-a" substitutes "team-a", and the unnamespaced
// tenant substitutes [DefaultNamespaceDir] — every tenant gets a segment, including
// the default one, which is what keeps a command that branches on the namespace
// unambiguous.
//
// Without it, a namespaced request is refused rather than run with an empty or
// omitted substitution: a command that was never told to expect a namespace should
// not silently be handed one tenant's identity while reading whatever it was
// configured to read for everyone.
func WithCommandNamespaced() CommandOption {
	return func(p *CommandProvider) {
		p.namespaced = true
	}
}

// WithCommandScheme changes the reference scheme the provider handles, which
// defaults to [DefaultCommandScheme].
//
// A [Registry] holds one provider per scheme, so this is what lets a worker
// configure two different escape-hatch commands at once — "command" for one and,
// say, "kms" for another — each with its own argv template.
func WithCommandScheme(scheme string) CommandOption {
	return func(p *CommandProvider) {
		p.scheme = scheme
	}
}

// withCommandRunner replaces the subprocess runner, for tests.
func withCommandRunner(runner commandRunner) CommandOption {
	return func(p *CommandProvider) {
		p.runner = runner
	}
}

// NewCommandProvider returns a provider that resolves secrets by running args,
// substituting "{{name}}" and, when [WithCommandNamespaced] is set, "{{namespace}}"
// into the configured arguments before each invocation.
//
// args must hold at least the executable; args[0] is resolved on PATH when the
// provider is constructed, so a worker configured for a command it does not have
// refuses to start rather than failing the first workflow that needs a secret.
// args[0] is taken literally and never has a placeholder substituted into it, so it
// must be a real executable name or path.
func NewCommandProvider(args []string, opts ...CommandOption) (*CommandProvider, error) {
	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		return nil, fmt.Errorf("secrets: command provider needs at least an executable to run")
	}

	provider := &CommandProvider{
		scheme: DefaultCommandScheme,
		args:   append([]string(nil), args...),
		runner: execRunner{timeout: DefaultCommandTimeout, maxBytes: DefaultCommandMaxBytes},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(provider)
	}

	if err := ValidateScheme(provider.scheme); err != nil {
		return nil, fmt.Errorf("secrets: command provider: %w", err)
	}

	if _, real := provider.runner.(execRunner); real {
		if err := hasCommand(provider.args[0]); err != nil {
			return nil, fmt.Errorf("secrets: command provider needs %q: %w", provider.args[0], err)
		}
	}

	return provider, nil
}

// Scheme implements [Provider].
func (p *CommandProvider) Scheme() string {
	return p.scheme
}

// Args returns the configured argument template. It is safe to log: it is what an
// operator configured, never a resolved secret.
func (p *CommandProvider) Args() []string {
	return append([]string(nil), p.args...)
}

// Resolve implements [Provider].
func (p *CommandProvider) Resolve(ctx context.Context, req Request) (Secret, error) {
	ref := req.Ref

	name := ref.GetName()
	if err := validateCommandName(name); err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	nsSegment, err := p.namespaceSegment(req.Namespace)
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	argv := make([]string, len(p.args))
	for i, arg := range p.args {
		arg = strings.ReplaceAll(arg, namePlaceholder, name)
		arg = strings.ReplaceAll(arg, namespacePlaceholder, nsSegment)
		argv[i] = arg
	}

	out, err := p.runner.run(ctx, argv[0], argv[1:]...)
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	value := strings.TrimSuffix(string(out), "\n")
	if value == "" {
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: command for %q produced nothing", ErrEmpty, RefString(ref)),
		}
	}

	return NewSecret(ref, value), nil
}

// namespaceSegment returns the namespace substitution for "{{namespace}}", or
// refuses.
//
// The same discipline as [KeychainProvider.serviceFor] and
// [OnePasswordProvider.vaultFor]: refusing a namespaced request on an unnamespaced
// provider says "not configured", never "not found", so a worker that has not been
// told to expect tenants does not quietly start serving them.
func (p *CommandProvider) namespaceSegment(namespace string) (string, error) {
	switch {
	case p.namespaced:
		if namespace == "" {
			return DefaultNamespaceDir, nil
		}
		return namespace, nil

	case namespace != "":
		return "", fmt.Errorf(
			"%w: this worker's command provider is not namespaced, so it cannot resolve secrets "+
				"for namespace %q; configure it with WithCommandNamespaced",
			ErrNamespace, namespace)
	}

	return "", nil
}

// validateCommandName rejects a name the substitution would misread.
//
// A leading dash would be taken as an option wherever "{{name}}" lands as a whole
// argument, which is the one way an argument can change what the command does with
// no shell involved.
func validateCommandName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: command secret name must not be empty", ErrInvalidRef)
	case strings.HasPrefix(name, "-"):
		return fmt.Errorf("%w: command secret name %q may not start with a dash", ErrInvalidRef, name)
	}

	if i := strings.IndexFunc(name, isControl); i >= 0 {
		return fmt.Errorf("%w: command secret name contains a control character at offset %d", ErrInvalidRef, i)
	}

	return nil
}
