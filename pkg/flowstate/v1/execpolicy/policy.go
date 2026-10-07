package execpolicy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"

	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/procgroup"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// Ceilings the policy file cannot raise. A bound an operator cannot remove is
// also one they cannot raise without limit: a file that asks for more is
// refused at load, so the file says what is enforced.
const (
	// MaxTimeout is the longest a policy may allow one program to run.
	MaxTimeout = time.Hour

	// MaxOutputBytes is the most a policy may keep of one output stream.
	//
	// A step's standard output and standard error travel together in one task
	// result, which is measured as ProtoJSON against the bound a durable
	// history can carry (v1.MaxTaskOutputBytes). Both streams at this ceiling,
	// made entirely of the bytes JSON spells longest, must still fit, or a
	// policy would promise output the history cannot hold and the step would
	// fail when it was fullest. TestExecOutputCeilingFitsTaskOutput holds the
	// two together.
	MaxOutputBytes = 128 << 10

	// DefaultRuleCostLimit bounds the CEL evaluation cost of one rule, the same
	// limit the egress and task-shape rules carry and for the same reason.
	DefaultRuleCostLimit = netpolicy.DefaultRuleCostLimit
)

// Bounds on one invocation's inputs, applied again here whatever the schema
// already checked, because a specification that never met the compiler reaches
// the runner too.
const (
	// MaxArgs is the most words in argv.
	MaxArgs = 256

	// MaxArgBytes is the longest single word.
	MaxArgBytes = 8192

	// MaxArgvBytes is the most bytes across argv, kept well under the kernel's
	// argument limit so the refusal is ours and says why.
	MaxArgvBytes = 128 << 10

	// MaxStepEnv is the most environment entries a step may set.
	MaxStepEnv = 64

	// MaxEnvNameBytes and MaxEnvValueBytes bound one environment entry.
	MaxEnvNameBytes  = 128
	MaxEnvValueBytes = 8192
)

// Config describes a policy in Go terms. The operator-facing file is the
// flowstate.v1.ExecPolicy message, which the root package decodes, validates
// and maps onto this; keeping this type free of the schema is what lets the
// package that runs programs stay below the one that defines the schema.
type Config struct {
	// Executables maps the names a workflow may write as argv[0] to absolute
	// paths.
	Executables map[string]string

	// ExecutableSHA256 optionally pins entries of Executables, by name, to the
	// lowercase hex SHA-256 of the file.
	ExecutableSHA256 map[string]string

	// Roots are the absolute directories a step's dir must resolve under. None
	// admits no directory.
	Roots []string

	// EnvPassthrough lists worker variables copied into the child when set.
	EnvPassthrough []string

	// Env holds the operator's literal variables.
	Env map[string]string

	// EnvAuthored lists the names a step may set.
	EnvAuthored []string

	// Timeout is required, positive, and at most [MaxTimeout].
	Timeout time.Duration

	// MaxOutputBytes is required, positive, and at most [MaxOutputBytes]. It
	// bounds each stream separately.
	MaxOutputBytes int64

	// Allow and Deny are CEL rules, deny first. See the package documentation
	// for the variables a rule reads.
	Allow []string
	Deny  []string

	// LookupEnv reads the worker's environment for passthrough. Nil means
	// [os.LookupEnv]; tests supply their own.
	LookupEnv func(string) (string, bool)
}

// executable is one verified table entry.
type executable struct {
	name   string
	path   string // symbolic links resolved
	sha256 string // lowercase hex, or empty when not pinned
}

// Policy is a loaded, ready-to-use exec policy. Build one with [New]. A nil
// *Policy is the unconfigured state and permits nothing.
type Policy struct {
	executables    map[string]executable
	roots          []string // symbolic links resolved
	envPassthrough []string
	env            map[string]string
	envAuthored    map[string]bool
	timeout        time.Duration
	maxOutput      int64
	rules          celrule.Set
	lookupEnv      func(string) (string, bool)
}

// Timeout is the longest one program may run under this policy.
func (p *Policy) Timeout() time.Duration { return p.timeout }

// MaxOutputBytes is the most kept of each output stream.
func (p *Policy) MaxOutputBytes() int64 { return p.maxOutput }

// Names returns the program names the policy lists, sorted.
func (p *Policy) Names() []string {
	if p == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(p.executables))
}

// New verifies cfg and builds the policy. Every failure wraps
// [ErrInvalidPolicy] and names the entry that caused it: a policy that loaded
// partly would govern some programs and not others.
func New(cfg Config) (*Policy, error) {
	p := &Policy{
		executables: make(map[string]executable, len(cfg.Executables)),
		env:         maps.Clone(cfg.Env),
		envAuthored: make(map[string]bool, len(cfg.EnvAuthored)),
		lookupEnv:   cmpLookup(cfg.LookupEnv),
	}

	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalidPolicy, fmt.Sprintf(format, args...))
	}

	if cfg.Timeout <= 0 {
		return nil, bad("timeout is required and must be positive; the time bound cannot be left out of a policy, only chosen (at most %s)", MaxTimeout)
	}
	if cfg.Timeout > MaxTimeout {
		return nil, bad("timeout %s is over the %s ceiling; a longer-running job belongs in a worker the operator runs for it, not in a step", cfg.Timeout, MaxTimeout)
	}
	p.timeout = cfg.Timeout

	if cfg.MaxOutputBytes <= 0 {
		return nil, bad("max_output_bytes is required and must be positive; the output bound cannot be left out of a policy, only chosen (at most 128KiB)")
	}
	if cfg.MaxOutputBytes > MaxOutputBytes {
		return nil, bad("max_output_bytes %d is over the %d-byte ceiling (128KiB)", cfg.MaxOutputBytes, MaxOutputBytes)
	}
	p.maxOutput = cfg.MaxOutputBytes

	for _, name := range slices.Sorted(maps.Keys(cfg.ExecutableSHA256)) {
		if _, ok := cfg.Executables[name]; !ok {
			return nil, bad("executable_sha256 pins %q, which is not in executables; a pin for a program the policy does not list would be silently ignored", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Executables)) {
		exe, err := loadExecutable(name, cfg.Executables[name], cfg.ExecutableSHA256[name])
		if err != nil {
			return nil, bad("%v", err)
		}
		p.executables[name] = exe
	}

	for _, root := range cfg.Roots {
		resolved, err := loadRoot(root)
		if err != nil {
			return nil, bad("%v", err)
		}
		p.roots = append(p.roots, resolved)
	}

	for _, key := range cfg.EnvPassthrough {
		if err := checkEnvName("env_passthrough", key); err != nil {
			return nil, bad("%v", err)
		}
		if isLoaderVar(key) {
			return nil, bad("env_passthrough lists %s, a dynamic-loader variable; passing it through would let whoever controls the worker's environment inject code into every program", key)
		}
	}
	p.envPassthrough = slices.Clone(cfg.EnvPassthrough)

	for _, key := range slices.Sorted(maps.Keys(cfg.Env)) {
		if err := checkEnvName("env", key); err != nil {
			return nil, bad("%v", err)
		}
		if err := checkEnvValue("env", key, cfg.Env[key]); err != nil {
			return nil, bad("%v", err)
		}
	}

	for _, key := range cfg.EnvAuthored {
		if err := checkEnvName("env_authored", key); err != nil {
			return nil, bad("%v", err)
		}
		if isLoaderVar(key) {
			return nil, bad("env_authored lists %s, a dynamic-loader variable; a workflow that could set it could load its own code into every program", key)
		}
		p.envAuthored[key] = true
	}

	rules, err := compileRules(cfg.Allow, cfg.Deny)
	if err != nil {
		return nil, err
	}
	p.rules = rules

	return p, nil
}

func cmpLookup(f func(string) (string, bool)) func(string) (string, bool) {
	if f == nil {
		return os.LookupEnv
	}
	return f
}

// loadExecutable verifies one table entry: the path is absolute, resolves
// through symbolic links to a regular file that is executable and not writable
// by everyone, and matches its pin when it has one.
func loadExecutable(name, path, pin string) (executable, error) {
	if name == "" || strings.ContainsAny(name, "/\x00") || strings.HasPrefix(name, "-") {
		return executable{}, fmt.Errorf("executables: %q is not a bare program name; names have no slash and do not start with a dash", name)
	}
	if !filepath.IsAbs(path) || strings.ContainsRune(path, 0) {
		return executable{}, fmt.Errorf("executables: %s: %q is not an absolute path; the table is the only place a program is located, so it cannot depend on where the worker was started", name, path)
	}

	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return executable{}, fmt.Errorf("executables: %s: %s: %w", name, path, unwrapPathError(err))
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return executable{}, fmt.Errorf("executables: %s: %s: %w", name, path, unwrapPathError(err))
	}
	if err := checkExecutableInfo(info); err != nil {
		return executable{}, fmt.Errorf("executables: %s: %s: %w", name, resolved, err)
	}

	exe := executable{name: name, path: resolved, sha256: pin}
	if pin != "" {
		f, err := openRegular(resolved)
		if err != nil {
			return executable{}, fmt.Errorf("executables: %s: %s: %w", name, resolved, unwrapPathError(err))
		}
		defer f.Close()
		if err := verifyPin(f, pin); err != nil {
			return executable{}, fmt.Errorf("executables: %s: %s: %w", name, resolved, err)
		}
	}

	return exe, nil
}

// checkExecutableMode is the part of [checkExecutableInfo] every platform shares.
func checkExecutableMode(info fs.FileInfo) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("is %s, not a regular file", describeMode(info.Mode()))
	}
	return nil
}

func describeMode(m fs.FileMode) string {
	switch {
	case m.IsDir():
		return "a directory"
	case m&fs.ModeDevice != 0:
		return "a device"
	case m&fs.ModeNamedPipe != 0:
		return "a named pipe"
	case m&fs.ModeSocket != 0:
		return "a socket"
	default:
		return m.String()
	}
}

// verifyPin hashes the file and compares it to the lowercase hex pin.
func verifyPin(f *os.File, pin string) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != pin {
		return fmt.Errorf("sha256 is %s, not the pinned %s", got, pin)
	}
	return nil
}

// loadRoot resolves one configured root to the real directory it names.
func loadRoot(root string) (string, error) {
	if !filepath.IsAbs(root) || strings.ContainsRune(root, 0) {
		return "", fmt.Errorf("roots: %q is not an absolute path", root)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(root))
	if err != nil {
		return "", fmt.Errorf("roots: %s: %w", root, unwrapPathError(err))
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("roots: %s: %w", root, unwrapPathError(err))
	}
	if !info.IsDir() {
		return "", fmt.Errorf("roots: %s is not a directory", root)
	}
	return resolved, nil
}

func unwrapPathError(err error) error {
	if pe, ok := errors.AsType[*fs.PathError](err); ok {
		return pe.Err
	}
	return err
}

// checkEnvName validates a variable name from the policy file.
func checkEnvName(field, key string) error {
	if !validEnvName(key) {
		return fmt.Errorf("%s: %q is not a valid environment variable name (letters, digits and underscore, not starting with a digit)", field, key)
	}
	return nil
}

func checkEnvValue(field, key, value string) error {
	if strings.ContainsRune(value, 0) {
		return fmt.Errorf("%s: %s: the value contains a NUL byte, which an environment cannot carry", field, key)
	}
	if len(value) > MaxEnvValueBytes {
		return fmt.Errorf("%s: %s: the value is %d bytes, over the %d-byte limit", field, key, len(value), MaxEnvValueBytes)
	}
	return nil
}

func validEnvName(key string) bool {
	if key == "" || len(key) > MaxEnvNameBytes {
		return false
	}
	for i := range len(key) {
		c := key[i]
		switch {
		case c == '_', 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z':
		case '0' <= c && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// isLoaderVar reports whether a variable name steers the dynamic loader, which
// turns an environment entry into code the child loads.
func isLoaderVar(key string) bool {
	return strings.HasPrefix(key, "LD_") || strings.HasPrefix(key, "DYLD_")
}

// newRuleEnv builds the CEL environment rules are compiled against. Declaring
// every variable is what makes a misspelled one a load-time error rather than a
// rule that quietly never matches.
func newRuleEnv() (*cel.Env, error) {
	return cel.NewEnv(
		principal.EnvOptions(),
		cel.Variable("argv", cel.ListType(cel.StringType)),
		cel.Variable("executable", cel.StringType),
		cel.Variable("name", cel.StringType),
		cel.Variable("dir", cel.StringType),
		cel.Variable("env_keys", cel.ListType(cel.StringType)),
		principal.Var("identity"),
		ext.Strings(ext.StringsVersion(5)),
	)
}

func compileRules(allow, deny []string) (celrule.Set, error) {
	if len(allow) == 0 && len(deny) == 0 {
		return celrule.Set{}, nil
	}
	env, err := newRuleEnv()
	if err != nil {
		return celrule.Set{}, fmt.Errorf("%w: building the rule environment: %w", ErrInvalidPolicy, err)
	}
	wrap := func(kind celrule.Kind, err error) error {
		return fmt.Errorf("%w: %s %w", ErrInvalidPolicy, kind, err)
	}
	denyRules, err := celrule.CompileAll(env, celrule.Deny, deny, DefaultRuleCostLimit, wrap)
	if err != nil {
		return celrule.Set{}, err
	}
	allowRules, err := celrule.CompileAll(env, celrule.Allow, allow, DefaultRuleCostLimit, wrap)
	if err != nil {
		return celrule.Set{}, err
	}
	return celrule.Set{Allow: allowRules, Deny: denyRules}, nil
}

// Request is one invocation to be checked.
type Request struct {
	// Argv is the command: a program name, then its arguments.
	Argv []string

	// Dir is the requested working directory.
	Dir string

	// Env is the step's environment request.
	Env map[string]string

	// Identity is the run's attested identity as a rule reads it; the zero
	// value is "no attested caller".
	Identity principal.Caller
}

// Command is an invocation the policy admitted: every value in it is the
// resolved one a rule saw. Run it with [Command.Run].
type Command struct {
	policy *Policy
	argv   []string
	exe    executable
	dir    string
	env    []string // KEY=VALUE, sorted by key
	keys   []string // sorted
}

// Executable is the resolved absolute path that will run.
func (c *Command) Executable() string { return c.exe.path }

// Dir is the resolved working directory.
func (c *Command) Dir() string { return c.dir }

// EnvKeys is the sorted names of the final environment. Never its values.
func (c *Command) EnvKeys() []string { return slices.Clone(c.keys) }

// processGroupsEnforced is whether this platform can stop a program together
// with its descendants. A variable only so a test can stand in for a platform
// that cannot.
var processGroupsEnforced = procgroup.Supported

// Check applies the policy to one invocation and returns the command to run
// or a [*DeniedError]. A nil policy denies everything with [ReasonNoPolicy].
//
// The structural checks run before the operator's rules so that a rule reads
// the final, resolved values: the executable path rather than the name alone,
// the directory after symbolic links, the environment as it will be.
func (p *Policy) Check(ctx context.Context, req Request) (*Command, error) {
	if p == nil {
		return nil, &DeniedError{Reason: ReasonNoPolicy, Detail: "no exec policy is configured"}
	}
	// Fail closed where the process-group guarantee cannot be enforced: a step
	// that ends must not leave descendants running, and on such a platform a
	// descendant could outlive it. Refusing is the only honest answer; a
	// weaker run under the same policy would be a silent downgrade.
	if !processGroupsEnforced {
		return nil, &DeniedError{
			Reason: ReasonPlatform,
			Detail: "the exec task is not available on " + runtime.GOOS + ": this platform cannot stop a program " +
				"together with the processes it starts, so a step could leave descendants running after it ended",
		}
	}

	exe, err := p.checkArgv(req.Argv)
	if err != nil {
		return nil, err
	}
	dir, err := p.checkDir(req.Dir)
	if err != nil {
		return nil, err
	}
	env, keys, err := p.buildEnv(req.Env)
	if err != nil {
		return nil, err
	}

	cmd := &Command{policy: p, argv: slices.Clone(req.Argv), exe: exe, dir: dir, env: env, keys: keys}

	if !p.rules.Empty() {
		identity := req.Identity.Normalized()
		decision, err := p.rules.Decide(ctx, map[string]any{
			"argv":       cmd.argv,
			"executable": exe.path,
			"name":       req.Argv[0],
			"dir":        dir,
			"env_keys":   keys,
			"identity":   identity,
		})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, &DeniedError{Reason: ReasonRuleError, Detail: err.Error(), Err: errors.Unwrap(err)}
		}
		switch decision.Verdict {
		case celrule.DeniedByRule:
			return nil, &DeniedError{Reason: ReasonDenyRule, Detail: decision.Rule.Source()}
		case celrule.NoAllowRuleMatched:
			return nil, &DeniedError{Reason: ReasonNoAllowRule, Detail: "no allow rule matched"}
		}
	}

	return cmd, nil
}

func (p *Policy) checkArgv(argv []string) (executable, error) {
	deny := func(format string, args ...any) (executable, error) {
		return executable{}, &DeniedError{Reason: ReasonArgv, Detail: fmt.Sprintf(format, args...)}
	}

	if len(argv) == 0 {
		return deny("argv is empty; name a program first")
	}
	if len(argv) > MaxArgs {
		return deny("argv has %d words, over the limit of %d", len(argv), MaxArgs)
	}
	total := 0
	for i, word := range argv {
		if strings.ContainsRune(word, 0) {
			return deny("argv[%d] contains a NUL byte, which no operating system can pass to a program", i)
		}
		if len(word) > MaxArgBytes {
			return deny("argv[%d] is %d bytes, over the limit of %d", i, len(word), MaxArgBytes)
		}
		total += len(word) + 1
	}
	if total > MaxArgvBytes {
		return deny("argv totals %d bytes, over the limit of %d", total, MaxArgvBytes)
	}

	name := argv[0]
	if strings.ContainsAny(name, `/\`) {
		return executable{}, &DeniedError{Reason: ReasonExecutable, Detail: fmt.Sprintf(
			"argv[0] %q is a path; a workflow names a program the policy lists and never locates one (%s)",
			name, p.known())}
	}
	exe, ok := p.executables[name]
	if !ok {
		return executable{}, &DeniedError{Reason: ReasonExecutable, Detail: fmt.Sprintf(
			"argv[0] %q is not a program this policy lists (%s)", name, p.known())}
	}

	return exe, nil
}

// known renders the table's names for a denial, bounded.
func (p *Policy) known() string {
	names := p.Names()
	if len(names) == 0 {
		return "the policy lists no programs"
	}
	const show = 16
	if len(names) > show {
		return fmt.Sprintf("it lists %s and %d more", strings.Join(names[:show], ", "), len(names)-show)
	}
	return "it lists " + strings.Join(names, ", ")
}

// checkDir resolves the requested directory and requires it to fall under a
// configured root, component by component.
func (p *Policy) checkDir(dir string) (string, error) {
	deny := func(format string, args ...any) (string, error) {
		return "", &DeniedError{Reason: ReasonDir, Detail: fmt.Sprintf(format, args...)}
	}

	if dir == "" {
		return deny("dir is required; the worker's own working directory is never used")
	}
	if strings.ContainsRune(dir, 0) {
		return deny("dir contains a NUL byte")
	}
	if !filepath.IsAbs(dir) {
		return deny("dir %q is not an absolute path", dir)
	}
	if len(p.roots) == 0 {
		return deny("the policy configures no roots, so no directory is permitted")
	}

	resolved, err := filepath.EvalSymlinks(filepath.Clean(dir))
	if err != nil {
		return deny("dir %q cannot be resolved: %v", dir, unwrapPathError(err))
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return deny("dir %q cannot be read: %v", dir, unwrapPathError(err))
	}
	if !info.IsDir() {
		return deny("dir %q is not a directory", dir)
	}

	for _, root := range p.roots {
		if within(root, resolved) {
			return resolved, nil
		}
	}

	// The resolved path is deliberately not named: it is where a symbolic link
	// leads, which is the worker's filesystem layout, and a denial is written
	// into a run's durable history for anyone who can read the run.
	return deny("dir %q is not under any root the policy configures", dir)
}

// within reports whether target is root or lies beneath it, comparing path
// components so that /srv/work-evil is not under /srv/work.
func within(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// buildEnv assembles the child's environment from nothing: the operator's
// literals, then passthrough variables that are set, then the step's entries
// the policy lets it set. It returns the KEY=VALUE slice and the sorted keys.
func (p *Policy) buildEnv(step map[string]string) ([]string, []string, error) {
	final := make(map[string]string, len(p.env)+len(p.envPassthrough)+len(step))
	maps.Copy(final, p.env)

	for _, key := range p.envPassthrough {
		if _, set := final[key]; set {
			continue
		}
		if value, ok := p.lookupEnv(key); ok {
			final[key] = value
		}
	}

	if len(step) > MaxStepEnv {
		return nil, nil, &DeniedError{Reason: ReasonEnv, Detail: fmt.Sprintf("env has %d entries, over the limit of %d", len(step), MaxStepEnv)}
	}
	for _, key := range slices.Sorted(maps.Keys(step)) {
		deny := func(format string, args ...any) error {
			return &DeniedError{Reason: ReasonEnv, Detail: fmt.Sprintf(format, args...)}
		}
		if !validEnvName(key) {
			return nil, nil, deny("%q is not a valid environment variable name", key)
		}
		if !p.envAuthored[key] {
			return nil, nil, deny("%s is not a variable the policy lets a step set (env_authored)", key)
		}
		if _, set := final[key]; set {
			return nil, nil, deny("%s is set by the operator's policy and a step cannot override it", key)
		}
		if err := checkEnvValue("env", key, step[key]); err != nil {
			return nil, nil, deny("%v", err)
		}
		final[key] = step[key]
	}

	keys := slices.Sorted(maps.Keys(final))
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+final[key])
	}

	return env, keys, nil
}
