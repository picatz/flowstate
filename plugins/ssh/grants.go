package main

import (
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/picatz/flowstate/internal/strictyaml"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// grantsEnv names the file an operator writes this plugin's authority in.
//
// It reaches this process through the worker's own per-plugin environment -
// `flow worker --plugin-env ssh=FLOWSTATE_SSH_GRANTS=/etc/flowstate/ssh.yaml`,
// or the --plugin-env-file form - because a plugin inherits nothing of the
// worker's environment.
//
// Unset means this plugin has no authority at all: ssh.run refuses every call
// naming the variable. That is the fail-closed direction and the only safe one,
// since the alternative default - every host, every command - is precisely the
// thing this plugin exists not to be.
const grantsEnv = "FLOWSTATE_SSH_GRANTS"

// maxGrantsBytes bounds the operator's own file. Large for a document a person
// writes, and still a bound on what this process reads into memory.
const maxGrantsBytes = 1 << 20

// Ceilings the operator's file may narrow within but never raise past. A grant
// is authority an operator delegates; these are what this plugin is willing to
// spend on one call regardless of what the file says.
const (
	maxCommandTimeout  = 10 * time.Minute
	maxConnectTimeout  = 2 * time.Minute
	maxOutputByteLimit = 1 << 20

	defaultCommandTimeout = 60 * time.Second
	defaultConnectTimeout = 15 * time.Second
	defaultOutputLimit    = 64 << 10
	defaultSSHPort        = "22"
)

// grants is the operator's whole authority statement.
type grants struct {
	// Hosts are the machines this worker may reach, by grant name.
	Hosts map[string]hostGrant `json:"hosts" yaml:"hosts"`

	// Commands are the programs it may run, by grant name. A command is
	// separate from a host so that one argv can be granted on many hosts
	// without being written many times, and so that reviewing "what may run
	// here" and "where may this run" are two readable lists.
	Commands map[string]commandGrant `json:"commands" yaml:"commands"`
}

// hostGrant is one machine, and what may be done on it.
type hostGrant struct {
	// Address is host or host:port. The port defaults to 22.
	Address string `json:"address" yaml:"address"`

	// User is the account to authenticate as.
	User string `json:"user" yaml:"user"`

	// IdentityFile is the private key the worker authenticates with, as a path
	// this process can read. A key is named here rather than supplied by a
	// workflow because an SSH identity is authority over a machine, not a
	// per-call credential.
	IdentityFile string `json:"identity_file" yaml:"identity_file"`

	// IdentityPassphraseFile, when set, is a file holding the passphrase the
	// key is encrypted with. A separate file so the key can live where keys
	// live and the passphrase where secrets live.
	IdentityPassphraseFile string `json:"identity_passphrase_file" yaml:"identity_passphrase_file"`

	// HostKeys are the public keys, in authorized_keys format, this host may
	// present. There is no trust-on-first-use and no known_hosts fallback: a
	// host presenting anything else is refused before authentication.
	HostKeys []string `json:"host_keys" yaml:"host_keys"`

	// Commands are the command grants this host permits. A command grant that
	// exists and is not listed here cannot be run here.
	Commands []string `json:"commands" yaml:"commands"`

	// Namespaces, when non-empty, are the tenant namespaces whose workflows may
	// spend this grant. The namespace compared is the one the host established
	// for the calling workload, never one the workload declared.
	Namespaces []string `json:"namespaces" yaml:"namespaces"`

	// ConnectTimeout bounds establishing the connection. Zero takes the
	// default; a value over the ceiling is refused when the file is read.
	ConnectTimeout Duration `json:"connect_timeout" yaml:"connect_timeout"`
}

// commandGrant is one program, and the shape of what may be passed to it.
type commandGrant struct {
	// Argv is the command and its arguments, already split. An element may hold
	// ${name} placeholders, which a call fills. There is no shell: the elements
	// are quoted individually and joined, so an argument is one argument.
	Argv []string `json:"argv" yaml:"argv"`

	// Parameters declares each placeholder and the pattern its value must
	// match. A placeholder with no declaration is a configuration error, caught
	// when the file is read rather than when a workflow fills it.
	Parameters map[string]parameterGrant `json:"parameters" yaml:"parameters"`

	// Timeout bounds the command. Zero takes the default; over the ceiling is
	// refused when the file is read.
	Timeout Duration `json:"timeout" yaml:"timeout"`

	// MaxOutputBytes bounds each of stdout and stderr. Zero takes the default.
	MaxOutputBytes int64 `json:"max_output_bytes" yaml:"max_output_bytes"`

	// SuccessExitCodes are the statuses that count as success. Empty means
	// exactly zero. It is the operator's decision rather than a task input
	// because "is a non-zero status a failure here" is a fact about the command,
	// which the person who granted the command knows.
	SuccessExitCodes []int32 `json:"success_exit_codes" yaml:"success_exit_codes"`
}

// parameterGrant is what one placeholder will accept.
type parameterGrant struct {
	// Pattern is a regular expression the whole value must match. It is
	// required: a parameter with no pattern is an unconstrained value in a
	// command line, which is the thing this plugin exists not to have.
	Pattern string `json:"pattern" yaml:"pattern"`

	// MaxBytes bounds the value. Zero takes 256, which is longer than any
	// service name, path component or identifier a runbook passes.
	MaxBytes int `json:"max_bytes" yaml:"max_bytes"`

	// compiled is the parsed Pattern, built once when the file is read.
	compiled *regexp.Regexp
}

// Duration is a YAML duration - "30s", "5m" - so an operator writes what they
// mean rather than a number of nanoseconds.
type Duration time.Duration

// UnmarshalText parses the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("%q is not a duration such as 30s or 5m: %w", string(text), err)
	}
	*d = Duration(parsed)
	return nil
}

// duration renders it, applying a default for zero.
func (d Duration) duration(fallback time.Duration) time.Duration {
	if d == 0 {
		return fallback
	}
	return time.Duration(d)
}

// loadGrants reads and checks the operator's file.
//
// Everything checkable is checked here, at startup, rather than when a workflow
// first names a grant: a file naming a command no host permits, a placeholder
// with no pattern, or a timeout over the ceiling is a mistake an operator should
// learn about from the worker's own logs, not from a runbook failing at three in
// the morning.
func loadGrants() (*grants, error) {
	path := os.Getenv(grantsEnv)
	if path == "" {
		return nil, fmt.Errorf(
			"%s is not set, so this plugin has no hosts and no commands. An operator grants them with "+
				"`flow worker --plugin-env ssh=%s=/path/to/grants.yaml`", grantsEnv, grantsEnv)
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s (%q): %w", grantsEnv, truncate(path, 256), err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s (%q) is a directory, not a grants file", grantsEnv, truncate(path, 256))
	}
	if info.Size() > maxGrantsBytes {
		return nil, fmt.Errorf("%s (%q) is %d bytes, over the %d-byte limit this plugin reads",
			grantsEnv, truncate(path, 256), info.Size(), maxGrantsBytes)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s (%q): %w", grantsEnv, truncate(path, 256), err)
	}

	var parsed grants
	if err := strictyaml.UnmarshalStrict(raw, &parsed); err != nil {
		return nil, fmt.Errorf("%s (%q): %w", grantsEnv, truncate(path, 256), err)
	}
	if err := parsed.check(); err != nil {
		return nil, fmt.Errorf("%s (%q): %w", grantsEnv, truncate(path, 256), err)
	}
	return &parsed, nil
}

// check validates the whole file and compiles what it can.
func (g *grants) check() error {
	if len(g.Hosts) == 0 {
		return fmt.Errorf("no hosts are granted, so no call could succeed")
	}

	for _, name := range slices.Sorted(maps.Keys(g.Commands)) {
		command := g.Commands[name]
		if err := command.check(name); err != nil {
			return err
		}
		g.Commands[name] = command
	}

	for _, name := range slices.Sorted(maps.Keys(g.Hosts)) {
		host := g.Hosts[name]
		if err := host.check(name, g.Commands); err != nil {
			return err
		}
		g.Hosts[name] = host
	}
	return nil
}

// check validates one command grant and compiles its patterns.
func (c *commandGrant) check(name string) error {
	if len(c.Argv) == 0 {
		return fmt.Errorf("command %q has no argv", name)
	}
	if !strings.HasPrefix(c.Argv[0], "/") {
		// An absolute path, so which program runs does not depend on the remote
		// account's PATH - the same reason plugins/codex refuses to search
		// $PATH for the binary it execs.
		return fmt.Errorf("command %q runs %q, which is not an absolute path; a program found on the remote PATH is a program the operator did not choose",
			name, truncate(c.Argv[0], 128))
	}
	if time.Duration(c.Timeout) < 0 || c.Timeout.duration(defaultCommandTimeout) > maxCommandTimeout {
		return fmt.Errorf("command %q has a timeout over this plugin's ceiling of %s", name, maxCommandTimeout)
	}
	if c.MaxOutputBytes < 0 || c.outputLimit() > maxOutputByteLimit {
		return fmt.Errorf("command %q has max_output_bytes over this plugin's ceiling of %d", name, maxOutputByteLimit)
	}

	declared := make(map[string]bool, len(c.Parameters))
	for parameter, grant := range c.Parameters {
		if !parameterNamePattern.MatchString(parameter) {
			return fmt.Errorf("command %q declares the parameter %q, which is not a name: letters, digits and underscores",
				name, truncate(parameter, 64))
		}
		if grant.Pattern == "" {
			return fmt.Errorf("command %q declares the parameter %q with no pattern; an unconstrained value in a command line is what this plugin exists not to have",
				name, parameter)
		}
		compiled, err := regexp.Compile("^(?:" + grant.Pattern + ")$")
		if err != nil {
			return fmt.Errorf("command %q parameter %q: pattern does not compile: %w", name, parameter, err)
		}
		if grant.MaxBytes < 0 {
			return fmt.Errorf("command %q parameter %q: max_bytes is negative", name, parameter)
		}
		grant.compiled = compiled
		c.Parameters[parameter] = grant
		declared[parameter] = true
	}

	// Every placeholder in the argv has to be declared, and every declaration
	// has to be used. The first would be an unconstrained value; the second is
	// a pattern an operator believes is protecting something.
	for _, argument := range c.Argv {
		for _, placeholder := range placeholderPattern.FindAllStringSubmatch(argument, -1) {
			if !declared[placeholder[1]] {
				return fmt.Errorf("command %q uses ${%s} and declares no parameter by that name", name, truncate(placeholder[1], 64))
			}
			delete(declared, placeholder[1])
		}
	}
	for parameter := range declared {
		return fmt.Errorf("command %q declares the parameter %q and never uses it", name, parameter)
	}
	return nil
}

// outputLimit is the per-stream bound this command's output is read under.
func (c commandGrant) outputLimit() int64 {
	if c.MaxOutputBytes == 0 {
		return defaultOutputLimit
	}
	return c.MaxOutputBytes
}

// successful reports whether an exit status counts as success here.
func (c commandGrant) successful(code int32) bool {
	if len(c.SuccessExitCodes) == 0 {
		return code == 0
	}
	return slices.Contains(c.SuccessExitCodes, code)
}

// check validates one host grant against the commands that exist.
func (h *hostGrant) check(name string, commands map[string]commandGrant) error {
	if h.Address == "" {
		return fmt.Errorf("host %q has no address", name)
	}
	if strings.ContainsAny(h.Address, " \t\r\n") {
		return fmt.Errorf("host %q has an address holding whitespace", name)
	}
	if h.User == "" {
		return fmt.Errorf("host %q names no user to connect as", name)
	}
	if h.IdentityFile == "" {
		return fmt.Errorf("host %q names no identity_file; this plugin authenticates with a key the operator names, and a workflow cannot supply one", name)
	}
	if len(h.HostKeys) == 0 {
		return fmt.Errorf("host %q pins no host_keys; there is no trust-on-first-use here, so a host with no pinned key can never be verified", name)
	}
	if len(h.Commands) == 0 {
		return fmt.Errorf("host %q permits no commands, so no call could succeed", name)
	}
	for _, command := range h.Commands {
		if _, ok := commands[command]; !ok {
			return fmt.Errorf("host %q permits the command %q, which is not granted anywhere in this file", name, truncate(command, 64))
		}
	}
	if time.Duration(h.ConnectTimeout) < 0 || h.ConnectTimeout.duration(defaultConnectTimeout) > maxConnectTimeout {
		return fmt.Errorf("host %q has a connect_timeout over this plugin's ceiling of %s", name, maxConnectTimeout)
	}
	return nil
}

// permits reports whether this host grant allows a command grant.
func (h hostGrant) permits(command string) bool {
	return slices.Contains(h.Commands, command)
}

// reachableFrom reports whether a caller's namespace may spend this grant. An
// empty list is every namespace, which is what a single-tenant deployment has.
func (h hostGrant) reachableFrom(namespace string) bool {
	if len(h.Namespaces) == 0 {
		return true
	}
	return slices.Contains(h.Namespaces, namespace)
}

// dialAddress is the address with the default port applied.
func (h hostGrant) dialAddress() string {
	if strings.Contains(h.Address, ":") {
		return h.Address
	}
	return h.Address + ":" + defaultSSHPort
}

var (
	// placeholderPattern finds ${name} in an argv element.
	placeholderPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

	// parameterNamePattern is what a parameter may be called.
	parameterNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// truncate bounds a value before it is interpolated into a message.
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}

// classifyGrantRefusal turns a grants-file problem into a task failure. A
// misconfigured grant is not a transient condition: the same file fails the same
// way next time.
func classifyGrantRefusal(err error) error {
	return sdk.Failed("%s", err)
}

// grantNames renders the grant names in a file, sorted and bounded, so a
// refusal tells an author what they could have written.
func grantNames[V any](m map[string]V) string {
	return joinNames(slices.Sorted(maps.Keys(m)))
}

// joinNames renders a list of grant names, bounded.
func joinNames(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	if len(names) > 20 {
		names = names[:20]
	}
	return truncate(strings.Join(names, ", "), 512)
}
