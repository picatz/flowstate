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
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// grantsEnv names the file an operator writes this plugin's authority in. It
// reaches this process through the worker's per-plugin environment:
//
//	flow worker --plugin-env docker=FLOWSTATE_DOCKER_GRANTS=/etc/flowstate/docker.yaml
//
// Unset means no daemon, no image and no run: every call is refused naming the
// variable, which is the only safe default for a plugin whose permissive
// alternative is "any image, any mount, any network".
const grantsEnv = "FLOWSTATE_DOCKER_GRANTS"

// maxGrantsBytes bounds the operator's own file.
const maxGrantsBytes = 1 << 20

// The ceilings a grant may narrow within and never raise past. They are this
// plugin's own: an operator delegates authority with a grants file, and these
// are what one call may spend whatever the file says.
const (
	maxRunTimeout      = 30 * time.Minute
	maxOutputByteLimit = 4 << 20
	maxMemoryBytes     = 16 << 30
	maxNanoCPUs        = 8_000_000_000 // eight cores, in the daemon's units
	maxPidsLimit       = 4096
	maxMounts          = 8
	maxEnvironment     = 32

	defaultRunTimeout  = 5 * time.Minute
	defaultOutputLimit = 256 << 10
	defaultCleanup     = 30 * time.Second
)

// grants is the operator's whole authority statement.
type grants struct {
	// Daemon is how to reach the container runtime.
	Daemon daemonGrant `json:"daemon" yaml:"daemon"`

	// Mounts are host paths, by grant name. A run grant names these; a workflow
	// never names a path, which is the difference between a mount grant and a
	// mount input.
	Mounts map[string]mountGrant `json:"mounts,omitempty" yaml:"mounts,omitempty"`

	// Runs are what may be executed, by grant name.
	Runs map[string]runGrant `json:"runs" yaml:"runs"`
}

// daemonGrant is the runtime this plugin talks to.
type daemonGrant struct {
	// Socket is a Unix socket path - the ordinary local daemon. Exactly one of
	// Socket and Address is set.
	Socket string `json:"socket,omitempty" yaml:"socket,omitempty"`

	// Address is a remote daemon as host:port, reached over TLS with the client
	// certificate below. A remote daemon is still ambient authority; what it
	// buys is that the authority is not this host's.
	Address string `json:"address,omitempty" yaml:"address,omitempty"`

	// TLSCAFile, TLSCertFile and TLSKeyFile are the material for Address. All
	// three are required with it: an unverified connection to a daemon is a
	// connection to whoever answered.
	TLSCAFile   string `json:"tls_ca_file,omitempty" yaml:"tls_ca_file,omitempty"`
	TLSCertFile string `json:"tls_cert_file,omitempty" yaml:"tls_cert_file,omitempty"`
	TLSKeyFile  string `json:"tls_key_file,omitempty" yaml:"tls_key_file,omitempty"`

	// APIVersion is the Engine API version to address, such as "v1.43". It is
	// named rather than negotiated so that an upgraded daemon does not silently
	// change what this plugin's requests mean.
	APIVersion string `json:"api_version,omitempty" yaml:"api_version,omitempty"`
}

// mountGrant is one host path an operator is willing to expose.
type mountGrant struct {
	// Source is the host path. It must be absolute.
	Source string `json:"source" yaml:"source"`

	// Target is where it appears inside the container, absolute.
	Target string `json:"target" yaml:"target"`

	// Writable makes the mount read-write. The default is read-only, so a grant
	// that can change the host says so in as many words.
	Writable bool `json:"writable,omitempty" yaml:"writable,omitempty"`
}

// runGrant is one thing that may be run.
type runGrant struct {
	// Image is a canonical digest-pinned reference: registry/repository@sha256:…
	// A tag is refused when the file is read.
	Image string `json:"image" yaml:"image"`

	// Argv is the command, already split, with optional ${name} placeholders.
	// Empty runs the image's own entrypoint and command.
	Argv []string `json:"argv,omitempty" yaml:"argv,omitempty"`

	// Parameters declares each placeholder and the pattern its value must match.
	Parameters map[string]parameterGrant `json:"parameters,omitempty" yaml:"parameters,omitempty"`

	// Env is the environment the container sees, assembled from nothing. The
	// worker's own environment never reaches a container.
	Env map[string]string `json:"env,omitempty" yaml:"env,omitempty"`

	// WorkingDir is the container's working directory, absolute when set.
	WorkingDir string `json:"working_dir,omitempty" yaml:"working_dir,omitempty"`

	// User is the uid:gid to run as. It defaults to 65534:65534 - nobody - and
	// a grant asking for root has to write it, which makes it reviewable.
	User string `json:"user,omitempty" yaml:"user,omitempty"`

	// Network is "none" (the default) or the name of a network the operator's
	// daemon already has. There is no host networking here at all.
	Network string `json:"network,omitempty" yaml:"network,omitempty"`

	// Mounts are mount grant names, resolved through [grants.Mounts].
	Mounts []string `json:"mounts,omitempty" yaml:"mounts,omitempty"`

	// WritableRootFilesystem turns off the read-only root filesystem. A grant
	// that needs to write somewhere should usually mount a writable path
	// instead; this exists because some images genuinely cannot start
	// otherwise, and it is written out so a reviewer sees it.
	WritableRootFilesystem bool `json:"writable_root_filesystem,omitempty" yaml:"writable_root_filesystem,omitempty"`

	// Timeout bounds the whole run. Zero takes the default.
	Timeout Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`

	// MemoryBytes, NanoCPUs and PidsLimit are the resource bounds. Memory and
	// CPU are required: a container with no limit is a container that can take
	// the worker's host down, and "the operator forgot" is not a limit.
	MemoryBytes int64 `json:"memory_bytes" yaml:"memory_bytes"`
	NanoCPUs    int64 `json:"nano_cpus" yaml:"nano_cpus"`
	PidsLimit   int64 `json:"pids_limit,omitempty" yaml:"pids_limit,omitempty"`

	// MaxOutputBytes bounds each of stdout and stderr. Zero takes the default.
	MaxOutputBytes int64 `json:"max_output_bytes,omitempty" yaml:"max_output_bytes,omitempty"`

	// SuccessExitCodes are the statuses that count as success; empty means
	// exactly zero.
	SuccessExitCodes []int32 `json:"success_exit_codes,omitempty" yaml:"success_exit_codes,omitempty"`

	// Namespaces, when non-empty, are the tenant namespaces whose workflows may
	// spend this grant, compared against the namespace the server established
	// for the caller.
	Namespaces []string `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
}

// parameterGrant is what one placeholder will accept.
type parameterGrant struct {
	Pattern  string `json:"pattern" yaml:"pattern"`
	MaxBytes int    `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`

	compiled *regexp.Regexp
}

// Duration is a YAML duration - "5m", "30s".
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

// loadGrants reads and checks the operator's file at startup, so a grant that
// could never work is a worker that says so rather than a workflow that
// discovers it.
func loadGrants() (*grants, error) {
	path := os.Getenv(grantsEnv)
	if path == "" {
		return nil, fmt.Errorf(
			"%s is not set, so this plugin has no daemon and no runs. An operator grants them with "+
				"`flow worker --plugin-env docker=%s=/path/to/grants.yaml`", grantsEnv, grantsEnv)
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
	if err := g.Daemon.check(); err != nil {
		return err
	}
	if len(g.Runs) == 0 {
		return fmt.Errorf("no runs are granted, so no call could succeed")
	}

	for _, name := range slices.Sorted(maps.Keys(g.Mounts)) {
		if err := g.Mounts[name].check(name); err != nil {
			return err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(g.Runs)) {
		run := g.Runs[name]
		if err := run.check(name, g.Mounts); err != nil {
			return err
		}
		g.Runs[name] = run
	}
	return nil
}

// check validates the daemon grant.
func (d daemonGrant) check() error {
	switch {
	case d.Socket == "" && d.Address == "":
		return fmt.Errorf("the daemon grant names neither a socket nor an address")
	case d.Socket != "" && d.Address != "":
		return fmt.Errorf("the daemon grant names both a socket and an address; name one")
	case d.Socket != "":
		if !strings.HasPrefix(d.Socket, "/") {
			return fmt.Errorf("the daemon socket %q is not an absolute path", truncate(d.Socket, 128))
		}
		if d.TLSCAFile != "" || d.TLSCertFile != "" || d.TLSKeyFile != "" {
			return fmt.Errorf("the daemon grant names a socket and TLS material; TLS belongs to a remote address")
		}
	default:
		if d.TLSCAFile == "" || d.TLSCertFile == "" || d.TLSKeyFile == "" {
			return fmt.Errorf(
				"the daemon address %q needs tls_ca_file, tls_cert_file and tls_key_file; an unverified connection to a daemon is a connection to whoever answered",
				truncate(d.Address, 128))
		}
		if strings.Contains(d.Address, "://") {
			return fmt.Errorf("the daemon address %q is host:port, without a scheme", truncate(d.Address, 128))
		}
	}

	if d.APIVersion != "" && !apiVersionPattern.MatchString(d.APIVersion) {
		return fmt.Errorf("the daemon api_version %q is not of the form v1.43", truncate(d.APIVersion, 32))
	}
	return nil
}

// apiVersion is the Engine API version this plugin addresses.
func (d daemonGrant) apiVersion() string {
	if d.APIVersion == "" {
		// Old enough that every maintained daemon speaks it, new enough for the
		// fields this plugin sets. Named rather than negotiated so an upgrade
		// does not change what a request means.
		return "v1.43"
	}
	return d.APIVersion
}

// check validates one mount grant.
func (m mountGrant) check(name string) error {
	if !strings.HasPrefix(m.Source, "/") {
		return fmt.Errorf("mount %q has a source that is not an absolute host path", name)
	}
	if !strings.HasPrefix(m.Target, "/") {
		return fmt.Errorf("mount %q has a target that is not an absolute container path", name)
	}
	if strings.Contains(m.Source, ":") || strings.Contains(m.Target, ":") {
		// A colon is the field separator in the daemon's own bind syntax, and a
		// path holding one could otherwise smuggle a third field - the one that
		// says "rw".
		return fmt.Errorf("mount %q has a path holding a colon", name)
	}
	return nil
}

// check validates one run grant and compiles its patterns.
func (r *runGrant) check(name string, mounts map[string]mountGrant) error {
	if err := flowstatev1.ValidateContentDigest(digestOf(r.Image)); err != nil {
		return fmt.Errorf(
			"run %q names the image %q, which is not digest-pinned; write registry/repository@sha256:… so that what an "+
				"operator reviewed and what a node executes are the same bytes (%v)",
			name, truncate(r.Image, 128), err)
	}
	if r.MemoryBytes <= 0 || r.MemoryBytes > maxMemoryBytes {
		return fmt.Errorf("run %q needs memory_bytes between 1 and %d; a container with no memory limit can take this host down", name, maxMemoryBytes)
	}
	if r.NanoCPUs <= 0 || r.NanoCPUs > maxNanoCPUs {
		return fmt.Errorf("run %q needs nano_cpus between 1 and %d", name, maxNanoCPUs)
	}
	if r.PidsLimit < 0 || r.PidsLimit > maxPidsLimit {
		return fmt.Errorf("run %q has a pids_limit over this plugin's ceiling of %d", name, maxPidsLimit)
	}
	if time.Duration(r.Timeout) < 0 || r.Timeout.duration(defaultRunTimeout) > maxRunTimeout {
		return fmt.Errorf("run %q has a timeout over this plugin's ceiling of %s", name, maxRunTimeout)
	}
	if r.MaxOutputBytes < 0 || r.outputLimit() > maxOutputByteLimit {
		return fmt.Errorf("run %q has max_output_bytes over this plugin's ceiling of %d", name, maxOutputByteLimit)
	}
	if len(r.Mounts) > maxMounts {
		return fmt.Errorf("run %q names %d mounts, over this plugin's ceiling of %d", name, len(r.Mounts), maxMounts)
	}
	for _, mount := range r.Mounts {
		if _, ok := mounts[mount]; !ok {
			return fmt.Errorf("run %q names the mount %q, which is not granted anywhere in this file", name, truncate(mount, 64))
		}
	}
	if len(r.Env) > maxEnvironment {
		return fmt.Errorf("run %q names %d environment variables, over this plugin's ceiling of %d", name, len(r.Env), maxEnvironment)
	}
	for key := range r.Env {
		if key == "" || strings.ContainsAny(key, "=\x00") {
			return fmt.Errorf("run %q names an environment variable that is not a KEY", name)
		}
	}
	if r.WorkingDir != "" && !strings.HasPrefix(r.WorkingDir, "/") {
		return fmt.Errorf("run %q has a working_dir that is not absolute", name)
	}
	if r.User != "" && !userPattern.MatchString(r.User) {
		return fmt.Errorf("run %q has a user %q that is not uid:gid", name, truncate(r.User, 64))
	}
	if r.User == "0:0" || r.User == "0" {
		// Allowed, and written out: a reviewer reading the file sees it.
		// Refusing it outright would send operators to a plugin that does not
		// check anything at all.
		_ = r.User
	}
	if r.Network != "" && r.Network != "none" && !networkPattern.MatchString(r.Network) {
		return fmt.Errorf("run %q names a network %q that is not a name the daemon could have", name, truncate(r.Network, 64))
	}
	if strings.EqualFold(r.Network, "host") {
		return fmt.Errorf(
			"run %q asks for host networking, which this plugin does not do at any grant level: a container on the "+
				"host's network reaches everything this worker can, including the loopback services an egress policy cannot see", name)
	}

	declared := make(map[string]bool, len(r.Parameters))
	for parameter, grant := range r.Parameters {
		if !parameterNamePattern.MatchString(parameter) {
			return fmt.Errorf("run %q declares the parameter %q, which is not a name", name, truncate(parameter, 64))
		}
		if grant.Pattern == "" {
			return fmt.Errorf("run %q declares the parameter %q with no pattern", name, parameter)
		}
		compiled, err := regexp.Compile("^(?:" + grant.Pattern + ")$")
		if err != nil {
			return fmt.Errorf("run %q parameter %q: pattern does not compile: %w", name, parameter, err)
		}
		if grant.MaxBytes < 0 {
			return fmt.Errorf("run %q parameter %q: max_bytes is negative", name, parameter)
		}
		grant.compiled = compiled
		r.Parameters[parameter] = grant
		declared[parameter] = true
	}

	for _, argument := range r.Argv {
		for _, placeholder := range placeholderPattern.FindAllStringSubmatch(argument, -1) {
			if !declared[placeholder[1]] {
				return fmt.Errorf("run %q uses ${%s} and declares no parameter by that name", name, truncate(placeholder[1], 64))
			}
			delete(declared, placeholder[1])
		}
	}
	for parameter := range declared {
		return fmt.Errorf("run %q declares the parameter %q and never uses it", name, parameter)
	}
	return nil
}

// outputLimit is the per-stream bound this run's output is read under.
func (r runGrant) outputLimit() int64 {
	if r.MaxOutputBytes == 0 {
		return defaultOutputLimit
	}
	return r.MaxOutputBytes
}

// successful reports whether an exit status counts as success here.
func (r runGrant) successful(code int32) bool {
	if len(r.SuccessExitCodes) == 0 {
		return code == 0
	}
	return slices.Contains(r.SuccessExitCodes, code)
}

// user is the uid:gid the container runs as, defaulting to nobody.
func (r runGrant) user() string {
	if r.User == "" {
		return "65534:65534"
	}
	return r.User
}

// network is the network mode, defaulting to none.
func (r runGrant) network() string {
	if r.Network == "" {
		return "none"
	}
	return r.Network
}

// reachableFrom reports whether a caller's namespace may spend this grant.
func (r runGrant) reachableFrom(namespace string) bool {
	if len(r.Namespaces) == 0 {
		return true
	}
	return slices.Contains(r.Namespaces, namespace)
}

// digestOf returns the digest component of a reference, or the whole string
// when there is no "@" - so that a tag reaches the digest validator and is
// refused by it with a message about digests.
func digestOf(reference string) string {
	if at := strings.LastIndex(reference, "@"); at >= 0 {
		return reference[at+1:]
	}
	return reference
}

var (
	placeholderPattern   = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)
	parameterNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	apiVersionPattern    = regexp.MustCompile(`^v[0-9]+\.[0-9]+$`)
	userPattern          = regexp.MustCompile(`^[0-9]+(:[0-9]+)?$`)
	networkPattern       = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
)

// truncate bounds a value before it is interpolated into a message.
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}
