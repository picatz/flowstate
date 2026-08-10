package plugin

import (
	"errors"
	"fmt"
)

// Sentinel errors, so a caller can tell a plugin that is misbehaving from one
// that is merely unreachable, and a misconfigured deployment from a broken
// binary. Every one of them is safe to log: they name plugins, paths, schemes,
// and versions, never anything a plugin resolved.
var (
	// ErrSearchPath reports that a configured plugin directory cannot be used —
	// it is relative, or it is writable by users other than this one. A
	// directory of plugin binaries is a directory of things this process will
	// execute, so an unsafe one is refused rather than scanned.
	ErrSearchPath = errors.New("plugin: unusable search path")

	// ErrHandshake reports that a launched process did not announce itself in
	// the expected form: no line, a malformed line, an unknown handshake format,
	// a protocol version the host did not offer, or an address other than the
	// socket it was assigned.
	ErrHandshake = errors.New("plugin: handshake failed")

	// ErrHandshakeTimeout reports that a launched process printed no handshake
	// line before the deadline. The process has been killed.
	ErrHandshakeTimeout = errors.New("plugin: handshake timed out")

	// ErrLaunch reports that a plugin could not be brought up: the process would
	// not start — no such file, not executable, a socket path the operating
	// system will not accept — or it started and then could not describe
	// itself. They are one classification because they are one outcome: no
	// usable plugin, and nothing to retry into.
	ErrLaunch = errors.New("plugin: launch failed")

	// ErrExited reports that the plugin process exited. It wraps the exit status
	// when there was one.
	ErrExited = errors.New("plugin: process exited")

	// ErrManifest reports that a plugin described itself in a way that cannot be
	// accepted: a manifest that fails its protovalidate rules, one advertising
	// no capabilities, or one that changed across a restart.
	ErrManifest = errors.New("plugin: invalid manifest")

	// ErrDistribution reports that the executable behind a plugin changed while
	// the plugin was running. It is separate from [ErrManifest] because it is a
	// different fact and a worse one: a manifest that changed says the plugin
	// admits to being something else, and this says it does not: same name, same
	// version, same tasks, different bytes.
	ErrDistribution = errors.New("plugin: distribution changed")

	// ErrDuplicateScheme reports that two plugins claim one secret scheme. Two
	// answers for one scheme is a configuration error, not something to resolve
	// by which plugin happened to load first.
	ErrDuplicateScheme = errors.New("plugin: duplicate secret scheme")

	// ErrSchemeNotPermitted reports that a plugin claims a scheme the deployment
	// did not permit. A deployment that lists its permitted schemes gets those
	// and nothing a binary dropped into the search path adds.
	ErrSchemeNotPermitted = errors.New("plugin: secret scheme not permitted")

	// ErrCapability reports that something was asked of a plugin that it did not
	// advertise. A plugin serves what its manifest claims and nothing else.
	ErrCapability = errors.New("plugin: capability not advertised")

	// ErrUnavailable reports that a plugin is not currently able to serve: it is
	// restarting, it has exhausted its restart budget, or the host is closed. It
	// is the one classification worth retrying.
	ErrUnavailable = errors.New("plugin: unavailable")

	// ErrClosed reports use of a host that has been closed.
	ErrClosed = errors.New("plugin: host is closed")

	// ErrDescriptor reports that a task manifest's serialized descriptors could
	// not be reconstructed into the message they name.
	ErrDescriptor = errors.New("plugin: invalid task descriptor")
)

// Retryable reports whether a plugin failure could plausibly succeed on another
// attempt.
//
// Only unavailability is transient. A refused handshake, an invalid manifest, a
// duplicate scheme, or a capability a plugin does not have describe a state the
// next attempt finds unchanged. An unclassified error is treated as permanent,
// because guessing that a failure is retryable is the more expensive mistake —
// the same rule the secrets and task error classifications use.
func Retryable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}

// Error reports a failure involving one plugin, naming which one.
//
// The name comes from the binary the host launched — the suffix of
// flowstate-plugin-<name> — rather than from anything the plugin said about
// itself, so an error attributes a failure to the process that actually caused
// it even when that process is lying about what it is.
type Error struct {
	// Plugin is the name of the plugin, from its binary.
	Plugin string

	// Path is the binary that was launched, when the failure is about a
	// particular one.
	Path string

	// Err is the underlying cause.
	Err error
}

// Error implements the error interface.
func (e *Error) Error() string {
	switch {
	case e.Plugin != "" && e.Path != "":
		return fmt.Sprintf("plugin %q (%s): %v", e.Plugin, e.Path, e.Err)
	case e.Plugin != "":
		return fmt.Sprintf("plugin %q: %v", e.Plugin, e.Err)
	default:
		return e.Err.Error()
	}
}

// Unwrap returns the cause, so the sentinels above match through errors.Is.
func (e *Error) Unwrap() error { return e.Err }

// pluginError wraps err as belonging to a named plugin.
func pluginError(name, path string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Plugin: name, Path: path, Err: err}
}
