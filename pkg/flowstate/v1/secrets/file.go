package secrets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

// DefaultFileMaxBytes bounds how large a secret file may be. Credentials are small;
// the limit is what stops a reference to a huge or endless file from becoming a way
// to exhaust a worker's memory.
const DefaultFileMaxBytes int64 = 1 << 20 // 1 MiB

// FileProvider resolves secrets from files under one directory. It handles the
// "file" scheme, and is the shape a Kubernetes secret takes when it is mounted as
// a volume, or a systemd credential, or a Docker secret.
//
// Reference names are relative to the directory. "file:api-key" reads
// <dir>/api-key, and "file:db/password" reads <dir>/db/password. That flat layout
// is what a Kubernetes secret volume mounts, which is why it is the default.
//
// [WithFileNamespaced] gives each tenant a directory of its own, for a worker
// serving more than one.
//
// The directory is opened once and every read goes through [os.Root], so a name
// cannot escape it: "..", an absolute path, and a symlink pointing outside are all
// refused by the kernel-level check rather than by string inspection, which closes
// the traversal and symlink races that string checks miss.
//
// It is safe for concurrent use.
type FileProvider struct {
	dir        string
	root       *os.Root
	maxBytes   int64
	verbatim   bool
	namespaced bool
}

// DefaultNamespaceDir is the directory the unnamespaced tenant reads when the
// provider is namespaced.
//
// It begins with an underscore, which [ValidateNamespace] forbids, so no tenant can
// name a namespace that resolves to it. That is what keeps the default tenant's
// secrets separate from every other tenant's rather than merely differently named.
const DefaultNamespaceDir = "_default"

// FileOption configures a [FileProvider].
type FileOption func(*FileProvider)

// WithFileMaxBytes sets the largest secret file that may be read. A file larger
// than the limit is an error wrapping [ErrTooLarge], never a truncated secret.
func WithFileMaxBytes(n int64) FileOption {
	return func(p *FileProvider) {
		p.maxBytes = n
	}
}

// WithFileNamespaced puts every tenant's secrets in a directory of their own,
// which is what makes this provider usable by more than one tenant.
//
// With it, "file:api-key" reads <dir>/<namespace>/api-key, and the unnamespaced
// tenant reads <dir>/[DefaultNamespaceDir]/api-key. Every tenant gets a segment,
// including the default one — without that, the default tenant could read
// <dir>/team-a/api-key simply by naming "team-a/api-key", since a reference may
// contain a slash.
//
// Without it, a non-empty namespace is refused. That is the fail-closed choice, and
// it keeps the flat layout a Kubernetes secret volume mounts, where every key sits
// directly in the mount directory.
func WithFileNamespaced() FileOption {
	return func(p *FileProvider) {
		p.namespaced = true
	}
}

// WithFileVerbatim keeps the file's bytes exactly as they are.
//
// By default one trailing line ending is removed — "\n", "\r\n", or a lone
// "\r" — because the tools that write secret files add one: a shell redirect, an
// editor, a templating step. A newline inside a bearer token produces a failure
// that looks like a rejected credential rather than a malformed one. Use this when
// the line ending is part of the secret, as it can be for a PEM-encoded key.
func WithFileVerbatim() FileOption {
	return func(p *FileProvider) {
		p.verbatim = true
	}
}

// NewFileProvider returns a provider reading secrets from files under dir.
//
// It fails if dir cannot be opened, so a worker configured with a directory that
// does not exist fails at startup rather than on the first workflow that needs a
// secret.
func NewFileProvider(dir string, opts ...FileOption) (*FileProvider, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("secrets: opening secret directory %q: %w", dir, err)
	}

	provider := &FileProvider{
		dir:      dir,
		root:     root,
		maxBytes: DefaultFileMaxBytes,
	}

	for _, opt := range opts {
		opt(provider)
	}

	return provider, nil
}

// Close releases the directory handle. A worker holds one provider for its
// lifetime, so this is for tests and for a worker that reconfigures itself.
func (p *FileProvider) Close() error {
	return p.root.Close()
}

// Dir returns the directory secrets are read from. It is safe to log.
func (p *FileProvider) Dir() string {
	return p.dir
}

// Scheme implements [Provider].
func (p *FileProvider) Scheme() string {
	return "file"
}

// Resolve implements [Provider], reading <dir>/<name>.
//
// Reads are not cached here. Wrap the provider in a [Cache] to bound how often the
// file is read and how stale a rotated secret may be.
func (p *FileProvider) Resolve(_ context.Context, req Request) (Secret, error) {
	ref := req.Ref

	name, err := cleanSecretPath(ref.GetName())
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	// The namespace becomes a leading directory. The name is cleaned first, so it
	// carries no dot segments to climb back out with, and os.Root refuses an escape
	// regardless.
	switch {
	case p.namespaced:
		segment := req.Namespace
		if segment == "" {
			segment = DefaultNamespaceDir
		}
		name = path.Join(segment, name)

	case req.Namespace != "":
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf(
				"%w: this worker's file provider is not namespaced, so it cannot resolve secrets for namespace %q; "+
					"configure it with WithFileNamespaced",
				ErrNamespace, req.Namespace,
			),
		}
	}

	file, err := p.root.Open(name)
	if err != nil {
		// A path that escapes the directory is reported as not found rather than
		// as a traversal attempt, so that probing cannot tell the difference
		// between "outside the directory" and "does not exist".
		if errors.Is(err, fs.ErrNotExist) || isEscapeError(err) {
			// The directory is deliberately not named: this message reaches
			// workflow history, and the reference alone identifies the problem.
			return Secret{}, &ResolveError{
				Ref: ref,
				Err: fmt.Errorf("%w: no secret file %q", ErrNotFound, name),
			}
		}

		return Secret{}, &ResolveError{Ref: ref, Err: fmt.Errorf("opening secret file: %w", err)}
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: fmt.Errorf("inspecting secret file: %w", err)}
	}
	if info.IsDir() {
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: %q is a directory, not a secret", ErrNotFound, name),
		}
	}

	value, err := p.read(file)
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	if value == "" {
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: %q is empty", ErrEmpty, name),
		}
	}

	return NewSecret(ref, value), nil
}

// read reads a secret file under the size limit. It reads one byte past the limit
// so that a file exactly at the limit is distinguishable from one that exceeds it,
// and so exceeding it is an error rather than a silently truncated credential.
func (p *FileProvider) read(r io.Reader) (string, error) {
	if p.maxBytes > 0 {
		r = io.LimitReader(r, p.maxBytes+1)
	}

	contents, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("reading secret file: %w", err)
	}

	if p.maxBytes > 0 && int64(len(contents)) > p.maxBytes {
		return "", fmt.Errorf("%w: larger than %d bytes", ErrTooLarge, p.maxBytes)
	}

	value := string(contents)
	if !p.verbatim {
		value = strings.TrimSuffix(value, "\n")
		value = strings.TrimSuffix(value, "\r")
	}

	return value, nil
}

// cleanSecretPath validates a reference name as a path relative to the secret
// directory.
//
// [os.Root] would refuse an escaping path on its own; this rejects the obvious
// cases first so the error says what is wrong with the reference instead of
// reporting a failed syscall.
func cleanSecretPath(name string) (string, error) {
	switch {
	case name == "":
		return "", fmt.Errorf("%w: name must not be empty", ErrInvalidRef)
	case path.IsAbs(name):
		return "", fmt.Errorf(
			"%w: %q must be relative to the secret directory, not an absolute path",
			ErrInvalidRef, name,
		)
	case strings.Contains(name, `\`):
		return "", fmt.Errorf("%w: %q must use forward slashes", ErrInvalidRef, name)
	}

	// A control character has no legitimate use in a path and would let a reference
	// forge lines in a log or an error that records it. Ref.Validate rejects these
	// too, but a Provider is exported and may be called directly.
	if i := strings.IndexFunc(name, isControl); i >= 0 {
		return "", fmt.Errorf(
			"%w: %q contains a control character at offset %d",
			ErrInvalidRef, name, i,
		)
	}

	cleaned := path.Clean(name)
	switch {
	case cleaned == ".":
		return "", fmt.Errorf("%w: %q names the secret directory, not a secret in it", ErrInvalidRef, name)
	case cleaned == "..", strings.HasPrefix(cleaned, "../"):
		return "", fmt.Errorf("%w: %q points outside the secret directory", ErrInvalidRef, name)
	}

	return cleaned, nil
}

// isEscapeError reports whether err is os.Root refusing a path that leaves the
// directory. There is no sentinel for it, so the check is on the message, and it
// only affects which error is reported for a path that was going to be refused
// either way.
func isEscapeError(err error) bool {
	return strings.Contains(err.Error(), "path escapes from parent")
}
