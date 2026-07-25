package secrets

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// secretDir builds a directory of secret files, plus a file outside it that a
// traversal would try to reach, and returns the directory.
func secretDir(t *testing.T, files map[string]string) string {
	t.Helper()

	base := t.TempDir()

	// Outside the secret directory, so a traversal has a real target to fail to
	// reach rather than merely hitting a missing file.
	require.NoError(t, os.WriteFile(filepath.Join(base, "outside.txt"), []byte("outside-value"), 0o600))

	dir := filepath.Join(base, "secrets")
	require.NoError(t, os.Mkdir(dir, 0o700))

	for name, contents := range files {
		full := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
		require.NoError(t, os.WriteFile(full, []byte(contents), 0o600))
	}

	return dir
}

func Test_NewFileProvider(t *testing.T) {
	t.Run("a missing directory fails at construction", func(t *testing.T) {
		// A worker configured with a directory that does not exist should fail at
		// startup, not on the first workflow that needs a secret.
		provider, err := NewFileProvider(filepath.Join(t.TempDir(), "nope"))
		require.Nil(t, provider)
		require.ErrorContains(t, err, "opening secret directory")
	})

	t.Run("an existing directory succeeds", func(t *testing.T) {
		dir := secretDir(t, map[string]string{"api-key": "v"})

		provider, err := NewFileProvider(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, provider.Close()) })

		require.Equal(t, "file", provider.Scheme())
		require.Equal(t, dir, provider.Dir())
	})
}

func Test_FileProvider_Resolve(t *testing.T) {
	files := map[string]string{
		"api-key":         "abc123",
		"trailing":        "abc123\n",
		"crlf":            "abc123\r\n",
		"two-newlines":    "abc123\n\n",
		"blank":           "",
		"only-newline":    "\n",
		"db/password":     "nested-value",
		"multiline":       "-----BEGIN KEY-----\nline\n-----END KEY-----\n",
		"internal-spaces": "a b\tc",
	}

	tests := []struct {
		name  string
		opts  []FileOption
		ref   Ref
		check func(t *testing.T, secret Secret, err error)
	}{
		// Negative cases first.
		{
			name: "missing file",
			ref:  NewRef("file", "absent"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrNotFound)
				require.ErrorContains(t, err, "no secret file")
			},
		},
		{
			name: "empty file",
			ref:  NewRef("file", "blank"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrEmpty)
			},
		},
		{
			name: "a file holding only a newline is empty once trimmed",
			ref:  NewRef("file", "only-newline"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrEmpty)
			},
		},
		{
			name: "a directory is not a secret",
			ref:  NewRef("file", "db"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrNotFound)
				require.ErrorContains(t, err, "is a directory")
			},
		},
		{
			name: "parent traversal",
			ref:  NewRef("file", "../outside.txt"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.ErrorContains(t, err, "points outside the secret directory")
			},
		},
		{
			name: "deep traversal",
			ref:  NewRef("file", "../../../../etc/passwd"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
			},
		},
		{
			name: "traversal disguised by a subdirectory",
			ref:  NewRef("file", "db/../../outside.txt"),
			check: func(t *testing.T, _ Secret, err error) {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "outside-value")
			},
		},
		{
			name: "absolute path",
			ref:  NewRef("file", "/etc/passwd"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.ErrorContains(t, err, "must be relative")
			},
		},
		{
			name: "backslash path",
			ref:  NewRef("file", `..\outside.txt`),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
			},
		},
		{
			name: "current directory",
			ref:  NewRef("file", "."),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
			},
		},
		{
			name: "a file over the size limit is an error, not a truncated secret",
			opts: []FileOption{WithFileMaxBytes(3)},
			ref:  NewRef("file", "api-key"),
			check: func(t *testing.T, secret Secret, err error) {
				require.ErrorIs(t, err, ErrTooLarge)
				require.True(t, secret.IsZero())
				require.NotContains(t, err.Error(), "abc", "the error must not include the contents")
			},
		},

		{
			name: "a secret file resolves",
			ref:  NewRef("file", "api-key"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123", secret.Reveal())
				require.Equal(t, "file:api-key", RefString(secret.Ref()))
			},
		},
		{
			name: "a single trailing newline is removed",
			ref:  NewRef("file", "trailing"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123", secret.Reveal(),
					"a newline left in a token produces a failure that looks like a rejected credential")
			},
		},
		{
			name: "a trailing CRLF is removed",
			ref:  NewRef("file", "crlf"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123", secret.Reveal())
			},
		},
		{
			name: "only one trailing newline is removed",
			ref:  NewRef("file", "two-newlines"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123\n", secret.Reveal())
			},
		},
		{
			name: "verbatim keeps the trailing newline",
			opts: []FileOption{WithFileVerbatim()},
			ref:  NewRef("file", "trailing"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123\n", secret.Reveal())
			},
		},
		{
			name: "internal whitespace is preserved",
			ref:  NewRef("file", "internal-spaces"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "a b\tc", secret.Reveal())
			},
		},
		{
			name: "a nested path resolves",
			ref:  NewRef("file", "db/password"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "nested-value", secret.Reveal())
			},
		},
		{
			name: "a multi-line secret keeps its internal newlines",
			ref:  NewRef("file", "multiline"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "-----BEGIN KEY-----\nline\n-----END KEY-----", secret.Reveal())
			},
		},
		{
			name: "a file exactly at the size limit resolves",
			opts: []FileOption{WithFileMaxBytes(6)},
			ref:  NewRef("file", "api-key"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123", secret.Reveal())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := secretDir(t, files)

			provider, err := NewFileProvider(dir, test.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, provider.Close()) })

			secret, err := provider.Resolve(t.Context(), Request{Ref: test.ref})
			test.check(t, secret, err)
		})
	}
}

// Test_FileProvider_symlinkEscape covers the case string path checks miss: a name
// with no ".." in it that still leaves the directory, because a symlink does the
// escaping.
func Test_FileProvider_symlinkEscape(t *testing.T) {
	dir := secretDir(t, map[string]string{"api-key": "abc123"})

	tests := []struct {
		name   string
		target string
	}{
		{name: "symlink to a file outside the directory", target: "../outside.txt"},
		{name: "symlink to an absolute path", target: "/etc/passwd"},
		{name: "symlink to the parent directory", target: ".."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			link := filepath.Join(dir, "escape")
			require.NoError(t, os.Symlink(test.target, link))
			t.Cleanup(func() { require.NoError(t, os.Remove(link)) })

			provider, err := NewFileProvider(dir)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, provider.Close()) })

			secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("file", "escape")})
			require.ErrorIs(t, err, ErrNotFound,
				"a symlink out of the directory is reported as not found, so probing learns nothing")
			require.True(t, secret.IsZero())
			require.NotContains(t, err.Error(), "outside-value")
		})
	}
}

// Test_FileProvider_kubernetesLayout covers the shape a real Kubernetes secret
// volume has, which is the primary use case this provider claims.
//
// The kubelet writes each version into a timestamped directory, points a "..data"
// symlink at the current one, and symlinks each key to "..data/<key>". So every
// read traverses two symlinks inside the directory, and confinement has to permit
// that while still refusing a link that leaves.
func Test_FileProvider_kubernetesLayout(t *testing.T) {
	dir := t.TempDir()

	version := filepath.Join(dir, "..2026_07_25_12_00_00.123456789")
	require.NoError(t, os.Mkdir(version, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(version, "api-key"), []byte("k8s-value\n"), 0o600))

	require.NoError(t, os.Symlink(filepath.Base(version), filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink("..data/api-key", filepath.Join(dir, "api-key")))

	provider, err := NewFileProvider(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("file", "api-key")})
	require.NoError(t, err)
	require.Equal(t, "k8s-value", secret.Reveal())

	t.Run("a rotation is picked up", func(t *testing.T) {
		// The kubelet writes a new version directory and repoints ..data atomically.
		next := filepath.Join(dir, "..2026_07_25_12_05_00.987654321")
		require.NoError(t, os.Mkdir(next, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(next, "api-key"), []byte("rotated-value\n"), 0o600))

		tmp := filepath.Join(dir, "..data_tmp")
		require.NoError(t, os.Symlink(filepath.Base(next), tmp))
		require.NoError(t, os.Rename(tmp, filepath.Join(dir, "..data")))

		secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("file", "api-key")})
		require.NoError(t, err)
		require.Equal(t, "rotated-value", secret.Reveal())
	})
}

// Test_isEscapeError pins the behavior that classifies an out-of-directory path as
// not found.
//
// os.Root reports an escape with an error that has no sentinel, so the check is on
// the message. This test fails if a Go release rewords it, which is the point:
// otherwise the reclassification would be silent.
func Test_isEscapeError(t *testing.T) {
	base := t.TempDir()

	dir := filepath.Join(base, "secrets")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(base, "outside.txt"), []byte("v"), 0o600))

	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })

	_, err = root.Open("../outside.txt")
	require.Error(t, err)
	require.True(t, isEscapeError(err),
		"os.Root's escape error is no longer recognized: %v", err)

	// It must not match an ordinary missing file, or every absent secret would be
	// reported as an escape.
	_, err = root.Open("absent")
	require.Error(t, err)
	require.False(t, isEscapeError(err))
}

func Test_FileProvider_controlCharacters(t *testing.T) {
	dir := secretDir(t, map[string]string{"api-key": "v"})

	provider, err := NewFileProvider(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	// A Provider is exported and may be called directly, so it cannot rely on
	// Ref.Validate having run. A raw newline or NUL in a name would otherwise reach
	// an error message bound for logs and workflow history.
	names := []string{
		"api-key\x00.txt",
		"api\nkey",
		"api\rkey",
		"api\x1b[31mkey",
	}

	for _, name := range names {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("file", name)})

			require.ErrorIs(t, err, ErrInvalidRef)
			require.ErrorContains(t, err, "control character")
			require.True(t, secret.IsZero())

			// The message must not carry the raw control character onward.
			require.NotContains(t, err.Error(), "\x00")
			require.NotContains(t, err.Error(), "\n")
		})
	}
}

func Test_FileProvider_symlinkInsideIsPermitted(t *testing.T) {
	dir := secretDir(t, map[string]string{"real/api-key": "linked-value"})

	require.NoError(t, os.Symlink("real/api-key", filepath.Join(dir, "api-key")))

	provider, err := NewFileProvider(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	// Confinement must not be so strict that it breaks the layout secret tooling
	// actually produces.
	secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("file", "api-key")})
	require.NoError(t, err)
	require.Equal(t, "linked-value", secret.Reveal())
}

func Test_FileProvider_hugeFile(t *testing.T) {
	dir := secretDir(t, map[string]string{
		"huge": strings.Repeat("a", 4096),
	})

	provider, err := NewFileProvider(dir, WithFileMaxBytes(1024))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	// The limit is what stops a reference to a huge file from being a way to
	// exhaust a worker's memory. It must not read the whole file to find out.
	secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("file", "huge")})
	require.ErrorIs(t, err, ErrTooLarge)
	require.True(t, secret.IsZero())
	require.ErrorContains(t, err, "larger than 1024 bytes")
}

func Test_FileProvider_rotation(t *testing.T) {
	dir := secretDir(t, map[string]string{"api-key": "first"})

	provider, err := NewFileProvider(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	ref := NewRef("file", "api-key")

	secret, err := provider.Resolve(t.Context(), Request{Ref: ref})
	require.NoError(t, err)
	require.Equal(t, "first", secret.Reveal())

	// An uncached provider reads the file each time, so a rotated secret takes
	// effect immediately. This is what the cache trades away for a bounded TTL.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "api-key"), []byte("second"), 0o600))

	secret, err = provider.Resolve(t.Context(), Request{Ref: ref})
	require.NoError(t, err)
	require.Equal(t, "second", secret.Reveal())
}

func Test_cleanSecretPath(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "empty", in: "", wantErr: true},
		{name: "dot", in: ".", wantErr: true},
		{name: "parent", in: "..", wantErr: true},
		{name: "parent prefix", in: "../x", wantErr: true},
		{name: "absolute", in: "/x", wantErr: true},
		{name: "backslash", in: `a\b`, wantErr: true},
		{name: "escaping through a subdirectory", in: "a/../../b", wantErr: true},
		{name: "simple", in: "api-key", want: "api-key"},
		{name: "nested", in: "db/password", want: "db/password"},
		{name: "redundant dot", in: "./api-key", want: "api-key"},
		{name: "doubled slash", in: "db//password", want: "db/password"},
		{name: "inner parent that stays inside", in: "db/../api-key", want: "api-key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := cleanSecretPath(test.in)

			if test.wantErr {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, got)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
