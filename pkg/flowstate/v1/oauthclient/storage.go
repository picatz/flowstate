package oauthclient

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// RefreshStore stores interactive refresh material. Production implementations
// should use the OS credential vault. The interface permits Keychain, Credential
// Manager and Secret Service adapters without exposing values to Client users.
type RefreshStore interface {
	Load(context.Context, string) ([]byte, error)
	Store(context.Context, string, []byte) error
	Delete(context.Context, string) error
}

// DevFileStore is an explicit development-only fallback. It refuses directories
// writable by group/other and creates files with owner-only permissions.
type DevFileStore struct{ Dir string }

func (s DevFileStore) path(key string) (string, error) {
	if s.Dir == "" {
		return "", errors.New("oauthclient: development credential directory is required")
	}
	st, err := os.Stat(s.Dir)
	if err != nil {
		return "", err
	}
	if !st.IsDir() || st.Mode().Perm()&0o077 != 0 {
		return "", errors.New("oauthclient: development credential directory must be owner-only")
	}
	h := sha256String(key)
	return filepath.Join(s.Dir, h), nil
}
func (s DevFileStore) Load(_ context.Context, key string) ([]byte, error) {
	p, e := s.path(key)
	if e != nil {
		return nil, e
	}
	return os.ReadFile(p)
}
func (s DevFileStore) Store(_ context.Context, key string, value []byte) error {
	p, e := s.path(key)
	if e != nil {
		return e
	}
	tmp, e := os.CreateTemp(s.Dir, ".oauth-")
	if e != nil {
		return e
	}
	name := tmp.Name()
	defer os.Remove(name)
	if e = tmp.Chmod(0o600); e == nil {
		_, e = tmp.Write(value)
	}
	if closeErr := tmp.Close(); e == nil {
		e = closeErr
	}
	if e == nil {
		e = os.Rename(name, p)
	}
	return e
}
func (s DevFileStore) Delete(_ context.Context, key string) error {
	p, e := s.path(key)
	if e != nil {
		return e
	}
	e = os.Remove(p)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	return e
}
func sha256String(v string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(v))) }
