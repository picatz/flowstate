package deviceflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

// maxEntryBytes bounds a stored entry; a real one is a few kilobytes.
const maxEntryBytes = 256 << 10

// NormalizeOrigin reduces a server URL to its origin, scheme://host[:port],
// lowercased and without the scheme's default port, so two spellings of one
// server compare equal. Only http and https are accepted; userinfo, a path
// other than "/", a query and a fragment are refused, because an address
// carrying them is not naming an origin and a credential bound to the wrong
// thing is no binding.
func NormalizeOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return "", fmt.Errorf("%q is not an absolute http(s) URL", clean(raw, 200))
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("%q must be only a scheme, host and optional port", clean(raw, 200))
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("%q must use http or https", clean(raw, 200))
	}
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if port == "" || (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		if strings.Contains(host, ":") {
			host = "[" + host + "]"
		}
		return scheme + "://" + host, nil
	}
	return scheme + "://" + net.JoinHostPort(host, port), nil
}

// Sentinel errors from the [Store].
var (
	// ErrNotLoggedIn is returned when no stored login matches. It wraps
	// [fs.ErrNotExist].
	ErrNotLoggedIn = fmt.Errorf("deviceflow: not logged in: %w", fs.ErrNotExist)

	// ErrAmbiguousLogin is returned by [Store.Select] when several stored
	// logins match and nothing says which to use.
	ErrAmbiguousLogin = errors.New("deviceflow: more than one stored login matches")

	// ErrInsecurePermissions is returned when the store directory or an entry
	// is readable or writable by anyone but its owner, or is not a plain
	// file or directory. The entry is not read: whoever else could read it
	// could have been the one to write it.
	ErrInsecurePermissions = errors.New("deviceflow: login storage has loose permissions")
)

// Entry is one stored login: who it is for, where to refresh it, and the
// tokens. It renders without secret through [Tokens].
type Entry struct {
	// Issuer and ClientID key the entry.
	Issuer   string
	ClientID string

	// ServerOrigin is the normalized origin ([NormalizeOrigin]) of the
	// Flowstate server this login was made for. The access token is presented
	// to that origin and no other.
	ServerOrigin string

	// Endpoints are the discovered endpoints, so refresh and revocation need
	// no second discovery. They are re-checked with [RequireSecureURL] before
	// each use.
	Endpoints Endpoints

	// Tokens are the credentials.
	Tokens Tokens
}

// record is the on-disk shape of an [Entry].
type record struct {
	Version      int       `json:"version"`
	Issuer       string    `json:"issuer"`
	ClientID     string    `json:"client_id"`
	ServerOrigin string    `json:"server_origin"`
	Endpoints    Endpoints `json:"endpoints"`
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scope        string    `json:"scope,omitempty"`
}

// Store keeps logins on disk, one file per issuer and client ID.
type Store struct {
	dir string
}

// NewStore returns a Store rooted at dir. The directory is created 0700 on the
// first [Store.Save].
func NewStore(dir string) *Store { return &Store{dir: dir} }

// DefaultStore returns the Store at os.UserConfigDir()/flowstate/login.
func DefaultStore() (*Store, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("locating the user config directory: %w", err)
	}
	return NewStore(filepath.Join(base, "flowstate", "login")), nil
}

// Dir is the directory entries live in.
func (s *Store) Dir() string { return s.dir }

func (s *Store) path(issuer, clientID string) string {
	sum := sha256.Sum256([]byte(issuer + "\x00" + clientID))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".json")
}

// checkDir refuses a directory that is not a real directory owned-only.
func (s *Store) checkDir() error {
	info, err := os.Lstat(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotLoggedIn
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrInsecurePermissions, s.dir)
	}
	if looseMode(info.Mode()) {
		return fmt.Errorf("%w: %s is mode %04o, want 0700 (run: chmod 700 %s)",
			ErrInsecurePermissions, s.dir, info.Mode().Perm(), s.dir)
	}
	return nil
}

// looseMode reports a group or world permission bit. Windows has no such bits
// to check.
func looseMode(m fs.FileMode) bool {
	return runtime.GOOS != "windows" && m.Perm()&0o077 != 0
}

// Save writes e atomically: a temporary file in the same directory at 0600,
// synced, then renamed over the entry.
func (s *Store) Save(e Entry) error {
	if e.Issuer == "" || e.ClientID == "" || e.Tokens.AccessToken == "" {
		return errors.New("deviceflow: refusing to store an entry without an issuer, client ID and access token")
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return fmt.Errorf("creating login directory: %w", err)
	}
	if err := s.checkDir(); err != nil {
		return err
	}

	data, err := json.Marshal(record{
		Version:      1,
		Issuer:       e.Issuer,
		ClientID:     e.ClientID,
		ServerOrigin: e.ServerOrigin,
		Endpoints:    e.Endpoints,
		AccessToken:  e.Tokens.AccessToken,
		RefreshToken: e.Tokens.RefreshToken,
		ExpiresAt:    e.Tokens.ExpiresAt,
		Scope:        e.Tokens.Scope,
	})
	if err != nil {
		return err
	}

	// The reader's bound, enforced where the entry is made: an entry [Store.Load]
	// would refuse is a login that reports success and then never works.
	if len(data) > maxEntryBytes {
		return fmt.Errorf("deviceflow: the login is %d bytes, over the %d-byte limit for a stored entry; "+
			"the identity provider's tokens are too large to store", len(data), maxEntryBytes)
	}

	tmp, err := os.CreateTemp(s.dir, ".login-*.tmp")
	if err != nil {
		return fmt.Errorf("writing login: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if err := tmp.Chmod(0o600); err != nil && runtime.GOOS != "windows" {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("writing login: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("writing login: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("writing login: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("writing login: %w", err)
	}
	if err := os.Rename(tmpName, s.path(e.Issuer, e.ClientID)); err != nil {
		cleanup()
		return fmt.Errorf("writing login: %w", err)
	}
	return nil
}

// Load returns the entry for issuer and clientID. It is [ErrNotLoggedIn] when
// there is none and [ErrInsecurePermissions] when the file or its directory is
// loose; in that case nothing is read.
func (s *Store) Load(issuer, clientID string) (Entry, error) {
	e, err := s.read(s.path(issuer, clientID))
	if err != nil {
		return Entry{}, err
	}
	if e.Issuer != issuer || e.ClientID != clientID {
		return Entry{}, fmt.Errorf("%w: the stored entry does not belong to this issuer and client", ErrInsecurePermissions)
	}
	return e, nil
}

func (s *Store) read(path string) (Entry, error) {
	if err := s.checkDir(); err != nil {
		return Entry{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Entry{}, ErrNotLoggedIn
		}
		return Entry{}, err
	}
	if !info.Mode().IsRegular() {
		return Entry{}, fmt.Errorf("%w: %s is not a regular file", ErrInsecurePermissions, path)
	}
	if looseMode(info.Mode()) {
		return Entry{}, fmt.Errorf("%w: %s is mode %04o, want 0600 (run: chmod 600 %s, or `flow logout` and log in again)",
			ErrInsecurePermissions, path, info.Mode().Perm(), path)
	}

	f, err := os.Open(path)
	if err != nil {
		return Entry{}, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxEntryBytes+1))
	if err != nil {
		return Entry{}, err
	}
	if len(data) > maxEntryBytes {
		return Entry{}, fmt.Errorf("stored login %s is larger than %d bytes", path, maxEntryBytes)
	}

	var r record
	if err := json.Unmarshal(data, &r); err != nil || r.Version != 1 || r.AccessToken == "" {
		return Entry{}, fmt.Errorf("stored login %s is not readable; run `flow logout` and log in again", path)
	}
	return Entry{
		Issuer:       r.Issuer,
		ClientID:     r.ClientID,
		ServerOrigin: r.ServerOrigin,
		Endpoints:    r.Endpoints,
		Tokens: Tokens{
			AccessToken:  r.AccessToken,
			RefreshToken: r.RefreshToken,
			ExpiresAt:    r.ExpiresAt,
			Scope:        r.Scope,
		},
	}, nil
}

// Delete removes the entry, [ErrNotLoggedIn] when there was none.
func (s *Store) Delete(issuer, clientID string) error {
	if err := os.Remove(s.path(issuer, clientID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotLoggedIn
		}
		return err
	}
	return nil
}

// List returns every stored entry, ordered by issuer then client ID. One
// unreadable or loose entry fails the call: a listing that skipped it would
// make the remaining entry look like the only login.
func (s *Store) List() ([]Entry, error) {
	if err := s.checkDir(); err != nil {
		return nil, err
	}
	names, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var entries []Entry
	for _, name := range names {
		if !strings.HasSuffix(name.Name(), ".json") {
			continue
		}
		e, err := s.read(filepath.Join(s.dir, name.Name()))
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b Entry) int {
		if c := strings.Compare(a.Issuer, b.Issuer); c != 0 {
			return c
		}
		return strings.Compare(a.ClientID, b.ClientID)
	})
	return entries, nil
}

// Select finds the entry for issuer and clientID, where either may be empty to
// mean "whichever": with both given it is [Store.Load]; otherwise exactly one
// stored entry must match, [ErrNotLoggedIn] if none and [ErrAmbiguousLogin] if
// several.
func (s *Store) Select(issuer, clientID string) (Entry, error) {
	if issuer != "" && clientID != "" {
		return s.Load(issuer, clientID)
	}
	all, err := s.List()
	if err != nil {
		return Entry{}, err
	}
	matches := slices.DeleteFunc(all, func(e Entry) bool {
		return (issuer != "" && e.Issuer != issuer) || (clientID != "" && e.ClientID != clientID)
	})
	switch len(matches) {
	case 0:
		return Entry{}, ErrNotLoggedIn
	case 1:
		return matches[0], nil
	default:
		return Entry{}, fmt.Errorf("%w: choose one with --issuer and --client-id (FLOWSTATE_ISSUER, FLOWSTATE_CLIENT_ID)", ErrAmbiguousLogin)
	}
}
