package deviceflow_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/deviceflow"
)

func skipWithoutUnixModes(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not enforced on windows")
	}
}

func sampleEntry(issuer, clientID string) deviceflow.Entry {
	return deviceflow.Entry{
		Issuer:       issuer,
		ClientID:     clientID,
		ServerOrigin: "https://flowstate.example.com",
		Endpoints:    deviceflow.Endpoints{Issuer: issuer, Token: issuer + "/token", Revocation: issuer + "/revoke"},
		Tokens: deviceflow.Tokens{
			AccessToken:  secretAccess,
			RefreshToken: secretRefresh,
			ExpiresAt:    time.Date(2026, 10, 7, 13, 0, 0, 0, time.UTC),
			Scope:        "openid",
		},
	}
}

func newStore(t *testing.T) *deviceflow.Store {
	t.Helper()
	return deviceflow.NewStore(filepath.Join(t.TempDir(), "flowstate", "login"))
}

func TestStoreRoundTripWithStrictModes(t *testing.T) {
	skipWithoutUnixModes(t)
	store := newStore(t)
	want := sampleEntry("https://idp.example", "flow-cli")
	require.NoError(t, store.Save(want))

	dirInfo, err := os.Stat(store.Dir())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())

	files, err := os.ReadDir(store.Dir())
	require.NoError(t, err)
	require.Len(t, files, 1, "an atomic write leaves no temporary file behind")
	info, err := files[0].Info()
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	got, err := store.Load("https://idp.example", "flow-cli")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestStoreOverwriteReplacesTheEntry(t *testing.T) {
	store := newStore(t)
	e := sampleEntry("https://idp.example", "flow-cli")
	require.NoError(t, store.Save(e))
	e.Tokens.AccessToken = "second"
	require.NoError(t, store.Save(e))

	got, err := store.Load(e.Issuer, e.ClientID)
	require.NoError(t, err)
	require.Equal(t, "second", got.Tokens.AccessToken)
	all, err := store.List()
	require.NoError(t, err)
	require.Len(t, all, 1)
}

func TestStoreRefusesLooseFile(t *testing.T) {
	skipWithoutUnixModes(t)
	store := newStore(t)
	require.NoError(t, store.Save(sampleEntry("https://idp.example", "flow-cli")))

	files, _ := os.ReadDir(store.Dir())
	path := filepath.Join(store.Dir(), files[0].Name())
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604, 0o660} {
		require.NoError(t, os.Chmod(path, mode))
		_, err := store.Load("https://idp.example", "flow-cli")
		require.ErrorIs(t, err, deviceflow.ErrInsecurePermissions, "mode %04o", mode)
		require.NotContains(t, err.Error(), secretAccess)
	}
	_, err := store.Select("", "")
	require.ErrorIs(t, err, deviceflow.ErrInsecurePermissions, "a loose entry must not be skipped by a listing")

	require.NoError(t, os.Chmod(path, 0o600))
	_, err = store.Load("https://idp.example", "flow-cli")
	require.NoError(t, err)
}

func TestStoreRefusesLooseDirectory(t *testing.T) {
	skipWithoutUnixModes(t)
	store := newStore(t)
	require.NoError(t, store.Save(sampleEntry("https://idp.example", "flow-cli")))
	require.NoError(t, os.Chmod(store.Dir(), 0o755))

	_, err := store.Load("https://idp.example", "flow-cli")
	require.ErrorIs(t, err, deviceflow.ErrInsecurePermissions)
	require.ErrorIs(t, store.Save(sampleEntry("https://idp.example", "flow-cli")), deviceflow.ErrInsecurePermissions)
}

func TestStoreRefusesSymlinkedEntry(t *testing.T) {
	skipWithoutUnixModes(t)
	store := newStore(t)
	require.NoError(t, store.Save(sampleEntry("https://idp.example", "flow-cli")))
	files, _ := os.ReadDir(store.Dir())
	path := filepath.Join(store.Dir(), files[0].Name())

	target := filepath.Join(t.TempDir(), "elsewhere")
	require.NoError(t, os.Rename(path, target))
	require.NoError(t, os.Symlink(target, path))

	_, err := store.Load("https://idp.example", "flow-cli")
	require.ErrorIs(t, err, deviceflow.ErrInsecurePermissions)
}

func TestStoreMissingAndDelete(t *testing.T) {
	store := newStore(t)
	_, err := store.Load("https://idp.example", "flow-cli")
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorIs(t, store.Delete("https://idp.example", "flow-cli"), deviceflow.ErrNotLoggedIn)

	require.NoError(t, store.Save(sampleEntry("https://idp.example", "flow-cli")))
	require.NoError(t, store.Delete("https://idp.example", "flow-cli"))
	_, err = store.Load("https://idp.example", "flow-cli")
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)
}

func TestStoreKeysByIssuerAndClient(t *testing.T) {
	store := newStore(t)
	require.NoError(t, store.Save(sampleEntry("https://a.example", "one")))
	require.NoError(t, store.Save(sampleEntry("https://a.example", "two")))
	require.NoError(t, store.Save(sampleEntry("https://b.example", "one")))

	got, err := store.Load("https://a.example", "two")
	require.NoError(t, err)
	require.Equal(t, "two", got.ClientID)

	_, err = store.Select("", "")
	require.ErrorIs(t, err, deviceflow.ErrAmbiguousLogin)
	_, err = store.Select("https://a.example", "")
	require.ErrorIs(t, err, deviceflow.ErrAmbiguousLogin)
	got, err = store.Select("https://b.example", "")
	require.NoError(t, err)
	require.Equal(t, "one", got.ClientID)
	_, err = store.Select("https://c.example", "")
	require.ErrorIs(t, err, deviceflow.ErrNotLoggedIn)
}

func TestStoreRefusesIncompleteEntries(t *testing.T) {
	store := newStore(t)
	require.Error(t, store.Save(deviceflow.Entry{Issuer: "https://a.example", ClientID: "c"}))
}

func TestNormalizeOrigin(t *testing.T) {
	for in, want := range map[string]string{
		"https://flowstate.example.com":      "https://flowstate.example.com",
		"HTTPS://FlowState.Example.COM/":     "https://flowstate.example.com",
		"https://flowstate.example.com:443":  "https://flowstate.example.com",
		"http://flowstate.example.com:80/":   "http://flowstate.example.com",
		"https://flowstate.example.com:8443": "https://flowstate.example.com:8443",
		"http://127.0.0.1:9233":              "http://127.0.0.1:9233",
		"http://[::1]:9233":                  "http://[::1]:9233",
		"https://[::1]:443":                  "https://[::1]",
	} {
		got, err := deviceflow.NormalizeOrigin(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}

	for _, in := range []string{
		"", "flowstate.example.com", "localhost:9233", "ftp://flowstate.example.com",
		"https://user@flowstate.example.com", "https://user:pw@flowstate.example.com",
		"https://flowstate.example.com/path", "https://flowstate.example.com/?q=1",
		"https://flowstate.example.com/#frag", "https://",
	} {
		_, err := deviceflow.NormalizeOrigin(in)
		require.Error(t, err, in)
	}
}

func TestStoreKeepsTheServerOrigin(t *testing.T) {
	store := newStore(t)
	require.NoError(t, store.Save(sampleEntry("https://idp.example", "flow-cli")))
	got, err := store.Load("https://idp.example", "flow-cli")
	require.NoError(t, err)
	require.Equal(t, "https://flowstate.example.com", got.ServerOrigin)
}
