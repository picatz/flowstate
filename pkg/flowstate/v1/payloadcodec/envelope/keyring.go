package envelope

import (
	"bytes"
	"context"
	"crypto/fips140"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/picatz/flowstate/internal/strictyaml"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/hpke"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

// MaxConfigBytes bounds a keyring configuration file.
const MaxConfigBytes = 64 << 10

// MaxKeyFileBytes bounds a key file. A local key is 45 bytes of base64 and a
// newline, and the largest HPKE private key a few kilobytes; anything much
// larger is not a key file.
const MaxKeyFileBytes = 16 << 10

// ParseConfig decodes and checks a keyring configuration, a
// [v1.PayloadKeyring] written as YAML or JSON. It reads no key material and
// contacts no provider; [Open] does that.
//
// The schema's own rules are checked first (protovalidate), then the one rule
// that spans the whole keyring and so cannot be written on a single message:
// a key id names exactly one key, across every namespace and every escrow key.
func ParseConfig(data []byte) (*v1.PayloadKeyring, error) {
	if len(data) > MaxConfigBytes {
		return nil, fmt.Errorf("envelope: keyring configuration is %d bytes, over the %d byte limit", len(data), MaxConfigBytes)
	}

	cfg := &v1.PayloadKeyring{}
	if err := strictyaml.UnmarshalProto(data, cfg); err != nil {
		return nil, fmt.Errorf("envelope: keyring configuration: %w", err)
	}
	if err := check(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func check(cfg *v1.PayloadKeyring) error {
	if err := v1.Validate(cfg); err != nil {
		return fmt.Errorf("envelope: keyring configuration: %w", err)
	}

	seen := map[string]string{}
	for _, k := range cfg.GetEscrowKeys() {
		seen[k.GetId()] = "the escrow keys"
	}
	for _, ns := range slices.Sorted(maps.Keys(cfg.GetNamespaces())) {
		n := cfg.GetNamespaces()[ns]
		if len(n.GetKeys()) == 0 && len(n.GetEscrow()) == 0 {
			return fmt.Errorf("envelope: namespace %q has no keys and no escrow keys, so nothing could read it", ns)
		}
		for _, k := range n.GetKeys() {
			if other, dup := seen[k.GetId()]; dup {
				return fmt.Errorf("envelope: key id %q is configured for namespace %q and again for %s: a key id "+
					"names exactly one key across the whole keyring, so a payload's key id alone says which "+
					"key wrapped it", k.GetId(), ns, other)
			}
			seen[k.GetId()] = fmt.Sprintf("namespace %q", ns)
		}
	}
	return nil
}

// Keyring holds a codec per Temporal namespace, and a decode-only reader over
// all of them, sharing one cache of unwrapped data keys.
type Keyring struct {
	byNamespace map[string]*Codec
	reader      *Codec
}

// OpenOptions controls how [Open] reads key material and reaches providers.
type OpenOptions struct {
	// BaseDir resolves relative file paths. Empty uses the working directory.
	BaseDir string

	// Getenv reads environment variables. Nil uses [os.Getenv].
	Getenv func(string) string

	// now, for tests.
	now func() time.Time
}

// Open reads every configured key, asks every provider to describe its keys,
// and wraps each writing namespace's first data key. Anything that fails is an
// error: a keyring that came up without one of its keys, or with a provider it
// cannot reach, would refuse that key's history at the first read or write,
// long after startup. ctx bounds the startup calls.
func Open(ctx context.Context, cfg *v1.PayloadKeyring, opts OpenOptions) (*Keyring, error) {
	if err := check(cfg); err != nil {
		return nil, err
	}
	if opts.Getenv == nil {
		opts.Getenv = os.Getenv
	}
	if opts.now == nil {
		opts.now = time.Now
	}

	providers, err := openProviders(cfg.GetProviders(), opts)
	if err != nil {
		return nil, err
	}
	loader := keyLoader{opts: opts, providers: providers}

	escrow := map[string]Recipient{}
	for _, kc := range cfg.GetEscrowKeys() {
		key, err := loader.load(kc)
		if err != nil {
			return nil, fmt.Errorf("envelope: escrow %w", err)
		}
		escrow[kc.GetId()] = Recipient{ID: kc.GetId(), Key: key}
	}

	// One cache for every codec, sized by the most generous namespace. Each
	// entry expires by its own namespace's window, so one namespace's short
	// revocation delay is not stretched by another's long one.
	entries := 0
	for _, n := range cfg.GetNamespaces() {
		entries = max(entries, int(n.GetDataKey().GetDecodeCacheEntries()))
	}
	cache := newDecodeCache(entries, opts.now)

	kr := &Keyring{byNamespace: map[string]*Codec{}}
	for _, ns := range slices.Sorted(maps.Keys(cfg.GetNamespaces())) {
		n := cfg.GetNamespaces()[ns]
		o := Options{
			Binding:           ns,
			Current:           n.GetCurrent(),
			Suite:             n.GetSuite(),
			DecryptSuites:     n.GetDecryptSuites(),
			AcceptUnencrypted: n.GetAcceptUnencrypted(),
			DataKey:           n.GetDataKey(),
			now:               opts.now,
			cache:             cache,
		}
		for _, kc := range n.GetKeys() {
			key, err := loader.load(kc)
			if err != nil {
				return nil, fmt.Errorf("envelope: namespace %q: %w", ns, err)
			}
			o.Keys = append(o.Keys, Recipient{ID: kc.GetId(), Key: key})
		}
		for _, id := range n.GetEscrow() {
			o.Escrow = append(o.Escrow, escrow[id])
		}
		codec, err := New(ctx, o)
		if err != nil {
			return nil, fmt.Errorf("envelope: namespace %q: %w", ns, err)
		}
		kr.byNamespace[ns] = codec
	}

	kr.reader = newReader(kr.byNamespace, cache)
	return kr, nil
}

// newReader is the decode-only codec over every namespace's own keys, each
// opened against its own namespace and suites.
func newReader(byNamespace map[string]*Codec, cache *decodeCache) *Codec {
	// It accepts unencrypted payloads only if every namespace does. It reads
	// for all of them, so the strictest answer is the only one that cannot
	// weaken a namespace that requires encryption.
	r := &Codec{ring: map[string]ringEntry{}, escrow: map[string]ringEntry{}, acceptUnencrypted: true, cache: cache,
		timeout: DefaultProviderTimeout}
	for _, c := range byNamespace {
		maps.Copy(r.ring, c.ring)
		r.acceptUnencrypted = r.acceptUnencrypted && c.acceptUnencrypted
		r.timeout = max(r.timeout, c.timeout)
		r.now = c.now
	}
	r.envelopeSize = maxMetadataSize()
	r.maxHeader = r.maxHeaderSize()
	return r
}

// LoadFile parses the keyring configuration at path and opens it, resolving
// relative paths against the file's directory.
func LoadFile(ctx context.Context, path string) (*Keyring, error) {
	data, err := readBounded(path, MaxConfigBytes)
	if err != nil {
		return nil, fmt.Errorf("envelope: reading keyring configuration %q: %w", path, err)
	}
	cfg, err := ParseConfig(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return Open(ctx, cfg, OpenOptions{BaseDir: filepath.Dir(path)})
}

// keyLoader turns one configured key into a [keyprovider.Key].
type keyLoader struct {
	opts      OpenOptions
	providers map[string]vaultConnection
}

func (l keyLoader) load(kc *v1.PayloadKey) (keyprovider.Key, error) {
	switch src := kc.GetSource().(type) {
	case *v1.PayloadKey_File, *v1.PayloadKey_Env:
		text, err := l.material(kc.GetFile(), kc.GetEnv(), true)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", kc.GetId(), err)
		}
		defer clear(text)
		key, err := local.Parse(text)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", kc.GetId(), err)
		}
		return key, nil

	case *v1.PayloadKey_Hpke:
		pub := src.Hpke.GetPublicKey()
		public, err := l.material(pub.GetFile(), pub.GetEnv(), false)
		if err != nil {
			return nil, fmt.Errorf("key %q: public key: %w", kc.GetId(), err)
		}
		var private []byte
		if priv := src.Hpke.GetPrivateKey(); priv != nil {
			if private, err = l.material(priv.GetFile(), priv.GetEnv(), true); err != nil {
				return nil, fmt.Errorf("key %q: private key: %w", kc.GetId(), err)
			}
			defer clear(private)
		}
		key, err := hpke.Parse(public, private)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", kc.GetId(), err)
		}
		return key, nil

	case *v1.PayloadKey_Vault:
		conn, ok := l.providers[src.Vault.GetProvider()]
		if !ok {
			// ParseConfig's rules refuse this; Open is also reachable with a
			// configuration built in code.
			return nil, fmt.Errorf("key %q: no provider named %q", kc.GetId(), src.Vault.GetProvider())
		}
		return conn.key(src.Vault.GetKey()), nil

	default:
		return nil, fmt.Errorf("key %q: no source", kc.GetId())
	}
}

// material reads key text from a file or an environment variable. A file
// holding secret material must be accessible by its owner only.
func (l keyLoader) material(file, env string, secret bool) ([]byte, error) {
	if env != "" {
		text := l.opts.Getenv(env)
		if text == "" {
			return nil, fmt.Errorf("environment variable %s is unset or empty", env)
		}
		return []byte(text), nil
	}
	path := l.resolve(file)
	if secret {
		if err := checkKeyFileMode(path); err != nil {
			return nil, err
		}
	}
	text, err := readBounded(path, MaxKeyFileBytes)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", path, err)
	}
	return text, nil
}

func (l keyLoader) resolve(path string) string {
	if path != "" && !filepath.IsAbs(path) && l.opts.BaseDir != "" {
		return filepath.Join(l.opts.BaseDir, path)
	}
	return path
}

// checkKeyFileMode refuses a key file others can reach, the way ssh refuses a
// private key with loose permissions: a wrapping key readable by the group is
// a key the group holds.
func checkKeyFileMode(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%q is not a regular file", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%q has mode %04o; a key file must be accessible by its owner only (chmod 600)",
			path, info.Mode().Perm())
	}
	return nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, &fs.PathError{Op: "read", Path: path, Err: errors.New("not a regular file")}
	}

	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		clear(data)
		return nil, fmt.Errorf("larger than the %d byte limit", limit)
	}
	return bytes.Clone(data), nil
}

// Namespaces lists the Temporal namespaces the keyring protects.
func (k *Keyring) Namespaces() []string { return slices.Sorted(maps.Keys(k.byNamespace)) }

// Codec is the codec for one Temporal namespace.
func (k *Keyring) Codec(namespace string) (*Codec, bool) {
	c, ok := k.byNamespace[namespace]
	return c, ok
}

// Reader is a decode-only codec over every namespace's own keys, each opened
// against its own namespace. It is for a process that reads what several
// namespaces wrote, such as `flow server` reading its own memos across a
// tenant pool, and must never be given to a client that writes; its Encode
// refuses. Because it holds every namespace's keys, it cannot tell a payload
// moved between two of them from one that was written in place. It does not
// read through escrow keys: without the primary key it cannot say which
// namespace a payload belongs to, so a recovery process reads through each
// namespace's own codec instead.
func (k *Keyring) Reader() *Codec { return k.reader }

// PayloadCodecConfig is the keyring as the codec slot every Flowstate process
// is configured with: a codec per namespace for clients, and the reader for
// everything else.
func (k *Keyring) PayloadCodecConfig() payloadcodec.Config {
	byNS := make(map[string]payloadcodec.Codec, len(k.byNamespace))
	for ns, c := range k.byNamespace {
		byNS[ns] = c
	}
	return payloadcodec.Config{Codec: k.reader, Namespaces: byNS}
}

// Status reports the keyring's namespaces, suites, data key bounds, and keys
// by id, kind and fingerprint, without material.
func (k *Keyring) Status() *v1.PayloadEncryptionStatus {
	status := &v1.PayloadEncryptionStatus{Enabled: true, Fips140: fips140.Enabled()}
	for _, ns := range k.Namespaces() {
		c := k.byNamespace[ns]
		n := &v1.PayloadEncryptionNamespaceStatus{
			Namespace:         ns,
			CurrentKeyId:      c.CurrentKeyID(),
			AcceptUnencrypted: c.AcceptsUnencrypted(),
			Suite:             c.Suite(),
			DecryptSuites:     c.DecryptSuites(),
			DataKey:           c.policy.proto(),
		}
		for _, key := range c.Keys() {
			n.Keys = append(n.Keys, &v1.PayloadKeyStatus{
				Id: key.ID, Kind: key.Kind, Fingerprint: key.Fingerprint, Current: key.Current,
				Escrow: key.Escrow, CanUnwrap: key.CanUnwrap, Version: key.Version,
			})
		}
		status.Namespaces = append(status.Namespaces, n)
	}
	return status
}
