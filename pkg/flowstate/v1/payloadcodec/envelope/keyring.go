package envelope

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"

	"github.com/picatz/flowstate/internal/strictyaml"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// MaxConfigBytes bounds a keyring configuration file.
const MaxConfigBytes = 64 << 10

// MaxKeyFileBytes bounds a key file. A key is 45 bytes of base64 and a newline;
// anything much larger is not a key file.
const MaxKeyFileBytes = 1 << 10

// ParseConfig decodes and checks a keyring configuration, a
// [v1.PayloadKeyring] written as YAML or JSON. It reads no key material;
// [Open] does that.
//
// The schema's own rules are checked first (protovalidate), then the one rule
// that spans namespaces and so cannot be written on a single message: a key id
// names exactly one key across the whole keyring.
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
	for _, ns := range slices.Sorted(maps.Keys(cfg.GetNamespaces())) {
		for _, k := range cfg.GetNamespaces()[ns].GetKeys() {
			if other, dup := seen[k.GetId()]; dup {
				return fmt.Errorf("envelope: key id %q is configured for namespace %q and again for %q: a key id "+
					"names exactly one key in one namespace across the whole keyring, so a payload's key id "+
					"alone says which namespace it was sealed for", k.GetId(), other, ns)
			}
			seen[k.GetId()] = ns
		}
	}
	return nil
}

// Keyring holds a codec per Temporal namespace, and a decode-only reader over
// all of them.
type Keyring struct {
	byNamespace map[string]*Codec
	reader      *Codec
}

// OpenOptions controls how [Open] reads key material.
type OpenOptions struct {
	// BaseDir resolves relative key file paths. Empty uses the working
	// directory.
	BaseDir string

	// Getenv reads environment variables. Nil uses [os.Getenv].
	Getenv func(string) string
}

// Open reads every configured key and builds the keyring. Any key that cannot
// be read is an error: a keyring that came up without one of its keys would
// refuse that key's history at the first read, long after startup.
func Open(cfg *v1.PayloadKeyring, opts OpenOptions) (*Keyring, error) {
	if err := check(cfg); err != nil {
		return nil, err
	}
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}

	kr := &Keyring{byNamespace: map[string]*Codec{}}
	readerRing := map[string]ringEntry{}

	for _, ns := range slices.Sorted(maps.Keys(cfg.GetNamespaces())) {
		n := cfg.GetNamespaces()[ns]
		keys := make([]Key, 0, len(n.GetKeys()))
		for _, kc := range n.GetKeys() {
			k, err := loadKey(kc, opts.BaseDir, getenv)
			if err != nil {
				return nil, fmt.Errorf("envelope: namespace %q: %w", ns, err)
			}
			keys = append(keys, k)
		}

		codec, err := New(Options{Keys: keys, Current: n.GetCurrent(), Binding: ns, AcceptUnencrypted: n.GetAcceptUnencrypted()})
		if err != nil {
			return nil, fmt.Errorf("envelope: namespace %q: %w", ns, err)
		}
		kr.byNamespace[ns] = codec

		if err := addToRing(readerRing, ns, keys); err != nil {
			return nil, err
		}
	}

	// The reader accepts unencrypted payloads only if every namespace does.
	// It reads for all of them, so the strictest answer is the only one that
	// cannot weaken a namespace that requires encryption.
	accept := true
	for _, c := range kr.byNamespace {
		accept = accept && c.acceptUnencrypted
	}
	kr.reader = &Codec{ring: readerRing, acceptUnencrypted: accept}
	for id := range readerRing {
		kr.reader.envelopeSize = max(kr.reader.envelopeSize, envelopeSizeFor(id))
	}

	return kr, nil
}

// LoadFile parses the keyring configuration at path and opens it, resolving
// relative key paths against the file's directory.
func LoadFile(path string) (*Keyring, error) {
	data, err := readBounded(path, MaxConfigBytes)
	if err != nil {
		return nil, fmt.Errorf("envelope: reading keyring configuration %q: %w", path, err)
	}
	cfg, err := ParseConfig(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return Open(cfg, OpenOptions{BaseDir: filepath.Dir(path)})
}

func loadKey(kc *v1.PayloadKeySource, baseDir string, getenv func(string) string) (Key, error) {
	if env := kc.GetEnv(); env != "" {
		text := getenv(env)
		if text == "" {
			return Key{}, fmt.Errorf("key %q: environment variable %s is unset or empty", kc.GetId(), env)
		}
		return ParseKey(kc.GetId(), []byte(text))
	}

	path := kc.GetFile()
	if !filepath.IsAbs(path) && baseDir != "" {
		path = filepath.Join(baseDir, path)
	}
	if err := checkKeyFileMode(path); err != nil {
		return Key{}, fmt.Errorf("key %q: %w", kc.GetId(), err)
	}
	text, err := readBounded(path, MaxKeyFileBytes)
	if err != nil {
		return Key{}, fmt.Errorf("key %q: reading %q: %w", kc.GetId(), path, err)
	}
	defer clear(text)
	return ParseKey(kc.GetId(), text)
}

// checkKeyFileMode refuses a key file others can reach, the way ssh refuses a
// private key with loose permissions: a payload key readable by the group is a
// key the group holds.
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

// Codec is the codec for one Temporal namespace, which seals under that
// namespace's current key and opens only its keys' payloads.
func (k *Keyring) Codec(namespace string) (*Codec, bool) {
	c, ok := k.byNamespace[namespace]
	return c, ok
}

// Reader is a decode-only codec over every namespace's keys, each opened
// against its own namespace. It is for a process that reads what several
// namespaces wrote, such as `flow server` reading its own memos across a
// tenant pool, and must never be given to a client that writes; its Encode
// refuses. Because it holds every namespace's keys, it cannot tell a payload
// moved between two of them from one that was written in place.
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

// Status reports the keyring's namespaces and keys by id and fingerprint,
// without material.
func (k *Keyring) Status() *v1.PayloadEncryptionStatus {
	status := &v1.PayloadEncryptionStatus{Enabled: true}
	for _, ns := range k.Namespaces() {
		c := k.byNamespace[ns]
		n := &v1.PayloadEncryptionNamespaceStatus{
			Namespace:         ns,
			CurrentKeyId:      c.CurrentKeyID(),
			AcceptUnencrypted: c.AcceptsUnencrypted(),
		}
		for _, key := range c.Keys() {
			n.Keys = append(n.Keys, &v1.PayloadKeyStatus{Id: key.ID, Fingerprint: key.Fingerprint, Current: key.Current})
		}
		status.Namespaces = append(status.Namespaces, n)
	}
	return status
}
