// Package keyprovidertest is the conformance suite every key provider passes:
// the behaviour the envelope relies on, asserted the same way for a local key,
// an HPKE recipient, a Vault Transit key, and whatever comes next.
package keyprovidertest

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
)

// Run asserts that key behaves as a [keyprovider.Key] must. key must be able
// to wrap and unwrap.
func Run(t *testing.T, key keyprovider.Key) {
	t.Helper()

	ectx := keyprovider.Context{Namespace: "tenant-a", KeyID: "k1", Suite: 1}

	info, err := key.Describe(t.Context())
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if !info.CanWrap || !info.CanUnwrap {
		t.Fatalf("Describe: the suite needs a key that wraps and unwraps, got %+v", info)
	}
	if info.Kind == "" || info.MaxWrappedBytes <= 0 || info.MaxWrappedBytes > keyprovider.MaxWrappedBytes {
		t.Fatalf("Describe: kind %q, max wrapped %d", info.Kind, info.MaxWrappedBytes)
	}

	dataKey := make([]byte, keyprovider.DataKeyBytes)
	_, _ = rand.Read(dataKey)

	t.Run("round trip", func(t *testing.T) {
		w, err := key.Wrap(t.Context(), dataKey, ectx)
		if err != nil {
			t.Fatalf("Wrap: %v", err)
		}
		if len(w.Bytes) == 0 || len(w.Bytes) > info.MaxWrappedBytes {
			t.Fatalf("Wrap: %d bytes, declared at most %d", len(w.Bytes), info.MaxWrappedBytes)
		}
		if bytes.Contains(w.Bytes, dataKey) {
			t.Fatal("Wrap: the data key appears in the wrapped bytes")
		}
		got, err := key.Unwrap(t.Context(), w, ectx)
		if err != nil {
			t.Fatalf("Unwrap: %v", err)
		}
		if !bytes.Equal(got, dataKey) {
			t.Fatal("Unwrap: a different data key came back")
		}
	})

	t.Run("each wrap is fresh", func(t *testing.T) {
		a, errA := key.Wrap(t.Context(), dataKey, ectx)
		b, errB := key.Wrap(t.Context(), dataKey, ectx)
		if errA != nil || errB != nil {
			t.Fatalf("Wrap: %v, %v", errA, errB)
		}
		if bytes.Equal(a.Bytes, b.Bytes) {
			t.Fatal("two wraps of one data key are identical, so wrapping is deterministic")
		}
	})

	t.Run("the context is bound", func(t *testing.T) {
		w, err := key.Wrap(t.Context(), dataKey, ectx)
		if err != nil {
			t.Fatalf("Wrap: %v", err)
		}
		for name, other := range map[string]keyprovider.Context{
			"namespace": {Namespace: "tenant-b", KeyID: ectx.KeyID, Suite: ectx.Suite},
			"key id":    {Namespace: ectx.Namespace, KeyID: "k2", Suite: ectx.Suite},
			"suite":     {Namespace: ectx.Namespace, KeyID: ectx.KeyID, Suite: 2},
		} {
			got, err := key.Unwrap(t.Context(), w, other)
			if !errors.Is(err, keyprovider.ErrInvalidWrapped) {
				t.Errorf("another %s: unwrapped (%d bytes) or wrong error: %v", name, len(got), err)
			}
			checkQuiet(t, err, dataKey, w.Bytes)
		}
	})

	t.Run("tampering is refused", func(t *testing.T) {
		w, err := key.Wrap(t.Context(), dataKey, ectx)
		if err != nil {
			t.Fatalf("Wrap: %v", err)
		}
		for _, i := range []int{0, len(w.Bytes) / 2, len(w.Bytes) - 1} {
			tampered := keyprovider.Wrapped{Bytes: bytes.Clone(w.Bytes), Version: w.Version}
			tampered.Bytes[i] ^= 0x01
			got, err := key.Unwrap(t.Context(), tampered, ectx)
			if err == nil {
				t.Errorf("byte %d flipped: unwrapped %d bytes", i, len(got))
			}
			checkQuiet(t, err, dataKey, w.Bytes)
		}
		for name, bad := range map[string][]byte{
			"empty":     nil,
			"truncated": w.Bytes[:len(w.Bytes)-1],
			"extended":  append(bytes.Clone(w.Bytes), 0),
		} {
			if _, err := key.Unwrap(t.Context(), keyprovider.Wrapped{Bytes: bad, Version: w.Version}, ectx); err == nil {
				t.Errorf("%s: unwrapped", name)
			}
		}
	})

	t.Run("a data key is exactly 32 bytes", func(t *testing.T) {
		for _, n := range []int{0, 16, 31, 33, 64} {
			if _, err := key.Wrap(t.Context(), make([]byte, n), ectx); err == nil {
				t.Errorf("wrapped a %d-byte data key", n)
			}
		}
	})

	t.Run("it does not format", func(t *testing.T) {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			out := fmt.Sprintf(verb, key)
			if bytes.Contains([]byte(out), dataKey) || bytes.Contains([]byte(out), []byte(hex.EncodeToString(dataKey))) {
				t.Errorf("%s printed the data key", verb)
			}
		}
	})
}

// checkQuiet fails when err quotes the data key or the wrapped bytes in any
// common encoding.
func checkQuiet(t *testing.T, err error, secrets ...[]byte) {
	t.Helper()
	if err == nil {
		return
	}
	msg := err.Error()
	for _, s := range secrets {
		for _, form := range []string{string(s), hex.EncodeToString(s), base64.StdEncoding.EncodeToString(s)} {
			if len(form) >= 8 && bytes.Contains([]byte(msg), []byte(form)) {
				t.Errorf("an error quotes secret or wrapped bytes: %q", msg)
			}
		}
	}
}
