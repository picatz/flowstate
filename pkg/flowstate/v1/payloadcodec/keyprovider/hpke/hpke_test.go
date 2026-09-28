package hpke_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/hpke"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/keyprovidertest"
)

func TestConformance(t *testing.T) {
	t.Parallel()

	for _, kem := range []uint16{hpke.DefaultKEM, 0x0050, 0x0020} {
		private, public, err := hpke.Generate(kem)
		require.NoError(t, err)
		k, err := hpke.Parse(public, private)
		require.NoError(t, err)
		t.Run(strings.SplitN(string(public), ":", 3)[1], func(t *testing.T) {
			t.Parallel()
			keyprovidertest.Run(t, k)
		})
	}
}

// TestThePublicHalfWrapsAndCannotUnwrap is escrow's premise: a worker
// configured with only the public key adds a recovery path it cannot use.
func TestThePublicHalfWrapsAndCannotUnwrap(t *testing.T) {
	t.Parallel()

	private, public, err := hpke.Generate(0)
	require.NoError(t, err)
	worker, err := hpke.Parse(public, nil)
	require.NoError(t, err)
	recovery, err := hpke.Parse(public, private)
	require.NoError(t, err)

	info, err := worker.Describe(t.Context())
	require.NoError(t, err)
	require.True(t, info.CanWrap)
	require.False(t, info.CanUnwrap)

	ectx := keyprovider.Context{Namespace: "ns", KeyID: "k1", Suite: 1}
	dk := bytes.Repeat([]byte{9}, keyprovider.DataKeyBytes)
	w, err := worker.Wrap(t.Context(), dk, ectx)
	require.NoError(t, err)
	_, err = worker.Unwrap(t.Context(), w, ectx)
	require.ErrorIs(t, err, keyprovider.ErrCannotUnwrap)

	got, err := recovery.Unwrap(t.Context(), w, ectx)
	require.NoError(t, err)
	require.Equal(t, dk, got)
}

func TestTheDefaultIsPostQuantumHybrid(t *testing.T) {
	t.Parallel()

	_, public, err := hpke.Generate(0)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(public), "flowstate-hpke-v1:647a:"), "ML-KEM-768 + X25519 by default")
	k, err := hpke.Parse(public, nil)
	require.NoError(t, err)
	info, err := k.Describe(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1120+32+16, info.MaxWrappedBytes, "encapsulated key, data key, tag")
}

func TestKeysAreRefusedWithoutEchoingThem(t *testing.T) {
	t.Parallel()

	private, public, err := hpke.Generate(0)
	require.NoError(t, err)
	otherPrivate, _, err := hpke.Generate(0)
	require.NoError(t, err)
	_, x25519Public, err := hpke.Generate(0x0020)
	require.NoError(t, err)

	for name, tc := range map[string]struct{ public, private []byte }{
		"garbage":                       {public: []byte("nope")},
		"private as public":             {public: private},
		"unknown KEM":                   {public: []byte("flowstate-hpke-v1:9999:AAAA")},
		"not base64":                    {public: []byte("flowstate-hpke-v1:647a:!!!!")},
		"truncated key":                 {public: public[:len(public)/2]},
		"private for another public":    {public: public, private: otherPrivate},
		"private for another KEM":       {public: x25519Public, private: private},
		"oversized":                     {public: bytes.Repeat([]byte("a"), 32<<10)},
		"public prefix on private text": {public: public, private: public},
	} {
		_, err := hpke.Parse(tc.public, tc.private)
		require.Error(t, err, name)
		secret := strings.TrimSpace(strings.SplitN(string(private), ":", 3)[2])
		require.NotContains(t, err.Error(), secret[:16], name)
		require.False(t, errors.Is(err, nil), name)
	}
}
