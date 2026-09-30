package local_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/keyprovidertest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

func TestConformance(t *testing.T) {
	t.Parallel()

	k, err := local.Parse(local.Generate())
	require.NoError(t, err)
	keyprovidertest.Run(t, k)
}

func TestParseRoundTripsGenerateAndNeverEchoes(t *testing.T) {
	t.Parallel()

	text := local.Generate()
	a, err := local.Parse(text)
	require.NoError(t, err)
	b, err := local.Parse(append([]byte("  "), text...))
	require.NoError(t, err, "surrounding whitespace is ignored")
	require.Equal(t, a.Fingerprint(), b.Fingerprint())

	other, err := local.Parse(local.Generate())
	require.NoError(t, err)
	require.NotEqual(t, a.Fingerprint(), other.Fingerprint())

	for _, bad := range []string{"not base64 !!", "c2hvcnQ=", strings.Repeat("A", 88)} {
		_, err := local.Parse([]byte(bad))
		require.Error(t, err)
		require.NotContains(t, err.Error(), bad)
	}
}

func TestAWrapIsSixtyBytes(t *testing.T) {
	t.Parallel()

	k, err := local.NewKey(bytes.Repeat([]byte{1}, local.KeyBytes))
	require.NoError(t, err)
	info, err := k.Describe(t.Context())
	require.NoError(t, err)
	require.Equal(t, 60, info.MaxWrappedBytes)
	require.Len(t, info.Fingerprint, 16)
}
