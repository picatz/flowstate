package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

func TestConfigureDevAuthenticationBuildsAnOfflineReusableTrustContract(t *testing.T) {
	authn, err := configureDevAuthentication(devFlags{auth: true}, "127.0.0.1:9233")
	require.NoError(t, err)
	dir := filepath.Dir(authn.keyPath)
	t.Cleanup(authn.cleanup)

	require.True(t, authn.enabled)
	require.Equal(t, "http://127.0.0.1:9233", authn.resource)
	require.FileExists(t, authn.keyPath)
	require.FileExists(t, authn.jwksPath)
	require.FileExists(t, authn.policyPath)

	policyData, err := os.ReadFile(authn.policyPath)
	require.NoError(t, err)
	policy, err := auth.ParsePolicy(policyData)
	require.NoError(t, err)
	require.Equal(t, authn.jwksPath, policy.Issuers[0].JWKSFile)
	require.Empty(t, policy.Issuers[0].JWKSURL)
	require.Equal(t, filepath.Join(dir, "token.jwt"), authn.tokenPath())
	require.Contains(t, authn.tokenCommand(), "> "+shellArg(authn.tokenPath()))
	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm(),
			"the advertised token target must stay unreadable through its parent even under umask 022")
	}

	token, _, err := runJWTSignInto(t,
		"key", authn.keyPath,
		"id", devAuthKeyID,
		"issuer", authn.issuer,
		"subject", authn.subject,
		"audience", authn.resource,
		"claim", "namespace="+authn.namespace,
	)
	require.NoError(t, err)
	principal, err := authn.verifier.Verify(t.Context(), strings.TrimSpace(token))
	require.NoError(t, err)
	require.Equal(t, devAuthSubject, principal.Subject)
	require.Equal(t, devAuthNamespace, principal.Namespace)

	wrongAudience, _, err := runJWTSignInto(t,
		"key", authn.keyPath,
		"id", devAuthKeyID,
		"issuer", authn.issuer,
		"subject", authn.subject,
		"audience", "http://127.0.0.1:9999",
		"claim", "namespace="+authn.namespace,
	)
	require.NoError(t, err)
	_, err = authn.verifier.Verify(t.Context(), strings.TrimSpace(wrongAudience))
	require.ErrorIs(t, err, auth.ErrInvalidAudience)

	authn.cleanup()
	require.NoDirExists(t, dir, "ephemeral authentication material must leave with the stack")
}

func TestConfigureDevAuthenticationReusesTheDatabaseSidecarKey(t *testing.T) {
	database := filepath.Join(t.TempDir(), "flowstate`literal`.db")
	first, err := configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9233")
	require.NoError(t, err)
	firstKey, err := os.ReadFile(first.keyPath)
	require.NoError(t, err)
	require.Contains(t, first.tokenCommand(), "--key '"+first.keyPath+"'",
		"the copyable command must quote command substitution in the generated key path")
	require.Contains(t, first.tokenCommand(), "> '"+first.tokenPath()+"'",
		"the copyable command must quote command substitution in the generated token path")

	second, err := configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9444")
	require.NoError(t, err)
	secondKey, err := os.ReadFile(second.keyPath)
	require.NoError(t, err)

	require.Equal(t, first.keyPath, second.keyPath)
	require.Equal(t, firstKey, secondKey, "restarting a persistent dev stack must not invalidate its tokens by rotating silently")
	require.NotEqual(t, first.resource, second.resource, "the audience must follow the endpoint actually bound")
}

func TestConfigureDevAuthenticationDerivesTrustFromARestoredKey(t *testing.T) {
	database := filepath.Join(t.TempDir(), "flowstate.db")
	dir, _, err := devAuthDirectory(database)
	require.NoError(t, err)
	private, err := generatePrivateKey(jwa.EdDSA)
	require.NoError(t, err)
	require.NoError(t, writePrivateKeyPEM(filepath.Join(dir, "signing-key.pem"), private))

	authn, err := configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9233")
	require.NoError(t, err)
	policyData, err := os.ReadFile(authn.policyPath)
	require.NoError(t, err)
	policy, err := auth.ParsePolicy(policyData)
	require.NoError(t, err)
	require.Equal(t, []jwa.Algorithm{jwa.EdDSA}, policy.Issuers[0].Algorithms,
		"the verifier policy must admit the algorithm the reused key actually signs with")

	token, _, err := runJWTSignInto(t,
		"key", authn.keyPath,
		"id", devAuthKeyID,
		"issuer", authn.issuer,
		"subject", authn.subject,
		"audience", authn.resource,
		"claim", "namespace="+authn.namespace,
	)
	require.NoError(t, err)
	_, err = authn.verifier.Verify(t.Context(), strings.TrimSpace(token))
	require.NoError(t, err)
}

func TestConfigureDevAuthenticationRefusesAReusableKeyWithLoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows file permissions are ACL-based")
	}

	database := filepath.Join(t.TempDir(), "flowstate.db")
	first, err := configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9233")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(first.keyPath, 0o644))

	_, err = configureDevAuthentication(devFlags{auth: true, db: database}, "127.0.0.1:9233")
	require.Error(t, err)
	require.ErrorContains(t, err, "want 0600")
}
