package secrets_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"
)

// Example shows what a reference is: text that names a secret and carries no way
// to obtain one. This is the only form a secret takes in a Flowfile, in the
// compiled specification, and in workflow history.
func Example() {
	ref, err := secrets.ParseRef("env:API_KEY")
	if err != nil {
		panic(err)
	}

	// A reference is safe to log. There is no method on it that returns a value,
	// so no workflow-side code can turn it into one.
	fmt.Println("provider:", ref.GetScheme())
	fmt.Println("name:    ", ref.GetName())
	fmt.Println("text:    ", secrets.RefString(ref))

	// Output:
	// provider: env
	// name:     API_KEY
	// text:     env:API_KEY
}

// Example_redaction shows that a resolved value does not escape through the paths
// that leak credentials by accident.
func Example_redaction() {
	secret := secrets.NewSecret(secrets.NewRef("env", "API_KEY"), "tok-live-9f8e7d")

	for _, verb := range []string{"%v", "%s", "%q", "%#v"} {
		fmt.Println(verb, "->", fmt.Sprintf(verb, secret))
	}

	fmt.Println("in an error:", fmt.Errorf("request failed: %v", secret))
	fmt.Println("length:", secret.Len())

	// Reaching the value takes a deliberate call.
	fmt.Println("revealed:", secret.Reveal())

	// Output:
	// %v -> [REDACTED]
	// %s -> [REDACTED]
	// %q -> "[REDACTED]"
	// %#v -> [REDACTED]
	// in an error: request failed: [REDACTED]
	// length: 15
	// revealed: tok-live-9f8e7d
}

// Example_scrubber shows the backstop for code that never saw a [secrets.Secret]:
// an HTTP client putting a token from the URL into its error message.
func Example_scrubber() {
	secret := secrets.NewSecret(secrets.NewRef("env", "TOKEN"), "tok-live-9f8e7d")

	scrubber := secrets.NewScrubber(secret)

	// The sort of error a client library produces, with the credential in it.
	err := fmt.Errorf(`Get "https://api.example.com/?token=%s": connection refused`, secret.Reveal())

	fmt.Println("unscrubbed:", err)
	fmt.Println("scrubbed:  ", scrubber.ScrubError(err))

	// Output:
	// unscrubbed: Get "https://api.example.com/?token=tok-live-9f8e7d": connection refused
	// scrubbed:   Get "https://api.example.com/?token=[REDACTED]": connection refused
}

// vaultProvider shows that a new source of secrets is an implementation of
// [secrets.Provider], not a change to anything else. A real one would call out to
// a vault; this one stands in for it.
type vaultProvider struct {
	values map[string]string
}

func (p *vaultProvider) Scheme() string { return "vault" }

func (p *vaultProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	// The namespace scopes the lookup: two tenants naming one reference must not
	// reach the same secret. Every provider owes this.
	value, ok := p.values[req.Namespace+"/"+req.Ref.GetName()]
	if !ok {
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: req.Ref,
			Err: fmt.Errorf("%w: no such path", secrets.ErrNotFound),
		}
	}

	return secrets.NewSecret(req.Ref, value), nil
}

// Example_customProvider shows registering an additional provider, and that a
// worker reports what it can resolve without reporting any values.
func Example_customProvider() {
	env, err := secrets.NewEnvProvider()
	if err != nil {
		panic(err)
	}

	store, err := secrets.NewStore(
		env,
		secrets.NewCache(&vaultProvider{
			values: map[string]string{"/apps/flowstate/token": "vault-value"},
		}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("configured providers:", store.Schemes())

	// A resolver is bound to a namespace, which is how a value is reached at all:
	// a single-tenant deployment binds the empty namespace.
	resolver, err := store.For(nil)
	if err != nil {
		panic(err)
	}

	secret, err := resolver.Resolve(context.Background(), secrets.NewRef("vault", "apps/flowstate/token"))
	if err != nil {
		panic(err)
	}

	fmt.Println("resolved:", secret, "of length", secret.Len())

	// A reference the worker cannot resolve says so, and says what it can.
	_, err = resolver.Resolve(context.Background(), secrets.NewRef("gcpsm", "x"))
	fmt.Println("unknown provider:", errors.Is(err, secrets.ErrUnknownScheme))

	// Output:
	// configured providers: [env vault]
	// resolved: [REDACTED] of length 11
	// unknown provider: true
}

// Test_compiledSecretRefIsARef proves the boundary against the real generated type
// rather than a stand-in: the schema's SecretRef satisfies [secrets.Ref] with no
// conversion, no adapter, and no import of the engine by this package.
//
// This lives in the external test package on purpose. The secrets package itself
// must not import flowstatev1 — the task library imports secrets, so that would be
// a cycle — but an external test package is a separate compilation unit and can
// import both, which is exactly what makes this checkable here.
func Test_compiledSecretRefIsARef(t *testing.T) {
	t.Setenv("FLOWSTATE_SECRET_API_KEY", "from-the-compiled-schema")

	env, err := secrets.NewEnvProvider()
	require.NoError(t, err)

	store, err := secrets.NewStore(env)
	require.NoError(t, err)

	resolver, err := store.For(nil)
	require.NoError(t, err)

	// The shape the task library will use: a Value carrying a SecretRef, passed
	// straight through with nothing in between.
	value := &flowstatev1.Value{
		Kind: &flowstatev1.Value_SecretRef{
			SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "API_KEY"},
		},
	}

	secret, err := resolver.Resolve(t.Context(), value.GetSecretRef())
	require.NoError(t, err)
	require.Equal(t, "from-the-compiled-schema", secret.Reveal())
	require.Equal(t, "env:API_KEY", secrets.RefString(secret.Ref()))

	t.Run("a reference the schema would reject is refused in code too", func(t *testing.T) {
		// protovalidate rejects these at the boundary, but a message built in Go
		// never passes through it, so the same checks run here.
		for _, ref := range []*flowstatev1.SecretRef{
			{Scheme: "", Name: "API_KEY"},
			{Scheme: "ENV", Name: "API_KEY"},
			{Scheme: "env", Name: ""},
			{Scheme: "env", Name: "API\nKEY"},
		} {
			_, err := resolver.Resolve(t.Context(), ref)
			require.ErrorIs(t, err, secrets.ErrInvalidRef, "ref %q", secrets.RefString(ref))
		}
	})

	t.Run("an absent secret_ref yields a nil reference, not a panic", func(t *testing.T) {
		// GetSecretRef on a Value of another kind returns a typed nil, and the
		// generated getters are nil-receiver safe, so it reads as empty and fails
		// validation rather than dereferencing.
		other := &flowstatev1.Value{}

		_, err := resolver.Resolve(t.Context(), other.GetSecretRef())
		require.ErrorIs(t, err, secrets.ErrInvalidRef)
	})
}

// Test_scrubbedErrorSurvivesTemporalHistory is the regression test for the leak
// this package exists to prevent, checked against the machinery that actually
// records it.
//
// Temporal's failure converter walks an error's whole chain with errors.Unwrap and
// writes every level's message into the Failure it persists. An error that redacts
// only its own message but exposes the original through Unwrap therefore still puts
// the value into durable, replayable history. Asserting on err.Error() alone does
// not catch that; converting the error exactly as the worker will does.
func Test_scrubbedErrorSurvivesTemporalHistory(t *testing.T) {
	const value = "tok-live-history-7d3e"

	scrubber := secrets.NewScrubber(
		secrets.NewSecret(secrets.NewRef("env", "TOKEN"), value),
	)

	// The shape a real client library produces, wrapped as an activity would wrap it.
	inner := fmt.Errorf(`Post "https://api.example.com/v1?token=%s": 401 Unauthorized`, value)
	activityErr := fmt.Errorf("step %q: %w", "fetch", scrubber.ScrubError(inner))

	failure := temporal.GetDefaultFailureConverter().ErrorToFailure(activityErr)
	require.NotNil(t, failure)

	// Walk the cause chain the converter produced, which is what lands in history.
	for level, f := 0, failure; f != nil; level, f = level+1, f.GetCause() {
		require.NotContains(t, f.GetMessage(), value,
			"failure level %d carries the secret into workflow history", level)
		require.NotContains(t, f.GetStackTrace(), value,
			"failure level %d stack trace carries the secret", level)
		require.Less(t, level, 10, "failure chain did not terminate")
	}

	// The whole serialized proto, so nothing hides in a field not walked above.
	marshaled, err := protojson.Marshal(failure)
	require.NoError(t, err)
	require.NotContains(t, string(marshaled), value)

	// The diagnosable part still survives, or scrubbing would have cost too much.
	require.Contains(t, string(marshaled), "401 Unauthorized")
	require.Contains(t, string(marshaled), secrets.Redacted)
}

// Test_activityPattern exercises the whole sequence a task activity performs, as a
// consumer of the package: resolve worker-side, use the value, and make sure
// nothing the activity returns carries it.
//
// This is the pattern the task library should follow.
func Test_activityPattern(t *testing.T) {
	const value = "tok-live-e2e-4c1f"

	// Worker startup: a mounted secret directory, as Kubernetes would provide.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "api-key"), []byte(value+"\n"), 0o600))

	files, err := secrets.NewFileProvider(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, files.Close()) })

	env, err := secrets.NewEnvProvider()
	require.NoError(t, err)

	store, err := secrets.NewStore(env, secrets.NewCache(files))
	require.NoError(t, err)

	resolver, err := store.For(nil)
	require.NoError(t, err)

	// Compile time: the Flowfile names a reference, which is all it ever holds.
	ref, err := secrets.ParseRef("file:api-key")
	require.NoError(t, err)

	// Activity, worker-side: the one place a value exists.
	activity := func(ctx context.Context, ref secrets.Ref) (output string, err error) {
		secret, err := resolver.Resolve(ctx, ref)
		if err != nil {
			return "", err
		}

		scrubber := secrets.NewScrubber(secret)

		// Stand-in for a client library that embeds the credential in its error.
		callErr := fmt.Errorf(
			`Post "https://api.example.com/v1?token=%s": 401 Unauthorized`,
			secret.Reveal(),
		)

		return "", scrubber.ScrubError(callErr)
	}

	_, err = activity(t.Context(), ref)

	require.Error(t, err)
	require.NotContains(t, err.Error(), value,
		"an activity error is recorded in workflow history, so it must not carry the value")
	require.Contains(t, err.Error(), secrets.Redacted)
	require.Contains(t, err.Error(), "401 Unauthorized", "the diagnosable part survives")

	// The trailing newline the file carried is gone, so the credential is usable.
	secret, err := resolver.Resolve(t.Context(), ref)
	require.NoError(t, err)
	require.Equal(t, value, secret.Reveal())

	// A missing reference is safe to surface as-is.
	_, err = resolver.Resolve(t.Context(), secrets.NewRef("file", "absent"))
	require.ErrorIs(t, err, secrets.ErrNotFound)
	require.Contains(t, err.Error(), "file:absent")
	require.NotContains(t, err.Error(), value)
}
