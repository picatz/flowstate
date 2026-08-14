package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// This file exists because a String method is not enough.
//
// fmt cannot call a method on a value it reaches through an unexported field: the
// reflected value is read-only and cannot be turned back into an interface, so fmt
// falls back to printing the fields it can see. A secret held in a plain field is
// therefore printed in full by %v on any struct that happens to contain it, even
// though the secret's own type redacts itself perfectly when printed directly.
//
// `type taskState struct{ cred auth.Credential }` and a debug log of it is all it
// takes, which is not an exotic mistake. So the material in these types is held in
// a closure: a func field is opaque to fmt, which prints it as an address, and no
// verb reaches inside it.
//
// %#v is the other gap a String method leaves, and is covered by GoString.
//
// Credit to the netpolicy-builder agent, who found this and demonstrated it.

// secretHolder contains every secret-bearing value this package produces, in
// unexported fields, which is the shape that defeats a String method.
type secretHolder struct {
	credential auth.Credential
	assertion  auth.Assertion
	key        auth.SigningKey
}

// nested holds a holder, so the fallback is reached at more than one level.
type nested struct {
	inner secretHolder
}

// exported holds the same values in exported fields, where fmt can call String.
type exported struct {
	Credential auth.Credential
	Assertion  auth.Assertion
	Key        auth.SigningKey
}

// TestSecretsNeverLeakThroughContainingStructs renders every secret-bearing value
// this package produces, in every containment shape and with every verb, and
// requires that no secret appears in any of them.
func TestSecretsNeverLeakThroughContainingStructs(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "SUPERSECRET-ACCESS-TOKEN",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	assertion := mintAssertion(t, issuer, "https://as.example.com")

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	key, err := auth.GenerateSigningKey("leak-test", jwa.ES256)
	require.NoError(t, err)

	// An exchanger configured with a client secret. This one was missed for as
	// long as this file claimed to cover "every secret-bearing value this
	// package produces": the secret was held in a plain string field, so every
	// verb printed it in full through the pointer NewClientCredentialsExchanger
	// returns. A test whose stated scope is wider than its actual reach is worse
	// than a narrower one, because it is believed.
	secretExchanger, err := auth.NewClientCredentialsExchanger(auth.ClientCredentialsConfig{
		TokenURL:     party.url + "/token",
		ClientID:     "leak-test-client",
		ClientSecret: "SUPERSECRET-CLIENT-SECRET",
		Clock:        clock.Now,
	})
	require.NoError(t, err)

	// An AWS credential carries three named values rather than one, so a leak
	// through any of them is covered too.
	aws, err := auth.NewCredential(auth.CredentialAWSSession, referenceTime.Add(time.Hour), map[string]string{
		auth.CredentialAccessKeyID:     "ASIA-SUPERSECRET-KEY-ID",
		auth.CredentialSecretAccessKey: "SUPERSECRET-SECRET-ACCESS-KEY",
		auth.CredentialSessionToken:    "SUPERSECRET-SESSION-TOKEN",
	})
	require.NoError(t, err)

	// Everything that must never appear in any rendering.
	secrets := []string{
		assertion.Token(),
		"SUPERSECRET-ACCESS-TOKEN",
		"ASIA-SUPERSECRET-KEY-ID",
		"SUPERSECRET-SECRET-ACCESS-KEY",
		"SUPERSECRET-SESSION-TOKEN",
		"SUPERSECRET-CLIENT-SECRET",
	}

	holder := secretHolder{credential: credential, assertion: assertion, key: key}

	renderings := map[string]func() string{
		// Directly, which a String method already covers.
		"credential %v":  func() string { return fmt.Sprintf("%v", credential) },
		"credential %+v": func() string { return fmt.Sprintf("%+v", credential) },
		"credential %#v": func() string { return fmt.Sprintf("%#v", credential) },
		"credential %s":  func() string { return fmt.Sprintf("%s", credential) },
		"assertion %v":   func() string { return fmt.Sprintf("%v", assertion) },
		"assertion %+v":  func() string { return fmt.Sprintf("%+v", assertion) },
		"assertion %#v":  func() string { return fmt.Sprintf("%#v", assertion) },
		"assertion %s":   func() string { return fmt.Sprintf("%s", assertion) },
		"key %v":         func() string { return fmt.Sprintf("%v", key) },
		"key %+v":        func() string { return fmt.Sprintf("%+v", key) },
		"key %#v":        func() string { return fmt.Sprintf("%#v", key) },
		"exchanger %v":   func() string { return fmt.Sprintf("%v", secretExchanger) },
		"exchanger %+v":  func() string { return fmt.Sprintf("%+v", secretExchanger) },
		"exchanger %#v":  func() string { return fmt.Sprintf("%#v", secretExchanger) },
		"exchanger %s":   func() string { return fmt.Sprintf("%s", secretExchanger) },
		"exchanger in a slice %v": func() string {
			return fmt.Sprintf("%v", []auth.Exchanger{secretExchanger})
		},
		"exchanger in a map %v": func() string {
			return fmt.Sprintf("%v", map[string]auth.Exchanger{"a": secretExchanger})
		},

		"aws %v":  func() string { return fmt.Sprintf("%v", aws) },
		"aws %+v": func() string { return fmt.Sprintf("%+v", aws) },
		"aws %#v": func() string { return fmt.Sprintf("%#v", aws) },

		// Through unexported fields, where fmt cannot call String.
		// %s on a struct with no String method is a vet error, so that vector is
		// caught at build time; %v is not, which is what makes it the dangerous one.
		"holder %v":  func() string { return fmt.Sprintf("%v", holder) },
		"holder %+v": func() string { return fmt.Sprintf("%+v", holder) },
		"holder %#v": func() string { return fmt.Sprintf("%#v", holder) },

		// And through more than one level of containment.
		"nested %v":  func() string { return fmt.Sprintf("%v", nested{inner: holder}) },
		"nested %+v": func() string { return fmt.Sprintf("%+v", nested{inner: holder}) },
		"nested %#v": func() string { return fmt.Sprintf("%#v", nested{inner: holder}) },

		"pointer to holder %v":  func() string { return fmt.Sprintf("%v", &holder) },
		"pointer to holder %+v": func() string { return fmt.Sprintf("%+v", &holder) },

		"slice of holders %v": func() string { return fmt.Sprintf("%v", []secretHolder{holder}) },
		"map of holders %v":   func() string { return fmt.Sprintf("%v", map[string]secretHolder{"a": holder}) },
		"array of holders %v": func() string { return fmt.Sprintf("%v", [1]secretHolder{holder}) },

		// Exported fields, where a String method does apply.
		"exported %v":  func() string { return fmt.Sprintf("%v", exported{credential, assertion, key}) },
		"exported %+v": func() string { return fmt.Sprintf("%+v", exported{credential, assertion, key}) },

		// Serialization, which is what a durable execution backend does.
		"json of holder": func() string {
			encoded, err := json.Marshal(struct {
				Credential auth.Credential `json:"credential"`
				Assertion  auth.Assertion  `json:"assertion"`
				Key        auth.SigningKey `json:"key"`
			}{credential, assertion, key})
			require.NoError(t, err)
			return string(encoded)
		},

		// Structured logging, both as attributes and as a contained value.
		"slog attrs": func() string {
			var buffer bytes.Buffer
			slog.New(slog.NewJSONHandler(&buffer, nil)).Info("x",
				"credential", credential, "assertion", assertion, "key", key)
			return buffer.String()
		},
		"slog any of holder": func() string {
			var buffer bytes.Buffer
			slog.New(slog.NewJSONHandler(&buffer, nil)).Info("x", "holder", holder)
			return buffer.String()
		},
		"slog text of holder": func() string {
			var buffer bytes.Buffer
			slog.New(slog.NewTextHandler(&buffer, nil)).Info("x", "holder", holder)
			return buffer.String()
		},
	}

	for name, render := range renderings {
		t.Run(name, func(t *testing.T) {
			rendered := render()

			for _, secret := range secrets {
				require.NotContains(t, rendered, secret,
					"%s leaked secret material:\n%s", name, rendered)
			}
		})
	}
}

// outsideExchanger is an [auth.Exchanger] written the way one outside this package
// has to be: it cannot set the credential's material directly, because there is no
// field for it, so it goes through [auth.NewCredential].
//
// This exists because the interface is documented as the extension point for
// reaching a new system, and an extension point that only works for code inside
// the package is not one.
type outsideExchanger struct {
	expiresAt time.Time
}

func (outsideExchanger) Name() string { return "outside" }

func (outsideExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://vault.example.com"}
}

func (e outsideExchanger) Exchange(ctx context.Context, assertion auth.Assertion) (auth.Credential, error) {
	if assertion.Token() == "" {
		return auth.Credential{}, auth.ErrCredentialUnresolved
	}

	return auth.NewCredential(auth.CredentialBearer, e.expiresAt, map[string]string{
		auth.CredentialAccessToken: "token-for-" + assertion.Subject,
	})
}

// TestThirdPartyExchanger checks that a new relying party can be supported from
// outside this package, which is the whole claim the [auth.Exchanger] interface
// makes.
func TestThirdPartyExchanger(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("vault", outsideExchanger{expiresAt: referenceTime.Add(time.Hour)}),
		auth.WithAssumeAllowRules(`target == "vault" && workload.namespace == "acme"`),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "vault")
	require.NoError(t, err)

	bearer, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, "token-for-flowstate:acme/prod/deploy-service/push-image", bearer)
	require.Equal(t, "vault", credential.Target)
	require.Equal(t, "outside", credential.Provider)

	// And the credential it built redacts itself exactly like the built-in ones,
	// because the redaction is a property of the type rather than of who made it.
	require.NotContains(t, fmt.Sprintf("%+v", struct{ c auth.Credential }{credential}), "token-for-")

	t.Run("a credential with no expiry cannot be built", func(t *testing.T) {
		_, err := auth.NewCredential(auth.CredentialBearer, time.Time{}, map[string]string{
			auth.CredentialAccessToken: "x",
		})
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
	})

	t.Run("a credential with no material cannot be built", func(t *testing.T) {
		_, err := auth.NewCredential(auth.CredentialBearer, referenceTime, map[string]string{
			auth.CredentialAccessToken: "",
		})
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
	})

	t.Run("a credential with no type cannot be built", func(t *testing.T) {
		_, err := auth.NewCredential("", referenceTime, map[string]string{"x": "y"})
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
	})

	t.Run("the values map is copied", func(t *testing.T) {
		values := map[string]string{auth.CredentialAccessToken: "original"}

		credential, err := auth.NewCredential(auth.CredentialBearer, referenceTime.Add(time.Hour), values)
		require.NoError(t, err)

		values[auth.CredentialAccessToken] = "swapped"

		bearer, ok := credential.Bearer()
		require.True(t, ok)
		require.Equal(t, "original", bearer, "a credential must not change under a caller who reuses the map")
	})
}

// TestSecretsStillReadableAfterRedaction checks that hiding the material from fmt
// did not hide it from the code that legitimately needs it. A type that leaks
// nothing because it carries nothing would pass every test above.
func TestSecretsStillReadableAfterRedaction(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	assertion := mintAssertion(t, issuer, "https://as.example.com")
	require.NotEmpty(t, assertion.Token())

	credential, err := auth.NewCredential(auth.CredentialBearer, referenceTime.Add(time.Hour), map[string]string{
		auth.CredentialAccessToken: "usable-token",
	})
	require.NoError(t, err)

	bearer, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, "usable-token", bearer)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.example.com/", nil)
	require.NoError(t, err)
	require.NoError(t, credential.Apply(request))
	require.Equal(t, "Bearer usable-token", request.Header.Get("Authorization"))

	// And the metadata a caller reasonably wants is still visible when printed.
	require.Contains(t, fmt.Sprintf("%v", credential), "bearer")
	require.Contains(t, fmt.Sprintf("%v", assertion), assertion.Subject)
}

// TestMaterialIsTheSharedPrimitive checks the type that holds every secret in this
// package, on its own, so that a type built on it inherits tested protections
// rather than reimplementing them.
func TestMaterialIsTheSharedPrimitive(t *testing.T) {
	material := auth.NewMaterial(map[string]string{
		"access_token": "SUPERSECRET-VALUE",
		"empty":        "",
	})

	value, ok := material.Value("access_token")
	require.True(t, ok)
	require.Equal(t, "SUPERSECRET-VALUE", value)

	_, ok = material.Value("empty")
	require.False(t, ok, "a name with no value is indistinguishable from absence")

	_, ok = material.Value("absent")
	require.False(t, ok)

	// The zero value carries nothing and reports so rather than panicking.
	var zero auth.Material
	require.True(t, zero.IsZero())
	_, ok = zero.Value("access_token")
	require.False(t, ok)

	// A single unnamed value, for a secret that is just a string.
	single := auth.NewSingleMaterial("SUPERSECRET-SINGLE")
	got, ok := single.Single()
	require.True(t, ok)
	require.Equal(t, "SUPERSECRET-SINGLE", got)

	// Nothing renders any part of it, at any depth, with any verb.
	type holder struct{ m auth.Material }

	for _, rendered := range []string{
		fmt.Sprint(material),
		fmt.Sprintf("%v", material),
		fmt.Sprintf("%+v", material),
		fmt.Sprintf("%#v", material),
		fmt.Sprintf("%s", material),
		fmt.Sprintf("%q", material),
		fmt.Sprintf("%v", holder{material}),
		fmt.Sprintf("%+v", holder{material}),
		fmt.Sprintf("%#v", holder{material}),
		fmt.Sprintf("%v", []holder{{material}}),
		fmt.Sprintf("%v", map[string]holder{"a": {material}}),
		fmt.Sprintf("%v", &holder{material}),
		fmt.Sprintf("%v", struct{ inner holder }{holder{material}}),
	} {
		require.NotContains(t, rendered, "SUPERSECRET-VALUE", "rendered as %q", rendered)
	}

	encoded, err := json.Marshal(struct{ M auth.Material }{material})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "SUPERSECRET-VALUE")

	// And a serialized material comes back empty rather than restored, whatever the
	// document claimed.
	var restored struct{ M auth.Material }
	require.NoError(t, json.Unmarshal([]byte(`{"M":{"access_token":"forged"}}`), &restored))
	require.True(t, restored.M.IsZero())

	var buffer bytes.Buffer
	slog.New(slog.NewJSONHandler(&buffer, nil)).Info("x", "material", material)
	require.NotContains(t, buffer.String(), "SUPERSECRET-VALUE")

	// The map is copied, so a caller who reuses it cannot change what was handed out.
	values := map[string]string{"k": "original"}
	held := auth.NewMaterial(values)
	values["k"] = "swapped"
	got, ok = held.Value("k")
	require.True(t, ok)
	require.Equal(t, "original", got)
}
