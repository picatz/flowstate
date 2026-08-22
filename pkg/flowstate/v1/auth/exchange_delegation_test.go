package auth_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// TestDelegatedTokenExchange covers the RFC 8693 delegation case: a workload
// obtaining a credential while acting for somebody else, with both identities
// on the wire so the authorization server can record the pair.
//
// The direction is the whole point and is the opposite of the intuitive
// reading, so it is asserted rather than described: the delegator is the
// *subject*, and the Flowstate assertion — the party actually making the call —
// is the *actor*.
func TestDelegatedTokenExchange(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	// The delegator's token comes from an identity provider that is not us,
	// which is the situation the parameter exists for.
	delegatorIssuer := newTestIssuer(t, authtest.WithClock(clock.Now))
	delegatorToken := delegatorIssuer.MintToken(delegatorIssuer.Claims(
		authtest.WithSubject("alice@example.com"),
		authtest.WithAudience("https://as.example.com"),
	))

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "delegated-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
		Delegator: func(context.Context) (auth.Material, string, error) {
			return auth.NewSingleMaterial(delegatorToken), "", nil
		},
	})
	require.NoError(t, err)

	assertion := mintAssertion(t, issuer, exchanger.Requirement().Audience)

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	sent := party.last(t)
	require.Equal(t, "urn:ietf:params:oauth:grant-type:token-exchange", sent.form.Get("grant_type"))

	// RFC 8693 §2.1: the subject is the party being acted for, and the actor is
	// the party doing the acting.
	require.Equal(t, delegatorToken, sent.form.Get("subject_token"),
		"the delegator is the subject of a delegated exchange")
	require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", sent.form.Get("subject_token_type"))
	require.Equal(t, assertion.Token(), sent.form.Get("actor_token"),
		"the workload's own assertion is the actor")
	require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", sent.form.Get("actor_token_type"))

	bearer, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, "delegated-token", bearer)

	// An undelegated exchanger sends no actor token at all, so the two shapes
	// are distinguishable on the wire rather than only in configuration.
	plain, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	_, err = plain.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	sent = party.last(t)
	require.Equal(t, assertion.Token(), sent.form.Get("subject_token"))
	require.Empty(t, sent.form.Get("actor_token"))
	require.Empty(t, sent.form.Get("actor_token_type"))
}

// TestDelegatedTokenExchangeFailsClosed covers what a delegated exchange must
// never degrade into.
//
// Falling back to the undelegated form would send the delegator's authority
// request without saying who was acting, and the authorization server would
// return a credential it believed the delegator itself had asked for. That is
// impersonation arriving through an error path, so every one of these refuses
// the exchange instead.
func TestDelegatedTokenExchangeFailsClosed(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	var reached bool
	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		reached = true
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "should-never-be-issued",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	tests := []struct {
		name      string
		delegator auth.DelegatorTokenFunc
	}{
		{
			name: "the delegator's token could not be acquired",
			delegator: func(context.Context) (auth.Material, string, error) {
				return auth.Material{}, "", errors.New("the session has expired")
			},
		},
		{
			name: "there is no delegator token to act for",
			delegator: func(context.Context) (auth.Material, string, error) {
				return auth.Material{}, "", nil
			},
		},
		{
			name: "the delegator's token is empty",
			delegator: func(context.Context) (auth.Material, string, error) {
				return auth.NewSingleMaterial(""), "", nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reached = false

			exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
				TokenURL:  party.url + "/token",
				Audience:  "https://as.example.com",
				Clock:     clock.Now,
				Delegator: test.delegator,
			})
			require.NoError(t, err)

			credential, err := exchanger.Exchange(t.Context(),
				mintAssertion(t, issuer, exchanger.Requirement().Audience))
			require.ErrorIs(t, err, auth.ErrExchangeFailed)
			require.True(t, credential.IsZero(), "a refused exchange must produce no credential")
			require.False(t, reached, "the authorization server must not be asked at all")
		})
	}
}
