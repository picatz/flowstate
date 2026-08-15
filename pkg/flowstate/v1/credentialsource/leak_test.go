package credentialsource_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// This file exists for the reason auth/leak_test.go does: fmt cannot call a
// method on a value it reaches through an unexported field, so it prints the
// fields it can see instead — a plain string field is how the client secret
// leak in PR #563 happened. [credentialsource.Token] holds its value in an
// auth.Material, which is a closure and therefore opaque to fmt at any depth,
// through any container, under any verb. This asserts that holds, the same
// way auth's own leak test does for the types in that package.

// holder contains a Token in an unexported field, the shape that defeats a
// String method.
type holder struct {
	token credentialsource.Token
}

// nested holds a holder, so the fallback is reached at more than one level.
type nested struct {
	inner holder
}

func TestTokenNeverLeaksThroughContainingStructs(t *testing.T) {
	server, requests := stubGitHubActionsEndpoint(t, mintedJWT(t, time.Now().Add(time.Hour)))
	defer server.Close()

	source := newTestGitHubActionsSource(t, server.URL, "flowstate", nil)

	token, err := source.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, requests.count())

	raw, ok := token.Bearer()
	require.True(t, ok)
	require.NotEmpty(t, raw)

	h := holder{token: token}

	renderings := map[string]func() string{
		"token %v":  func() string { return fmt.Sprintf("%v", token) },
		"token %+v": func() string { return fmt.Sprintf("%+v", token) },
		"token %#v": func() string { return fmt.Sprintf("%#v", token) },
		"token %s":  func() string { return fmt.Sprintf("%s", token) },

		"holder %v":  func() string { return fmt.Sprintf("%v", h) },
		"holder %+v": func() string { return fmt.Sprintf("%+v", h) },
		"holder %#v": func() string { return fmt.Sprintf("%#v", h) },

		"nested %v":  func() string { return fmt.Sprintf("%v", nested{inner: h}) },
		"nested %+v": func() string { return fmt.Sprintf("%+v", nested{inner: h}) },
		"nested %#v": func() string { return fmt.Sprintf("%#v", nested{inner: h}) },

		"pointer to holder %v": func() string { return fmt.Sprintf("%v", &h) },

		"slice of holders %v": func() string { return fmt.Sprintf("%v", []holder{h}) },
		"map of holders %v":   func() string { return fmt.Sprintf("%v", map[string]holder{"a": h}) },
		"array of holders %v": func() string { return fmt.Sprintf("%v", [1]holder{h}) },
	}

	for name, render := range renderings {
		t.Run(name, func(t *testing.T) {
			rendered := render()
			require.NotContains(t, rendered, raw, "rendering %q leaked the bearer token", name)
		})
	}
}
