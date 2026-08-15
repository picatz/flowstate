package credentialsource

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// envSource reads a bearer token from an environment variable, fresh on every
// call.
type envSource struct {
	variable string
}

// NewEnvSource returns a [Source] that reads the named environment variable
// on every [Source.Token] call.
//
// Unlike the default, implicit "check FLOWSTATE_TOKEN and otherwise go
// anonymous" a caller with no configured source falls back to, a Source built
// through this function — meaning one a caller asked for by name — refuses
// rather than going anonymous when the variable is unset or empty. Naming a
// source is asking for a credential; an empty one is a misconfiguration, not
// a preference for anonymity.
func NewEnvSource(variable string) Source {
	return envSource{variable: variable}
}

func (e envSource) Name() string { return SourceEnv }

func (e envSource) Token(ctx context.Context) (Token, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}

	raw := strings.TrimSpace(os.Getenv(e.variable))
	if raw == "" {
		return Token{}, fmt.Errorf("%w: %s is unset or empty", ErrSourceUnusable, e.variable)
	}

	return newToken(SourceEnv, raw, time.Time{}), nil
}
