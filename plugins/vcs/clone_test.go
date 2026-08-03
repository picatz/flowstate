package main

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// containmentSecret is a value that would be obviously wrong to find in any
// of the outputs below.
const containmentSecret = "ghp_containment_canary_do_not_print_me"

// TestCloneOptionsNeverPrintsItsToken is the containment-shape test
// CLAUDE.md requires for anything holding a credential: %v, %+v, %#v, and
// %s, on the value itself, on a struct holding it, and on a slice of those -
// because a redacting String method protects a value printed directly and
// does nothing when it sits inside another struct, and the only pattern that
// survives both is holding the material in a closure fmt's reflection cannot
// reach.
func TestCloneOptionsNeverPrintsItsToken(t *testing.T) {
	u, err := url.Parse("https://example.com/owner/repo.git")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	opts := cloneOptions{
		url:   u,
		depth: 1,
		token: func() string { return containmentSecret },
	}

	type holder struct {
		Options cloneOptions
		Label   string
	}
	wrapped := holder{Options: opts, Label: "clone request"}

	rendered := []string{
		fmt.Sprintf("%v", opts),
		fmt.Sprintf("%+v", opts),
		fmt.Sprintf("%#v", opts),
		fmt.Sprintf("%s", containedStringer{opts}),
		fmt.Sprintf("%v", wrapped),
		fmt.Sprintf("%+v", wrapped),
		fmt.Sprintf("%#v", wrapped),
		fmt.Sprintf("%v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%+v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%#v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%v", []holder{wrapped}),
	}

	for _, r := range rendered {
		if strings.Contains(r, containmentSecret) {
			t.Fatalf("token leaked through fmt reflection: %q", r)
		}
	}
}

// containedStringer gives %s something to format that is not already a
// string, exercising the same %s path a struct holding a cloneOptions would
// go through if some caller's error message or log line interpolated it.
type containedStringer struct{ opts cloneOptions }

func (c containedStringer) String() string { return fmt.Sprintf("%v", c.opts) }
