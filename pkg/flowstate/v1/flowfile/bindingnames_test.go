package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEveryDeclarationSiteRefusesAnUnreadableName holds the rule of #1427: a name
// an author chooses and an expression then reads has to be a CEL identifier, and
// is refused where it is declared. Each site is one line of the table, so a new
// declaration position cannot be added without a decision about joining it.
func TestEveryDeclarationSiteRefusesAnUnreadableName(t *testing.T) {
	t.Parallel()

	sites := []struct {
		name string
		// source renders a Flowfile declaring the name at the site.
		source func(name string) string
	}{
		{"workflow vars key", func(n string) string {
			return fmt.Sprintf("edition: v2026.4\nname: s\nvars:\n  %q: hi\nsteps:\n  - id: s\n    log: {message: x}\n", n)
		}},
		{"step vars key", func(n string) string {
			return fmt.Sprintf("edition: v2026.4\nname: s\nsteps:\n  - id: s\n    vars:\n      %q: hi\n    log: {message: x}\n", n)
		}},
		{"signal wait outputs key", func(n string) string {
			return fmt.Sprintf("edition: v2026.4\nname: s\nsteps:\n  - id: gate\n    wait_for_signal:\n      name: go\n      timeout: 1m\n      outputs:\n        %q: ${timed_out}\n", n)
		}},
		{"nested step id", func(n string) string {
			return fmt.Sprintf("edition: v2026.4\nname: s\nsteps:\n  - id: p\n    parallel:\n      - steps:\n          - id: %q\n            log: {message: x}\n", n)
		}},
	}

	for _, site := range sites {
		for _, name := range []string{"a-b", "a.b", "1a", "in"} {
			t.Run(site.name+"/"+name, func(t *testing.T) {
				t.Parallel()

				got := diagnose(t, site.source(name))
				assert.Contains(t, got, fmt.Sprintf("%q", name), "the refusal names the spelling")
				assert.Contains(t, got, "cannot be parsed", "and says why it cannot be written")
			})
		}
	}

	t.Run("a legal name is left alone", func(t *testing.T) {
		t.Parallel()

		for _, site := range sites {
			got := diagnose(t, site.source("fine_name"))
			assert.False(t, strings.Contains(got, "cannot be parsed"), "%s: %s", site.name, got)
		}
	})
}
