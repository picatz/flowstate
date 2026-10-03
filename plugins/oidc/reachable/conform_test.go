package reachable

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/plugintest"
)

// TestConform holds this plugin to what every plugin owes the engine and its
// authors beyond running: a version and description, a summary and declared
// outputs for each task, a comment on every schema field (the hover text and
// the reference page), and descriptors that come out the same on every launch
// so a pin can hold. See [plugintest.Session.Conform] for each check and why.
//
// It does not register into the default registry, so it may run beside the
// reachability test that does.
func TestConform(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}

	plugintest.Launch(t, plugintest.Build(t, "github.com/picatz/flowstate/plugins/oidc", "oidc")).Conform(t)
}
