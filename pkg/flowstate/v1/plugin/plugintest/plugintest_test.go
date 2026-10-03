package plugintest_test

import (
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/plugintest"
)

// examplePkg is the repository's worked example plugin, the subject of the
// positive tests: a plugin written the way an author would write one.
const examplePkg = "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/examples/flowstate-plugin-example"

// sloppyPkg declares as little as the SDK permits.
const sloppyPkg = "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/plugintest/testdata/sloppy"

func skipShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
}

func TestCallRunsATaskThroughTheHost(t *testing.T) {
	skipShort(t)
	s := plugintest.Launch(t, plugintest.Build(t, examplePkg, "example"))

	out := s.Run(t, "example.greet", map[string]any{"name": "Ada"})
	if got := out.String(t, "message"); got != "Hello, Ada!" {
		t.Errorf("message = %q", got)
	}
	if got := out.Get(t, "length"); got != float64(len("Hello, Ada!")) {
		t.Errorf("length = %v (%T)", got, got)
	}
	if _, ok := out.Lookup("nonesuch"); ok {
		t.Error("Lookup found an output the task did not return")
	}
}

func TestCallClassifiesAFailure(t *testing.T) {
	skipShort(t)
	s := plugintest.Launch(t, plugintest.Build(t, examplePkg, "example"))

	_, err := s.Call(t.Context(), "example.greet", nil)
	if err == nil {
		t.Fatal("an empty name was accepted")
	}
	if kind := plugintest.ErrorKind(err); kind != flowstatev1.ErrorKindInvalidInput {
		t.Errorf("kind = %q, want %q (%v)", kind, flowstatev1.ErrorKindInvalidInput, err)
	}

	if _, err := s.Call(t.Context(), "example.nonesuch", nil); err == nil || !strings.Contains(err.Error(), "example.greet") {
		t.Errorf("an unknown task should list what is served, got %v", err)
	}
}

func TestSecretInputsAreResolvedByTheHostAndNeverReturned(t *testing.T) {
	skipShort(t)
	const secret = "s3cr3t-token-value"
	dir := plugintest.Build(t, examplePkg, "example")

	t.Run("listed reference resolves", func(t *testing.T) {
		s := plugintest.Launch(t, dir, plugintest.WithSecrets(map[string]string{"env:TOKEN": secret}))
		out := s.Run(t, "example.greet", map[string]any{"name": "Ada", "token": plugintest.SecretRef("env", "TOKEN")})
		if out.Get(t, "authenticated") != true {
			t.Error("the plugin did not receive the resolved token")
		}
		if strings.Contains(out.String(t, "message"), secret) {
			t.Error("the secret value leaked into an output")
		}
	})

	t.Run("unlisted reference is refused", func(t *testing.T) {
		s := plugintest.Launch(t, dir, plugintest.WithSecrets(map[string]string{"env:OTHER": secret}))
		_, err := s.Call(t.Context(), "example.greet", map[string]any{"name": "Ada", "token": plugintest.SecretRef("env", "TOKEN")})
		if err == nil {
			t.Fatal("a reference the store does not hold resolved")
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("the error carries a secret: %v", err)
		}
	})
}

func TestResolveReachesAPluginsSecretScheme(t *testing.T) {
	skipShort(t)
	s := plugintest.Launch(t, plugintest.Build(t, examplePkg, "example"),
		plugintest.WithEnv("EXAMPLE_SECRET_TEAMA_KEY=a-value"))

	got, err := s.Resolve(t.Context(), "example:key", "teama")
	if err != nil || got != "a-value" {
		t.Fatalf("Resolve = %q, %v", got, err)
	}
	if _, err := s.Resolve(t.Context(), "example:key", "teamb"); err == nil {
		t.Error("another tenant resolved a secret scoped to teama")
	}
	if _, err := s.Resolve(t.Context(), "nonesuch:key", ""); err == nil {
		t.Error("a scheme no plugin serves resolved")
	}
}

func TestConformPassesTheWorkedExample(t *testing.T) {
	skipShort(t)
	plugintest.Launch(t, plugintest.Build(t, examplePkg, "example")).Conform(t)
}

// TestAuditFindsWhatASloppyPluginLeavesOut is the negative direction: a check
// that cannot fail proves nothing, so each check that a plugin can violate is
// shown to report the violation, and only that one.
func TestAuditFindsWhatASloppyPluginLeavesOut(t *testing.T) {
	skipShort(t)
	s := plugintest.Launch(t, plugintest.Build(t, sloppyPkg, "sloppy"))

	got := map[plugintest.Check][]string{}
	for _, f := range s.Audit(t.Context(), t) {
		got[f.Check] = append(got[f.Check], f.Message)
	}

	for check, want := range map[plugintest.Check]string{
		plugintest.CheckIdentity:         "no version",
		plugintest.CheckSummaries:        "sloppy.bare has no summary",
		plugintest.CheckDeclaredOutputs:  "sloppy.bare declares no outputs",
		plugintest.CheckDocumentedFields: "has no comment",
	} {
		if !anyContains(got[check], want) {
			t.Errorf("check %q did not report %q; it reported %q", check, want, got[check])
		}
	}

	// The checks a sloppy plugin does not violate stay quiet: health and
	// digests are properties of the process, not of how much it declared.
	for _, check := range []plugintest.Check{plugintest.CheckHealth, plugintest.CheckStableDigests} {
		if len(got[check]) != 0 {
			t.Errorf("check %q reported %q on a plugin that does not violate it", check, got[check])
		}
	}

	// A field inside a nested message is held to the same standard as one at the
	// top, and is named by its path.
	if !anyContains(got[plugintest.CheckDocumentedFields], `output field "detail.inner"`) {
		t.Errorf("a nested undocumented field was not reported: %q", got[plugintest.CheckDocumentedFields])
	}

	// A task that does declare outputs is not accused of declaring none.
	if anyContains(got[plugintest.CheckDeclaredOutputs], "sloppy.claims") {
		t.Errorf("sloppy.claims declares an output but was reported: %q", got[plugintest.CheckDeclaredOutputs])
	}
}

// TestAuditRunsOnlyTheChecksNotIgnored shows that an ignored check does not run
// rather than running and being hidden: the flaky plugin violates both of the
// process checks, and ignoring them yields no finding from either.
func TestAuditRunsOnlyTheChecksNotIgnored(t *testing.T) {
	skipShort(t)
	s := plugintest.Launch(t, plugintest.Build(t, flakyPkg, "flaky"))

	if len(s.Audit(t.Context(), t)) == 0 {
		t.Fatal("the flaky plugin was not reported at all, so ignoring checks proves nothing")
	}
	for _, f := range s.Audit(t.Context(), t, plugintest.CheckHealth, plugintest.CheckStableDigests) {
		if f.Check == plugintest.CheckHealth || f.Check == plugintest.CheckStableDigests {
			t.Errorf("ignored check reported: %v", f)
		}
	}
}

func anyContains(messages []string, substr string) bool {
	for _, m := range messages {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

// flakyPkg declares a schema that differs on every launch and answers its health
// poll with a refusal and no reason.
const flakyPkg = "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/plugintest/testdata/flaky"

// TestAuditFindsAPluginThatCannotBePinnedOrDiagnosed is the negative direction for
// the two checks about the process rather than the declarations: digests that move
// between launches, and a health refusal an operator cannot read.
func TestAuditFindsAPluginThatCannotBePinnedOrDiagnosed(t *testing.T) {
	skipShort(t)
	s := plugintest.Launch(t, plugintest.Build(t, flakyPkg, "flaky"))

	got := map[plugintest.Check][]string{}
	for _, f := range s.Audit(t.Context(), t) {
		got[f.Check] = append(got[f.Check], f.Message)
	}

	if !anyContains(got[plugintest.CheckStableDigests], "not deterministic") {
		t.Errorf("a schema named for the launch time was not reported; stable digests said %q", got[plugintest.CheckStableDigests])
	}
	if !anyContains(got[plugintest.CheckHealth], "or not serving with a reason") {
		t.Errorf("a health refusal with no reason was not reported; health said %q", got[plugintest.CheckHealth])
	}
}
