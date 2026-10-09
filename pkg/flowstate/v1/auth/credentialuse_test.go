package auth

import (
	"errors"
	"strings"
	"testing"

	"github.com/picatz/jose/pkg/jwa"
)

// What a rule can say about the use of a credential: the task whose step reads
// it and the plugin credential its input receives. Read from the context at the
// seams that decide, so the policy and the audit record see one thing.

func useBroker(t *testing.T, rules ...string) (*Broker, *vocabularyExchanger) {
	t.Helper()

	key, err := GenerateSigningKey("test-key", jwa.ES256)
	if err != nil {
		t.Fatalf("GenerateSigningKey: %v", err)
	}
	issuer, err := NewIssuer("https://issuer.example.com", key)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	exchanger := &vocabularyExchanger{audience: "https://as.example.com"}
	broker, err := NewBroker(issuer, WithTarget("partner", exchanger), WithAssumeAllowRules(rules...))
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	return broker, exchanger
}

func TestSecretRulesReadTheTaskAndCredentialOfTheUse(t *testing.T) {
	t.Parallel()

	identity, ref := callerFixture()
	slack := CredentialUse{Task: "slack.post", Plugin: "slack", Credential: "bot_token"}
	pinned := `secret.name == "DEPLOY_TOKEN" && credential.plugin == "slack" && credential.name == "bot_token" && task == "slack.post"`

	tests := []struct {
		name  string
		allow []string
		deny  []string
		use   CredentialUse
		want  bool
	}{
		{name: "the pinned rule permits the use it names", allow: []string{pinned}, use: slack, want: true},
		{name: "another task is refused", allow: []string{pinned}, use: CredentialUse{Task: "slack.delete", Plugin: "slack", Credential: "bot_token"}},
		{name: "another plugin is refused", allow: []string{pinned}, use: CredentialUse{Task: "slack.post", Plugin: "git", Credential: "bot_token"}},
		{name: "another credential is refused", allow: []string{pinned}, use: CredentialUse{Task: "slack.post", Plugin: "slack", Credential: "app_token"}},
		{name: "a use that names nothing matches no rule that pins one", allow: []string{pinned}},
		{name: "a use naming a task and no credential is refused by a credential rule", allow: []string{`credential.plugin == "slack"`}, use: CredentialUse{Task: "slack.post"}},
		{name: "a deny rule on the task wins over an allow", allow: []string{"true"}, deny: []string{`task == "slack.delete"`}, use: CredentialUse{Task: "slack.delete"}},
		{name: "a deny rule on the task leaves another task alone", allow: []string{"true"}, deny: []string{`task == "slack.delete"`}, use: slack, want: true},
		{name: "a deny rule on the credential refuses every task that receives it", allow: []string{"true"}, deny: []string{`credential.name == "bot_token"`}, use: slack},
		{name: "a rule that does not pin a use is unaffected by it", allow: []string{`secret.scheme == "env"`}, use: slack, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			policy, err := (SecretAccessPolicy{Allow: test.allow, Deny: test.deny}).Compile()
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}

			err = policy.Authorize(WithCredentialUse(t.Context(), test.use), identity, ref, vocabularySecretRef{})
			if test.want && err != nil {
				t.Fatalf("Authorize = %v, want permitted", err)
			}
			if !test.want && !errors.Is(err, ErrSecretDenied) {
				t.Fatalf("Authorize = %v, want ErrSecretDenied", err)
			}
		})
	}
}

func TestAssumptionRulesReadTheTaskAndCredentialOfTheUse(t *testing.T) {
	t.Parallel()

	identity, ref := callerFixture()
	use := CredentialUse{Task: "anthropic.complete", Plugin: "anthropic", Credential: "api_key"}

	for _, test := range []struct {
		name string
		rule string
		use  CredentialUse
		want bool
	}{
		{name: "a rule pinning the task permits it", rule: `task == "anthropic.complete" && target == "partner"`, use: use, want: true},
		{name: "a rule pinning the credential permits it", rule: `credential.plugin == "anthropic" && credential.name == "api_key"`, use: use, want: true},
		{name: "another task is refused", rule: `task == "anthropic.complete"`, use: CredentialUse{Task: "anthropic.other"}},
		{name: "an unnamed use is refused by a rule that pins one", rule: `task == "anthropic.complete"`},
		{name: "an unnamed use is refused by a rule on the credential", rule: `credential.plugin == "anthropic"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			broker, exchanger := useBroker(t, test.rule)
			_, err := broker.Credential(WithCredentialUse(t.Context(), test.use), identity, ref, "partner")
			if test.want && err != nil {
				t.Fatalf("Credential = %v, want permitted", err)
			}
			if !test.want {
				if !errors.Is(err, ErrAssumeDenied) {
					t.Fatalf("Credential = %v, want ErrAssumeDenied", err)
				}
				if len(exchanger.seen) != 0 {
					t.Fatalf("a refused use reached the exchanger %d times", len(exchanger.seen))
				}
			}
		})
	}
}

func TestAnUnknownAttributeIsStillAStartupError(t *testing.T) {
	t.Parallel()

	for _, rule := range []string{
		`credential.plugn == "slack"`,
		`credential.target == "x"`,
		`credential == "slack"`,
		`task.plugin == "slack"`,
		`tasks == "slack.post"`,
	} {
		if _, err := compileSecretRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err == nil {
			t.Errorf("compiling %q against the secret surface succeeded", rule)
		}
		if _, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit); err == nil {
			t.Errorf("compiling %q against the assumption surface succeeded", rule)
		}
	}
}

func TestWithCredentialUseKeepsTheTaskAnOuterLayerNamed(t *testing.T) {
	t.Parallel()

	ctx := WithCredentialUse(t.Context(), CredentialUse{Task: "slack.post"})
	ctx = WithCredentialUse(ctx, CredentialUse{Plugin: "slack", Credential: "bot_token"})

	got := CredentialUseFrom(ctx)
	if got.Task != "slack.post" || got.Plugin != "slack" || got.Credential != "bot_token" {
		t.Fatalf("use = %+v", got)
	}

	// A later task replaces the earlier use whole, credential included.
	got = CredentialUseFrom(WithCredentialUse(ctx, CredentialUse{Task: "git.push"}))
	if got != (CredentialUse{Task: "git.push"}) {
		t.Fatalf("a new task kept the earlier credential: %+v", got)
	}
	if CredentialUseFrom(t.Context()) != (CredentialUse{}) {
		t.Fatal("an empty context carries a use")
	}
}

// FuzzCredentialUseAttributes holds the attribute surface to its one promise:
// whatever a context carries, a rule that pins an exact task and credential
// permits only that exact use, never panics, and never reads an attribute as
// anything but text.
func FuzzCredentialUseAttributes(f *testing.F) {
	f.Add("slack.post", "slack", "bot_token")
	f.Add("slack.post", "slack", "")
	f.Add("", "", "")
	f.Add("slack.post\x00", "sla\"ck", "bot_token'")
	f.Add(strings.Repeat("a", 1<<12), "slack", "bot_token")

	identity, ref := callerFixture()
	policy, err := (SecretAccessPolicy{
		Allow: []string{`task == "slack.post" && credential.plugin == "slack" && credential.name == "bot_token"`},
		Deny:  []string{`task.startsWith("slack.delete")`},
	}).Compile()
	if err != nil {
		f.Fatalf("Compile: %v", err)
	}

	f.Fuzz(func(t *testing.T, task, plugin, credential string) {
		err := policy.Authorize(WithCredentialUse(t.Context(), CredentialUse{Task: task, Plugin: plugin, Credential: credential}),
			identity, ref, vocabularySecretRef{})

		exact := task == "slack.post" && plugin == "slack" && credential == "bot_token"
		if exact && err != nil {
			t.Fatalf("the exact use was refused: %v", err)
		}
		if !exact && err == nil {
			t.Fatalf("a use that is not the pinned one was permitted: task=%q plugin=%q credential=%q", task, plugin, credential)
		}
	})
}
