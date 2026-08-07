package flowtest

import (
	"context"
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// secretTestSubject and secretTestIssuer are the identity a case's secret
// resolution runs as. `flow test` has no concept of "who ran this test" any
// more for a secret than it does for a scripted signal ([runCase]'s own note
// on hasStarter) — these exist only so [auth.WorkloadIdentity] has something
// to log, never to be checked against a rule, since [secretPolicy] always
// allows.
const (
	secretTestSubject = "flow-test"
	secretTestIssuer  = "flow-test"
)

// secretRuntime builds the [v1.TaskRuntime] one case resolves `${secret(...)}`
// references through: a store holding exactly the references test.Secrets
// binds, and a policy that allows every one of them.
//
// Authorization is deliberately not the thing under test here.
// [auth.SecretPolicy] already has its own tests
// (TestSecretPolicyAuthorize and friends); a case that wants to exercise a
// *denial* writes one directly against that package, the same way a case
// wanting to exercise signal-policy denial writes `sender:` to not match
// rather than asking `flow test` to reimplement policy compilation. What
// `flow test` owns is the one thing nothing else can stand in for: whether a
// reference a case never named is refused rather than answered.
func secretRuntime(bindings map[string]string) (v1.TaskRuntime, error) {
	store, err := newTestSecretStore(bindings)
	if err != nil {
		return v1.TaskRuntime{}, err
	}

	policy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	if err != nil {
		// Unreachable with a fixed, always-true rule; guarded anyway because
		// this function is not the place to assume a CEL compile can never
		// fail.
		return v1.TaskRuntime{}, fmt.Errorf("flow test: compiling the secret access policy: %w", err)
	}

	return v1.TaskRuntime{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: secretTestSubject,
			Issuer:  secretTestIssuer,
		},
	}, nil
}

// newTestSecretStore builds a [secrets.Store] whose only providers are
// [testSecretProvider]s constructed from bindings, one per scheme referenced
// — never the schemes this build happens to have real providers for. That is
// what makes a scheme need not exist to be tested: `vault` here is nothing
// but a map key, satisfied entirely by this file, with no dependency on
// pkg/flowstate/v1/secrets/vault at all.
func newTestSecretStore(bindings map[string]string) (*secrets.Store, error) {
	byScheme := make(map[string]map[string]string)

	for refText, value := range bindings {
		// Already validated once, at load time ([parseSource]); reparsed
		// here rather than threading the parsed form through the YAML
		// struct, which is cheap at [MaxSecretsPerTest]'s bound and keeps
		// [Test.Secrets] itself a plain, YAML-native map.
		ref, err := secrets.ParseRef(refText)
		if err != nil {
			return nil, fmt.Errorf("flow test: secrets: %w", err)
		}

		names, ok := byScheme[ref.GetScheme()]
		if !ok {
			names = make(map[string]string)
			byScheme[ref.GetScheme()] = names
		}
		names[ref.GetName()] = value
	}

	providers := make([]secrets.Provider, 0, len(byScheme))
	for scheme, names := range byScheme {
		providers = append(providers, &testSecretProvider{scheme: scheme, values: names})
	}

	return secrets.NewStore(providers...)
}

// testSecretProvider answers every reference of one scheme from the fixed
// map a case's `secrets:` block gave it — the secret sibling of stub.go's
// stubbedTask. No real backend is ever consulted: a name absent from values
// is refused with [secrets.ErrNotFound], never resolved to empty and never
// falling through to whatever this process happens to have configured for
// the scheme.
type testSecretProvider struct {
	scheme string
	values map[string]string
}

// Scheme implements [secrets.Provider].
func (p *testSecretProvider) Scheme() string { return p.scheme }

// Resolve implements [secrets.Provider].
func (p *testSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	value, ok := p.values[req.Ref.GetName()]
	if !ok {
		return secrets.Secret{}, fmt.Errorf("%w: no `secrets:` entry for %q in this test case",
			secrets.ErrNotFound, secrets.RefString(req.Ref))
	}
	return secrets.NewSecret(req.Ref, value), nil
}

// resolveSecretInputs resolves every `${secret(...)}` reference among a
// stubbed task's inputs, the way the real task it replaces would — taskFuncHTTP
// resolves the "bearer" input it was given, for instance — so that a stub's
// `where:` can assert on the plaintext value a case declared, and so that a
// reference with no matching `secrets:` entry is refused before any matcher
// runs, whether or not `where:` ever mentions the input carrying it.
//
// It fails closed rather than silently, on the same reasoning
// [unstubbedTaskFn] applies to a task with no stub at all: an unresolved
// reference must never look like an empty value, and must never reach a real
// secret backend this process happens to have configured for the scheme —
// see [secretRuntime]'s doc for why that backend is never the one consulted
// here.
//
// The returned map holds the resolved plaintext, keyed by input name, for
// [compiledStub.matches] to fold into the `where:` activation. It is not the
// reference's job to stay redacted here: the value a case supplies in its own
// `secrets:` block is a value the test's author already wrote down in the
// clear, in the same file.
func resolveSecretInputs(ctx context.Context, inputs map[string]*v1.Value) (map[string]string, error) {
	var resolved map[string]string

	for name, value := range inputs {
		ref := value.GetSecretRef()
		if ref == nil {
			continue
		}

		secret, err := v1.ResolveSecret(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf(
				"flow test: input %q names secret %q, but this case does not resolve it (%v); "+
					"add a `secrets:` entry binding %q to a value — flow test never resolves a real secret",
				name, secrets.RefString(ref), err, secrets.RefString(ref))
		}

		if resolved == nil {
			resolved = make(map[string]string, len(inputs))
		}
		resolved[name] = secret.Reveal()
	}

	return resolved, nil
}
