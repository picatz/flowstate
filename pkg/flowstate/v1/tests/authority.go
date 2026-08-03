package tests

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Authority is the worker-side capability a case needs installed before it
// runs, when it exercises secret resolution or JIT credential federation.
//
// It exists because #91 and #94 landed with the plumbing that installs a
// [v1.TaskRuntime] written twice — once as a context value for the local
// driver, once as worker registration for the durable one — and only the
// plumbing was under test on each side. A [Case] naming a scheme, a policy and
// an identity gets the *same* fixture built on both sides: the same fixture
// provider, the same compiled policy, the same broker when one is asked for.
// What differs between drivers is only how the result is installed, which
// stays in each driver's own test file — see eval_test.go's and
// engine/authority_test.go's runAuthorityCase.
type Authority struct {
	// Scheme and FixtureValue describe the fixture secret provider a case
	// resolves through: Resolve always succeeds and returns FixtureValue,
	// whatever name was asked for. One fixed value is enough because every
	// case here resolves exactly one reference, and the containment matrix
	// does not change shape between values.
	Scheme       string
	FixtureValue string

	// Allow and Deny compile into a [auth.SecretAccessPolicy] exactly the way a
	// deployment's policy file would, deny evaluated first.
	Allow []string
	Deny  []string

	// Identity is the authenticated workload the policy evaluates against.
	Identity auth.WorkloadIdentity

	// Federation, when set, configures a [auth.Broker] that answers Target
	// with a credential carrying Token — used by the JIT credential cases.
	// Left nil, a case with Store and Policy configured but no Federation
	// exercises the "secrets are configured, federation is not" fail-closed
	// path, distinct from NoRuntime's "nothing is configured at all".
	Federation *Federation

	// NoRuntime means no [v1.TaskRuntime] is installed at all — the genuinely
	// unconfigured worker, rather than one configured with a policy that then
	// refuses.
	NoRuntime bool

	// ProviderCalls, when non-nil, counts how many times the fixture provider
	// actually resolved a reference. A denial case that names the ordering
	// guarantee points this at its own counter and the caller asserts it stayed
	// at zero after the run — proof that policy really ran before the provider
	// was ever consulted, not merely that the final error text looks right.
	ProviderCalls *atomic.Int32
}

// Federation configures the fixture [auth.Broker] an [Authority] builds.
type Federation struct {
	// Target is the credential target name a case's `credential:` input
	// names.
	Target string
	// Token is the bearer material the fixture exchanger hands back.
	Token string
}

// HasSecrets reports whether this Authority configures a fixture secret store
// at all, as opposed to only a broker. A credential-only case — one testing
// JIT federation and nothing else — has no scheme to resolve, and building a
// store for it would fail on the empty scheme rather than describe anything
// about the case.
func (a Authority) HasSecrets() bool { return a.Scheme != "" }

// Store builds the fixture secret store this Authority resolves through.
func (a Authority) Store(tb testing.TB) *secrets.Store {
	tb.Helper()

	store, err := secrets.NewStore(fixtureSecretProvider{
		scheme: a.Scheme,
		value:  a.FixtureValue,
		calls:  a.ProviderCalls,
	})
	if err != nil {
		tb.Fatalf("building fixture secret store: %v", err)
	}
	return store
}

// Policy compiles this Authority's allow and deny rules.
func (a Authority) Policy(tb testing.TB) *auth.SecretPolicy {
	tb.Helper()

	policy, err := (auth.SecretAccessPolicy{Allow: a.Allow, Deny: a.Deny}).Compile()
	if err != nil {
		tb.Fatalf("compiling fixture secret policy: %v", err)
	}
	return policy
}

// Broker builds this Authority's federation broker, or returns nil when it
// declares none — which is itself the fixture for "secrets are configured,
// federation is not".
func (a Authority) Broker(tb testing.TB) *auth.Broker {
	tb.Helper()

	if a.Federation == nil {
		return nil
	}

	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		tb.Fatalf("generating fixture signing key: %v", err)
	}
	key, err := auth.NewSigningKey("fixture", private)
	if err != nil {
		tb.Fatalf("building fixture signing key: %v", err)
	}
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	if err != nil {
		tb.Fatalf("building fixture issuer: %v", err)
	}
	broker, err := auth.NewBroker(issuer,
		auth.WithTarget(a.Federation.Target, fixtureExchanger{token: a.Federation.Token}),
		auth.WithAssumeAllowRules("true"))
	if err != nil {
		tb.Fatalf("building fixture broker: %v", err)
	}
	return broker
}

// ProtoIdentity renders this Authority's identity as the wire type
// [v1.RunState.Identity] carries, for the durable driver to install at worker
// registration.
//
// Claims is copied too, not just the four scalar fields: a case whose policy
// keys on workload.claims["repository"] would otherwise see them on the local
// driver, which installs auth.WorkloadIdentity directly, and lose them on the
// durable driver, which only ever sees what crossed this conversion — a
// driver disagreement the harness itself would have caused rather than caught.
func (a Authority) ProtoIdentity() *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{
		Subject:    a.Identity.Subject,
		Issuer:     a.Identity.Issuer,
		Claims:     a.Identity.Claims,
		Namespace:  a.Identity.Namespace,
		Deployment: a.Identity.Deployment,
	}
}

// fixtureSecretProvider always resolves to value, whatever name was asked
// for, and counts how many times it was asked — the ordering cases' proof
// that a denial never reached the provider.
type fixtureSecretProvider struct {
	scheme string
	value  string
	calls  *atomic.Int32
}

func (p fixtureSecretProvider) Scheme() string { return p.scheme }

func (p fixtureSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	if p.calls != nil {
		p.calls.Add(1)
	}
	return secrets.NewSecret(req.Ref, p.value), nil
}

// fixtureExchanger hands back a fixed bearer credential for whatever target
// it is registered under, mirroring the shape a real STS exchange takes
// without making one.
type fixtureExchanger struct{ token string }

func (e fixtureExchanger) Name() string { return "fixture-sts" }

func (e fixtureExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://resource.example"}
}

func (e fixtureExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.NewCredential(auth.CredentialBearer, time.Now().Add(time.Hour),
		map[string]string{"access_token": e.token})
}

// AuthorityCase is a [Case] that exercises secret resolution or JIT credential
// federation, alongside the [Authority] fixture that must be installed before
// it runs.
type AuthorityCase struct {
	Case
	Authority Authority

	// ContainmentValue, when non-empty, is the fixture material that must
	// never appear in any observable rendering of the run's output — see
	// [AssertNoLeak]. Denial cases leave this empty: nothing is ever
	// resolved, so there is nothing to check for.
	ContainmentValue string
}

// bearerSecretStep builds an http step that reads a static secret reference
// as its bearer token, tolerated so a denial is recorded rather than failing
// the run — matching the shape [ErrorTextCases] uses for the same reason.
func bearerSecretStep(stepID, url, scheme, name string) *v1.Node {
	return &v1.Node{
		Id:     stepID,
		Policy: &v1.StepPolicy{ContinueOnError: true},
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":    v1.NewLiteral(url),
				"bearer": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: scheme, Name: name}}},
			},
		}},
	}
}

// headerSecretStep builds an http step that reads a static secret reference from
// inside its `headers:` mapping, tolerated for the same reason [bearerSecretStep]
// is.
//
// The reference sits one level down, which is the whole difference: `bearer:` is a
// whole input declared to take one, and this is an entry of a structure. Both must
// reach the worker as references and fail the same way when they may not be read,
// or the two spellings of one intention have two behaviours.
func headerSecretStep(stepID, url, scheme, name string) *v1.Node {
	return &v1.Node{
		Id:     stepID,
		Policy: &v1.StepPolicy{ContinueOnError: true},
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral(url),
				"headers": v1.NewStructureMap(map[string]*v1.Value{
					"Accept":        v1.NewLiteral("application/json"),
					"Authorization": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: scheme, Name: name}}},
				}),
			},
		}},
	}
}

// credentialStep builds an http step that authorizes a JIT federation target,
// tolerated for the same reason [bearerSecretStep] is.
func credentialStep(stepID, url, target string) *v1.Node {
	return &v1.Node{
		Id:     stepID,
		Policy: &v1.StepPolicy{ContinueOnError: true},
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":        v1.NewLiteral(url),
				"credential": v1.NewLiteral(target),
			},
		}},
	}
}

// AuthorityDenialCases exercise fail-closed and policy-denied secret and
// credential access, in both drivers.
//
// The workflow never reaches the network — resolution or authorization fails
// before the request is dialed — so the URL each step names is deliberately
// unreachable. What is asserted is the recorded failure text, taken from
// [v1.StepErrorText] the same way [ErrorTextCases] does: written out as a
// literal so that changing it is a change somebody reads, rather than derived
// through the function under test.
//
// Every case's workflow name and step id feed [auth.WorkloadIdentity.SubjectFor],
// which is part of what a denial's text names — so both drivers, given the
// same Authority and the same Workflow, produce the identical sentence: the
// local driver reaches it by patching the workflow name into a
// [v1.TaskRuntime] installed on the context, the durable driver by passing it
// as an activity argument at worker registration, and neither can drift from
// the other without this catching it.
func AuthorityDenialCases() []AuthorityCase {
	const unreachable = "https://authority-denial.invalid/never-dialed"

	identity := auth.WorkloadIdentity{
		Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
	}

	return []AuthorityCase{
		{
			Case: Case{
				Name: "a bearer reference fails closed with no runtime configured",
				Workflow: &v1.Workflow{
					Name:  "authority-fail-closed-bearer",
					Steps: []*v1.Node{bearerSecretStep("read", unreachable, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`resolving bearer reference fixture-secret:API_TOKEN: ` +
						`secret access is not configured on this worker`),
				}},
			},
			Authority: Authority{NoRuntime: true},
		},
		{
			Case: Case{
				Name: "a credential target fails closed with no runtime configured",
				Workflow: &v1.Workflow{
					Name:  "authority-fail-closed-credential",
					Steps: []*v1.Node{credentialStep("read", unreachable, "partner-api")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`authorizing federation target "partner-api": ` +
						`workload identity federation is not configured on this worker`),
				}},
			},
			Authority: Authority{NoRuntime: true},
		},
		{
			Case: Case{
				Name: "a credential target fails closed when secrets are configured but federation is not",
				Workflow: &v1.Workflow{
					Name:  "authority-fail-closed-broker",
					Steps: []*v1.Node{credentialStep("read", unreachable, "partner-api")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`authorizing federation target "partner-api": ` +
						`workload identity federation is not configured on this worker`),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "unused", Allow: []string{"true"}, Identity: identity,
			},
		},
		{
			Case: Case{
				Name: "a deny rule refuses a bearer reference",
				Workflow: &v1.Workflow{
					Name:  "authority-denied-bearer",
					Steps: []*v1.Node{bearerSecretStep("read", unreachable, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`resolving bearer reference fixture-secret:API_TOKEN: ` +
						`auth: denied by secret access policy: no rule permits workload ` +
						`"flowstate:acme-tenant/default/authority-denied-bearer/read" ` +
						`in namespace "acme-tenant" to read fixture-secret:API_TOKEN (deny rule: true)`),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "must-not-resolve",
				Allow: []string{"true"}, Deny: []string{"true"}, Identity: identity,
			},
		},
		{
			// The nested position, failing closed exactly as the whole-value one
			// does. A reference inside a header map is resolved by the same
			// activity through the same authority, so a worker with none refuses
			// it in the same words — with the header named, because a mapping can
			// hold several and an author needs to know which.
			Case: Case{
				Name: "a header reference fails closed with no runtime configured",
				Workflow: &v1.Workflow{
					Name:  "authority-fail-closed-header",
					Steps: []*v1.Node{headerSecretStep("read", unreachable, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`header "Authorization": resolving reference fixture-secret:API_TOKEN: ` +
						`secret access is not configured on this worker`),
				}},
			},
			Authority: Authority{NoRuntime: true},
		},
		{
			Case: Case{
				Name: "a deny rule refuses a header reference",
				Workflow: &v1.Workflow{
					Name:  "authority-denied-header",
					Steps: []*v1.Node{headerSecretStep("read", unreachable, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`header "Authorization": resolving reference fixture-secret:API_TOKEN: ` +
						`auth: denied by secret access policy: no rule permits workload ` +
						`"flowstate:acme-tenant/default/authority-denied-header/read" ` +
						`in namespace "acme-tenant" to read fixture-secret:API_TOKEN (deny rule: true)`),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "must-not-resolve",
				Allow: []string{"true"}, Deny: []string{"true"}, Identity: identity,
			},
		},
		{
			// Same denial as above, under a case whose whole point is the
			// ordering: the caller asserts, after running this, that
			// Authority.ProviderCalls never advanced past zero — proof the
			// provider was never consulted, not merely that the eventual
			// error text reads as if it wasn't.
			Case: Case{
				Name: "policy runs before the provider is ever consulted",
				Workflow: &v1.Workflow{
					Name:  "authority-denied-ordering",
					Steps: []*v1.Node{bearerSecretStep("read", unreachable, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"read": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` +
						`resolving bearer reference fixture-secret:API_TOKEN: ` +
						`auth: denied by secret access policy: no rule permits workload ` +
						`"flowstate:acme-tenant/default/authority-denied-ordering/read" ` +
						`in namespace "acme-tenant" to read fixture-secret:API_TOKEN (deny rule: true)`),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "must-not-resolve",
				Allow: []string{"true"}, Deny: []string{"true"}, Identity: identity,
				ProviderCalls: new(atomic.Int32),
			},
		},
	}
}

// AuthorityContainmentCases exercise a secret and a JIT credential that
// resolve successfully, and pin that the revealed material never survives
// into a run's recorded output — the matrix CLAUDE.md's secrets discipline
// asks for, run once and shared by both drivers rather than proven separately
// by each.
//
// baseURL should come from [NewHTTPServer]; each case points its step at
// "/reflect-authorization", which echoes the Authorization header a bearer or
// a minted credential produced, so what comes back is exactly the shape a
// peer reflecting a credential into its response takes — the path that turns
// a request credential into a durable output if scrubbing ever misses it.
func AuthorityContainmentCases(baseURL string) []AuthorityCase {
	identity := auth.WorkloadIdentity{
		Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
	}

	const bearerMaterial = "material-that-must-not-appear-in-any-rendering-bearer"
	const jitMaterial = "material-that-must-not-appear-in-any-rendering-jit"
	const headerMaterial = "material-that-must-not-appear-in-any-rendering-header"

	reflectOutputs := v1.NewExpr(`{"body": response.body, "reflected": response.headers["X-Reflected"][0]}`)

	// A bearer step only ever registers the revealed secret itself with the
	// scrubber, so "Bearer <secret>" redacts to "Bearer [REDACTED]" — the
	// prefix survives because it was never material. A credential step
	// registers the whole rendered header too (see eval_task_library.go),
	// because the broker chooses the header's shape and the activity does
	// not assume it always says "Bearer "; the longest match wins, so
	// "Bearer <token>" collapses to a single [REDACTED] with no prefix left.
	// The two expected shapes below are that difference, not a mistake in one
	// of them.
	bearerRedacted := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"call": {NamedValues: map[string]*v1.Value{
			"body":      v1.NewLiteral("echo: " + secrets.Redacted),
			"reflected": v1.NewLiteral("Bearer " + secrets.Redacted),
		}},
	}}
	// A header written by the author carries what the author wrote and nothing
	// else, so the reflected value is the material alone — one [REDACTED] with no
	// prefix, the same shape the JIT case produces and for the opposite reason.
	headerRedacted := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"call": {NamedValues: map[string]*v1.Value{
			"body":      v1.NewLiteral("echo: " + secrets.Redacted),
			"reflected": v1.NewLiteral(secrets.Redacted),
		}},
	}}
	jitRedacted := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"call": {NamedValues: map[string]*v1.Value{
			"body":      v1.NewLiteral("echo: " + secrets.Redacted),
			"reflected": v1.NewLiteral(secrets.Redacted),
		}},
	}}

	return []AuthorityCase{
		{
			Case: Case{
				Name: "a resolved bearer secret is contained end to end",
				Workflow: &v1.Workflow{
					Name: "authority-contained-bearer",
					Steps: []*v1.Node{{
						Id: "call",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":     v1.NewLiteral(baseURL + "/reflect-authorization"),
								"bearer":  {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
								"outputs": reflectOutputs,
							},
						}},
					}},
				},
				ExpectedOutputs: bearerRedacted,
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: bearerMaterial,
				Allow: []string{"true"}, Identity: identity,
			},
			ContainmentValue: bearerMaterial,
		},
		{
			// The same containment, from the position a reference reaches by
			// being nested rather than by being a whole input. It matters
			// separately: `bearer:` hands its value to one `Header.Set` in a
			// function written around a single reference, while a header map is
			// walked, and a walk is where a value gets copied into something that
			// outlives it. What comes back is the material the peer reflected,
			// redacted — and with no "Bearer " prefix, because this header carries
			// the credential exactly as the author wrote it.
			Case: Case{
				Name: "a secret nested in a header is contained end to end",
				Workflow: &v1.Workflow{
					Name: "authority-contained-header",
					Steps: []*v1.Node{{
						Id: "call",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url": v1.NewLiteral(baseURL + "/reflect-authorization"),
								"headers": v1.NewStructureMap(map[string]*v1.Value{
									"Accept":        v1.NewLiteral("application/json"),
									"Authorization": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
								}),
								"outputs": reflectOutputs,
							},
						}},
					}},
				},
				ExpectedOutputs: headerRedacted,
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: headerMaterial,
				Allow: []string{"true"}, Identity: identity,
			},
			ContainmentValue: headerMaterial,
		},
		{
			Case: Case{
				Name: "a minted JIT credential is contained end to end",
				Workflow: &v1.Workflow{
					Name: "authority-contained-jit",
					Steps: []*v1.Node{{
						Id: "call",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":        v1.NewLiteral(baseURL + "/reflect-authorization"),
								"credential": v1.NewLiteral("partner-api"),
								"outputs":    reflectOutputs,
							},
						}},
					}},
				},
				ExpectedOutputs: jitRedacted,
			},
			Authority: Authority{
				Identity:   identity,
				Federation: &Federation{Target: "partner-api", Token: jitMaterial},
			},
			ContainmentValue: jitMaterial,
		},
	}
}

// AssertNoLeak fails tb if material appears in any observable rendering of
// out — the containment matrix CLAUDE.md's secrets discipline asks for: the
// value itself, a struct holding it through an unexported field (the shape
// `fmt` cannot call a redacting method through, and so reflects into
// instead), and a slice of those, under %v, %+v, %#v and %s.
//
// Operating on [v1.Workflow_StepOutputs] rather than on a single task's
// [v1.Node_Outputs] is what lets both drivers call this: it is the type both
// [v1.Run] and [engine.Run] hand back, so one assertion covers the shape both
// produce instead of each driver needing its own rendering of "the result".
func AssertNoLeak(tb testing.TB, out *v1.Workflow_StepOutputs, material string) {
	tb.Helper()

	if material == "" {
		tb.Fatal("AssertNoLeak called with no material to check for")
	}

	// A struct holding the outputs through an unexported field, which is the
	// arrangement `fmt` cannot call a method on and therefore reflects into.
	type holder struct{ outputs *v1.Workflow_StepOutputs }

	renderings := map[string]string{
		"%v on the outputs":  fmt.Sprintf("%v", out),
		"%+v on the outputs": fmt.Sprintf("%+v", out),
		"%#v on the outputs": fmt.Sprintf("%#v", out),
		// `out.String()` would be the same string and a different test. What is
		// under test is the verb, not the method: a log line writes `%s` and fmt
		// decides what to call, so calling String() here would assert only that
		// the method redacts and stop covering the path an operator's formatter
		// actually takes.
		//lint:ignore S1025 the %s verb is one of the containment shapes, not a roundabout String()
		"%s on the outputs":      fmt.Sprintf("%s", out),
		"%v on a struct":         fmt.Sprintf("%v", holder{outputs: out}),
		"%+v on a struct":        fmt.Sprintf("%+v", holder{outputs: out}),
		"%#v on a struct":        fmt.Sprintf("%#v", holder{outputs: out}),
		"%v on a slice":          fmt.Sprintf("%v", []holder{{outputs: out}}),
		"%+v on a slice":         fmt.Sprintf("%+v", []holder{{outputs: out}}),
		"%#v on a slice":         fmt.Sprintf("%#v", []holder{{outputs: out}}),
		"%v on the step values":  fmt.Sprintf("%v", out.GetStepValues()),
		"%#v on the step values": fmt.Sprintf("%#v", out.GetStepValues()),
	}
	for name, rendered := range renderings {
		if strings.Contains(rendered, material) {
			tb.Errorf("the revealed value appears under %s, so a log line or an error "+
				"built that way would carry it into somewhere durable", name)
		}
	}
}
