// Package auth establishes identity in both directions between Flowstate and the
// systems around it, using OpenID Connect and Workload Identity Federation.
//
// Trust runs two ways, and both halves live here:
//
//   - Inbound, Flowstate is a relying party. A GitHub Actions workflow, a
//     Kubernetes pod, or a person's browser presents a token; Flowstate verifies
//     it and gets a [Principal]. Start at [Policy] and [OIDCVerifier].
//   - Outbound, Flowstate is an identity. A workload presents a signed assertion
//     of its own to AWS, Google Cloud, or a partner API, and trades it for a
//     short-lived credential. Start at [FederationPolicy] and [Broker].
//
// Neither direction uses a shared secret. That is the point: a deployment holds
// one signing key, publishes the public half, and everything else is a trust
// relationship an operator can read and review. The two directions differ in
// how withdrawing that trust takes effect. Outbound, reviewing it is
// prospective, not retroactive: withdrawing an issuer or narrowing a policy
// stops [Broker] from minting further assertions or exchanging them for new
// credentials, but does nothing to an assertion or exchanged credential
// already issued within its lifetime — there is no revocation of those (see
// THREAT_MODEL.md, "The issuer as a single point of failure" and "Non-goals
// and honest gaps"). Short assertion and credential lifetimes are what bound
// that exposure. Inbound is different: [OIDCVerifier] checks issuer
// membership and claim rules against the [Policy] it was built with, and it
// checks them on every request, not only at token mint time. Removing an
// issuer or tightening a claim rule and reconstructing the verifier with the
// new policy does invalidate access for previously-valid inbound tokens,
// starting with the next request each one presents — even though the token
// itself, as a JWT, is not and cannot be revoked.
//
// # Inbound: authenticating callers
//
// Flowstate starts workflows, so a caller who can reach the API can run code.
// This package exists to make sure the API knows who that caller is. It verifies
// a bearer token against keys the token's issuer publishes, checks the token
// really was minted for this deployment, and turns it into a [Principal] that
// authorization decisions can key on.
//
// # Failing closed
//
// Every path that is not a successfully verified token is a rejection. A missing
// Authorization header, a header that is not a bearer token, a token that does
// not parse, an issuer that is not in the trust policy, a signature that does not
// verify, an expired token, a token minted for another audience, a token whose
// claims do not satisfy the policy, an unreachable issuer, and a verifier that
// was never configured all end the same way: [connect.CodeUnauthenticated].
//
// There is no configuration in which this package accepts an unsigned ("none")
// token, and none in which it verifies an HMAC-signed token against an issuer's
// public key, which is the shape of the classic algorithm confusion attack. A
// token is only ever verified with a key whose type matches the algorithm it
// claims.
//
// Anonymous access exists for local development, but only through
// [InsecureAnonymousVerifier], which an operator has to ask for by name. It is
// not a default, not a fallback, and not what a zero value does.
//
// # Configuring an identity provider
//
// A [Policy] names the issuers Flowstate trusts. For ordinary single sign-on,
// one issuer and the audience it mints tokens for is the whole configuration:
//
//	policy := auth.Policy{
//		Issuers: []auth.TrustedIssuer{{
//			Name:      "okta",
//			Issuer:    "https://example.okta.com/oauth2/default",
//			Audiences: []string{"flowstate"},
//			Role:      "operator",
//		}},
//	}
//
//	verifier, err := auth.NewOIDCVerifier(policy)
//
// The issuer's signing keys are found through its
// /.well-known/openid-configuration document, cached, and refetched when the
// issuer rotates them, so key rotation needs no operator involvement.
//
// # Configuring Workload Identity Federation
//
// Workload Identity Federation replaces long-lived Flowstate credentials with
// tokens the caller's own platform already issues. Nothing is deployed with a
// secret: the platform attests to the workload, and the policy says which
// attestations are good enough. That is a matter of adding issuers, not code.
//
// A GitHub Actions workflow, restricted to one repository's main branch:
//
//	auth.TrustedIssuer{
//		Name:      "github-actions-flowstate-main",
//		Issuer:    "https://token.actions.githubusercontent.com",
//		Audiences: []string{"flowstate"},
//		Require: []auth.ClaimRule{
//			auth.RequireClaim("repository", "picatz/flowstate"),
//			auth.RequireClaim("ref", "refs/heads/main"),
//		},
//		Role:        "deployer",
//		MaxTokenAge: 10 * time.Minute,
//	}
//
// A Kubernetes projected service account token, restricted to one service
// account:
//
//	auth.TrustedIssuer{
//		Name:      "k8s-runner",
//		Issuer:    "https://kubernetes.default.svc.cluster.local",
//		Audiences: []string{"flowstate"},
//		Require: []auth.ClaimRule{
//			auth.RequireClaim("sub", "system:serviceaccount:flowstate:runner"),
//		},
//		Role: "runner",
//	}
//
// The claim rules are the point. An issuer with no rules trusts every workload
// that platform will ever mint a token for, which for a public CI provider means
// everyone. Rules match exactly, never by prefix or pattern, so a policy cannot
// accidentally trust more than it names.
//
// For the public multi-tenant issuers this package knows by name — GitHub
// Actions, GitLab.com, HCP Terraform — that is enforced rather than advised: an
// entry naming one of them is refused when the policy loads unless it carries a
// require rule or a namespace_claim. An audience does not substitute for either,
// because on those platforms the audience is a value the workload requesting the
// token names. The list of such issuers is a floor and not a ceiling — it says
// nothing about an issuer it has not heard of, since an audience is a real
// restriction for a single-tenant issuer whose tokens only its own operator can
// obtain.
//
// Several entries may share an issuer, which is how one platform grants
// different roles to different workloads. The first entry whose audience and
// rules a token satisfies wins, and [Principal.IssuerName] records which one it
// was, so an audit log shows the rule that admitted a caller rather than only the
// issuer that signed the token.
//
// A policy is data, and can be kept in a file next to the rest of a deployment's
// configuration and reviewed like any other change. See [ParsePolicy]:
//
//	issuers:
//	  - name: github-actions-flowstate-main
//	    issuer: https://token.actions.githubusercontent.com
//	    audiences: [flowstate]
//	    role: deployer
//	    max_token_age: 10m
//	    require:
//	      - claim: repository
//	        any_of: [picatz/flowstate]
//	      - claim: ref
//	        any_of: [refs/heads/main]
//
// # Serving
//
// [Authenticator] adapts a [Verifier] to Connect's authentication middleware,
// which runs before request bodies are decoded:
//
//	authenticator := auth.NewAuthenticator(verifier,
//		auth.WithFailureObserver(func(ctx context.Context, req *http.Request, err error) {
//			slog.WarnContext(ctx, "rejected unauthenticated request", "error", err)
//		}),
//	)
//
//	httpServer := &http.Server{
//		Handler: authn.NewMiddleware(authenticator.Authenticate).Wrap(mux),
//	}
//
// Handlers then recover the caller with [PrincipalFromContext] and use
// [Principal.ID] as the identity to authorize against.
//
// Tokens and keys are never logged by this package. [Principal] implements
// [log/slog.LogValuer] so that logging a caller records its identity, issuer, and
// role without spilling the rest of its claims.
//
// # Outbound: Flowstate as an identity
//
// A workload usually has to call something: push an image, write to a bucket, tell
// a partner API that an order shipped. The traditional answer is a stored
// credential, which then has to be distributed, rotated, and explained to an
// auditor. The alternative is for Flowstate to have an identity of its own, prove
// it, and trade that proof for a credential that expires in minutes.
//
// [Issuer] mints those proofs. Each [Assertion] is a short-lived JWT naming one
// workload, addressed to one relying party, signed by a key whose public half is
// published at [Issuer.Handler]:
//
//	key, err := auth.GenerateSigningKey("2026-07", jwa.ES256)
//	issuer, err := auth.NewIssuer("https://flowstate.example.com", key)
//
//	mux.Handle(auth.DiscoveryPath, issuer.Handler())
//	mux.Handle(issuer.JWKSPath(), issuer.Handler())
//
// A process that starts this way publishes exactly one key, which is why
// rotating one across a restart takes [WithVerifyOnlyKey]: it publishes a
// previous key's public half beside the signing key, so assertions the process
// before this one signed keep verifying until the retention lapses. That is
// what `flow`'s repeatable --identity-key builds, and [Issuer.Rotate] is its
// in-process counterpart for a deployment that never restarts.
//
// The subject names the workload hierarchically, so a relying party can authorize
// at whatever level it wants with a prefix match:
//
//	flowstate:<namespace>/<deployment>/<workflow>/<step>          server-attested
//	flowstate:_local/<namespace>/<deployment>/<workflow>/<step>   flow run local
//
// Two components are reserved and begin with an underscore: "_default" stands in
// for a namespace or deployment nobody set, and "_local" marks an assertion
// minted by `flow run local` rather than by a server-attested run. Both are
// unforgeable by an operator-chosen namespace, and for the same reason: the
// grammar that admits a namespace into this subject ([ValidateNamespace])
// forbids the underscore, so no namespace can ever equal either reserved
// segment. That is what makes a subject's meaning a property of the string
// itself rather than of an operator's discipline — a trust policy written for
// "flowstate:acme/prod/..." cannot match a namespace that renamed itself
// "_default" or "_local", because no namespace can. The same reasoning
// protects `flow run local`: `flow run local --identity-key <prod key>
// --as-namespace acme --as-deployment prod` would otherwise mint an assertion
// byte-indistinguishable from a server-attested one. With "_local" prepended,
// it cannot, on AWS, GCP, or any other RFC 8693 peer, because AWS STS ignores
// custom claims and can only condition a trust policy on "sub" and "aud" — a
// run-mode marker carried only as a claim would be unenforceable there. The
// mode is set by which constructor built the [WorkloadIdentity]
// ([NewLocalWorkloadIdentity] versus [IdentityFromPrincipal] or [IdentityFrom]),
// never by a flag, since the field recording it is unexported.
//
// A local run's [ClaimNamespace] claim and the workload attributes an
// assumption rule sees are unaffected by any of this: only the subject gains
// the "_local" segment, so a local rehearsal still exercises Flowstate's own
// assumption policy exactly as a server-attested run would. What fails, and
// should, is the final exchange with the cloud provider — the rehearsal is
// faithful right up to the boundary that only a real deployment can cross. A
// driver-set "run_mode" claim ("local" or "server") carries the same
// distinction for relying parties that read claims, such as a GCP attribute
// mapping — belt and braces, where the braces work.
//
// Delegation stays visible rather than being flattened away. The subject says which
// workload is calling; the [ClaimOnBehalfOf] and [ClaimOnBehalfOfIssuer] claims say
// who caused it to run. A relying party can require both: this workload, acting for
// that pipeline.
//
// # Outbound: obtaining credentials
//
// [Broker] is what a task uses. Given a [WorkloadIdentity] and a [StepRef], it
// decides whether the workload may reach a target, mints an assertion for exactly
// that target, exchanges it, and caches the result until shortly before it expires:
//
//	identity := auth.IdentityFrom(state.GetIdentity())
//	ref := auth.StepRef{Workflow: workflowName, Run: runID, Step: stepID}
//
//	credential, err := broker.Credential(ctx, identity, ref, "aws-prod")
//
// Or, for an HTTP call, without the task ever holding the secret:
//
//	err := broker.Authorize(ctx, req, identity, ref, "aws-prod")
//
// Exchanging is an [Exchanger], and supporting a new system is an implementation of
// that one interface. Four come with this package: [NewTokenExchanger] for RFC 8693
// OAuth 2.0 Token Exchange, which is the standards-based path to prefer;
// [NewAWSExchanger] for STS AssumeRoleWithWebIdentity; [NewGCPExchanger] for Google
// Cloud Workload Identity Federation; and [NewClientCredentialsExchanger] for plain
// service-to-service calls, authenticated by the assertion rather than a secret.
//
// # Outbound: who may assume what
//
// Which workloads may reach which targets is CEL, the same language workflow
// conditions and egress rules are written in. Rules are compiled and type-checked
// when the policy is built, deny beats allow, and a rule that cannot be evaluated
// refuses:
//
//	# assumption policy
//	allow:
//	  - 'target == "aws-prod" && workload.on_behalf_of.startsWith("repo:picatz/flowstate:")'
//	  - 'target == "partner" && workload.namespace == "acme"'
//	deny:
//	  - 'workload.step == "debug"'
//
// A rule sees target, audience, and the workload object, whose fields are the same
// names as the assertion's claims: workload.subject, workload.namespace,
// workload.deployment, workload.workflow, workload.run, workload.step,
// workload.on_behalf_of, workload.on_behalf_of_issuer, and workload.claims.
//
// # Tenancy
//
// Teams sharing a deployment need their workloads, secrets, and egress kept apart,
// and a namespace is that boundary. One rule decides whether the boundary is real:
//
// A workload's namespace comes from the authenticated caller, never from the
// workload. A trust policy entry either fixes the namespace for every caller it
// admits, or names the claim to read it from:
//
//	issuers:
//	  - name: github-actions
//	    issuer: https://token.actions.githubusercontent.com
//	    audiences: [flowstate]
//	    namespace_claim: repository_owner
//
// A verified caller whose namespace cannot be determined is rejected with
// [ErrNoNamespace], not admitted to a shared one. And a policy is either
// tenant-aware or it is not: if one issuer determines a namespace, every issuer
// must, because the ones that did not would put their callers in a namespace
// alongside tenants meant to be separated. There is no switch to forget.
//
// The namespace reaches [Principal.Namespace], then [WorkloadIdentity.Namespace],
// then every assertion subject and every policy decision the workload's steps make.
//
// [ValidateNamespace] is the one grammar a namespace is checked against on that
// whole path — a signed subject, a secret provider's path or environment
// variable name — never two. secrets.ValidateNamespace delegates to it rather
// than checking separately, which is what makes "one value, one grammar" true
// of the running system and not only of the intent.
//
// # Authorizing secrets
//
// Whether a workload may read a secret is the same question as whether it may
// assume a downstream identity, so it is asked in the same language against the
// same workload attributes, with a secret object added:
//
//	# secret access policy
//	secrets:
//	  allow:
//	    - 'secret.scheme == "env" && secret.name.startsWith(workload.namespace + "_")'
//	  deny:
//	    - 'secret.name.endsWith("_ROOT")'
//
// A secret store calls [SecretPolicy.Authorize] before resolving a reference.
// Unlike credential targets, **no rules means nothing is permitted**: a target has
// to be configured before it exists, so an unconfigured one is already a refusal,
// whereas a secret scheme becomes readable the moment a provider is registered. The
// refusal says so — naming the workload and the reference — rather than reporting
// the secret as missing, because those need different fixes.
//
// # Outbound: credentials never enter workflow history
//
// Minting reads the clock and exchanging calls the network, so both must happen in
// an activity. Workflow code is replayed against recorded history, and a replay
// cannot reproduce either one.
//
// The API is built so that getting this wrong fails closed rather than leaking. The
// secret material in [Credential] and the token in [Assertion] are unexported and
// are dropped by any serializer, including the one a durable execution backend uses
// to record an activity's result. A credential mistakenly returned to a workflow
// therefore arrives with its metadata and no secret, and using it reports
// [ErrCredentialUnresolved]. Resolve credentials in the activity that presents
// them, and let them go out of scope with it.
package auth
