package auth

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"
)

// Broker resolves the credentials a workload needs to call other systems.
//
// It is the entry point for outbound federation, and the only one a task should
// need: given who the workload is and what it wants to reach, it decides whether
// the workload may, mints an assertion scoped to that one relying party, exchanges
// it for a short-lived credential, and caches the result until shortly before it
// expires.
//
// # This must run in an activity
//
// Everything a Broker does is time-dependent and talks to the network: it reads
// the clock to mint, calls a relying party to exchange, and returns a value that
// differs every time. None of that can happen in workflow code, where a replay
// must reproduce the same result. Call it from the activity that uses the
// credential, and do not return the credential to the workflow.
//
// [Credential] is built so that violating this fails closed rather than leaking:
// its secret material is unexported and is dropped by any serializer, so a
// credential mistakenly returned from an activity arrives with no secret and its
// use reports [ErrCredentialUnresolved].
//
// A Broker is safe for concurrent use.
type Broker struct {
	issuer  *Issuer
	targets map[string]Exchanger
	rules   assumeRules
	cache   *credentialCache
	clock   func() time.Time
}

// brokerConfig collects the options for [NewBroker].
type brokerConfig struct {
	targets    map[string]Exchanger
	allow      []string
	deny       []string
	costLimit  uint64
	margin     time.Duration
	limit      int
	clock      func() time.Time
	duplicates []string
}

// A BrokerOption configures a [Broker].
type BrokerOption func(*brokerConfig)

// WithTarget registers a system a workload may obtain credentials for, under an
// operator-chosen name.
//
// The name is what assumption rules match on and what appears in audit records,
// so it should name the thing in the operator's terms, such as "aws-prod" or
// "partner-api", rather than restating the mechanism.
func WithTarget(name string, exchanger Exchanger) BrokerOption {
	return func(c *brokerConfig) {
		if name == "" || exchanger == nil {
			// Recorded rather than ignored: NewBroker reports it.
			c.duplicates = append(c.duplicates, name)
			return
		}
		if _, exists := c.targets[name]; exists {
			c.duplicates = append(c.duplicates, name)
			return
		}
		c.targets[name] = exchanger
	}
}

// WithAssumeAllowRules adds CEL rules that gate credential assumption. When any
// are configured, a request must match at least one of them.
func WithAssumeAllowRules(rules ...string) BrokerOption {
	return func(c *brokerConfig) { c.allow = append(c.allow, rules...) }
}

// WithAssumeDenyRules adds CEL rules that refuse credential assumption. A request
// matching any of them is refused, whatever the allow rules say.
func WithAssumeDenyRules(rules ...string) BrokerOption {
	return func(c *brokerConfig) { c.deny = append(c.deny, rules...) }
}

// WithAssumeRuleCostLimit bounds the CEL evaluation cost of a single rule.
func WithAssumeRuleCostLimit(limit uint64) BrokerOption {
	return func(c *brokerConfig) { c.costLimit = limit }
}

// WithRefreshMargin sets how long before expiry a cached credential is exchanged
// again, so a credential handed to a caller has time left to be used.
func WithRefreshMargin(margin time.Duration) BrokerOption {
	return func(c *brokerConfig) { c.margin = margin }
}

// WithMaxCachedCredentials bounds how many credentials are cached at once.
func WithMaxCachedCredentials(limit int) BrokerOption {
	return func(c *brokerConfig) { c.limit = limit }
}

// WithBrokerClock sets the clock used for credential expiry. It exists for tests.
func WithBrokerClock(clock func() time.Time) BrokerOption {
	return func(c *brokerConfig) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// NewBroker returns a broker that mints assertions with the given issuer.
//
// Assumption rules are compiled and type-checked here, so a rule that references
// an attribute that does not exist, or that does not produce a boolean, is a
// startup error rather than a surprise at the first credential request.
//
// A broker with no targets is valid and refuses every request with
// [ErrUnknownTarget]. A broker with no rules permits any workload to use any
// configured target, which is why a real deployment writes rules.
func NewBroker(issuer *Issuer, opts ...BrokerOption) (*Broker, error) {
	if issuer == nil {
		return nil, fmt.Errorf("%w: a broker needs an issuer to mint assertions with", ErrInvalidPolicy)
	}

	cfg := brokerConfig{
		targets:   make(map[string]Exchanger),
		costLimit: DefaultAssumeRuleCostLimit,
		margin:    DefaultRefreshMargin,
		limit:     DefaultMaxCachedCredentials,
		clock:     time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if len(cfg.duplicates) > 0 {
		return nil, fmt.Errorf("%w: target %q is registered more than once, or has no name or exchanger",
			ErrInvalidPolicy, cfg.duplicates[0])
	}

	for name, exchanger := range cfg.targets {
		if requirement := exchanger.Requirement(); requirement.Audience == "" {
			// Without an audience the broker would mint an assertion that any
			// relying party would accept, so an exchanger that does not name one
			// cannot be used.
			return nil, fmt.Errorf("%w: exchanger %q for target %q requires no audience",
				ErrInvalidPolicy, exchanger.Name(), name)
		}
	}

	switch {
	case cfg.margin < 0:
		return nil, fmt.Errorf("%w: refresh margin must not be negative", ErrInvalidPolicy)
	case cfg.limit <= 0:
		return nil, fmt.Errorf("%w: credential cache limit must be positive", ErrInvalidPolicy)
	case cfg.costLimit == 0:
		return nil, fmt.Errorf("%w: assumption rule cost limit must be positive", ErrInvalidPolicy)
	}

	rules, err := compileAssumeRules(cfg.allow, cfg.deny, cfg.costLimit)
	if err != nil {
		return nil, err
	}

	return &Broker{
		issuer:  issuer,
		targets: cfg.targets,
		rules:   rules,
		cache:   newCredentialCache(cfg.clock, cfg.margin, cfg.limit),
		clock:   cfg.clock,
	}, nil
}

// Targets returns the configured target names, sorted. It exists so a server can
// report what it is able to reach, and so an operator can confirm a policy file
// produced the targets they meant.
func (b *Broker) Targets() []string {
	return slices.Sorted(maps.Keys(b.targets))
}

// Issuer returns the issuer whose assertions this broker mints, so a server can
// publish its discovery document and key set.
func (b *Broker) Issuer() *Issuer { return b.issuer }

// Credential returns a credential the workload may use against target.
//
// The order is deliberate. Policy is evaluated before anything is minted, so a
// refused request produces no assertion at all: nothing signed by Flowstate exists
// for a workload that was not allowed to ask. Then an assertion is minted for
// exactly the audience that one relying party requires, which is what stops a
// credential request for one target from producing something usable at another.
//
// Credentials are cached per workload, per target, keyed on everything that shapes
// the assertion, and refreshed shortly before they expire. A cached credential is
// never shared between workloads acting for different callers.
func (b *Broker) Credential(ctx context.Context, identity WorkloadIdentity, ref StepRef, target string) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}

	exchanger, ok := b.targets[target]
	if !ok {
		return Credential{}, fmt.Errorf("%w: %q is not a configured target", ErrUnknownTarget, truncate(target, 128))
	}

	if err := identity.Validate(); err != nil {
		return Credential{}, err
	}

	subject, err := identity.SubjectFor(ref)
	if err != nil {
		return Credential{}, err
	}

	requirement := exchanger.Requirement()

	if err := b.rules.evaluate(ctx, target, subject, assumeVars(target, subject, requirement.Audience, identity, ref)); err != nil {
		return Credential{}, err
	}

	// A credential is cached under the workload identity and target, which are
	// the whole of what determines it — unless the exchange also depends on a
	// party neither of them names.
	//
	// A delegated exchange does. The same workload, step and target acting for
	// two different delegators produces two different credentials, minted by
	// the authorization server for two different subjects with possibly
	// different scopes; caching under a key that cannot tell them apart hands
	// the second caller the first delegator's credential. That is the tenancy
	// mistake this repository already legislates against, arriving through a
	// cache key rather than through a name: a boundary that holds for every
	// pair the key distinguishes and fails silently for the pair it does not.
	//
	// So a delegated exchange is not cached. The cost is stated rather than
	// hidden: one exchange per request against such a target, with no reuse
	// inside the credential's lifetime. Caching per delegator instead would
	// need a stable, non-secret discriminator for the delegator, which means
	// reading somebody else's token to derive one — new surface, and a
	// discriminator that is wrong is this same leak with more machinery in
	// front of it. When the grant model lands (#567 D1/D2) it can decide what
	// a delegated credential is keyed by; until then it is keyed by nothing,
	// which is the fail-closed answer.
	// Every exit from here down is one the policy already permitted, so a
	// failure below is [AssumptionFailedError] rather than a bare exchange
	// error: the decision happened, and a caller recording decisions has to be
	// able to tell it from the refusal it currently looks like. The wrapper
	// unwraps to the failure itself, so [Retryable] and every errors.Is check
	// against the exchange sentinels answer exactly as before.
	if delegated, ok := exchanger.(delegatingExchanger); ok && delegated.isDelegated() {
		credential, err := b.exchange(ctx, exchanger, requirement, identity, ref, subject, target)
		if err != nil {
			return Credential{}, assumptionFailed(target, err)
		}

		return credential, nil
	}

	credential, err := b.cache.get(ctx, credentialKey(target, subject, identity), func(ctx context.Context) (Credential, error) {
		return b.exchange(ctx, exchanger, requirement, identity, ref, subject, target)
	})
	if err != nil {
		return Credential{}, assumptionFailed(target, err)
	}

	return credential, nil
}

// delegatingExchanger is implemented by an [Exchanger] whose result depends on
// something the workload identity and target do not name — today, the delegator
// an RFC 8693 delegated exchange acts for.
//
// An optional interface rather than a method on [Exchanger], because that
// interface is implemented outside this package: adding a method would break
// every external exchanger to describe a property almost none of them have. An
// exchanger that does not implement this is cached exactly as before, which is
// the right default — the only exchanger that answers yes is one that was
// deliberately configured with a delegator.
type delegatingExchanger interface {
	// isDelegated reports whether this exchange depends on a delegator, and so
	// must not be served from a cache keyed without one.
	isDelegated() bool
}

// exchange mints an assertion and trades it for a credential.
func (b *Broker) exchange(ctx context.Context, exchanger Exchanger, requirement Requirement, identity WorkloadIdentity, ref StepRef, subject, target string) (Credential, error) {
	// A protocol that dictates its own subject gets it here. The policy above
	// evaluated the workload's real subject either way, so an override cannot
	// widen what a workload is allowed to do.
	minted := subject
	if requirement.Subject != "" {
		minted = requirement.Subject
	}

	assertion, err := b.issuer.mintFor(ctx, identity, ref, minted, requirement.Audience)
	if err != nil {
		return Credential{}, err
	}

	credential, err := exchanger.Exchange(ctx, assertion)
	if err != nil {
		return Credential{}, err
	}

	if credential.Type == "" {
		return Credential{}, fmt.Errorf("%w: exchanger %q returned a credential with no type",
			ErrExchangeFailed, exchanger.Name())
	}
	if credential.ExpiresAt.IsZero() {
		// A credential with no expiry would be cached forever and used until it
		// stopped working, which is the opposite of short-lived.
		return Credential{}, fmt.Errorf("%w: exchanger %q returned a credential with no expiry",
			ErrExchangeFailed, exchanger.Name())
	}

	// The operator's name for the target, not the exchanger's, is what belongs in
	// an audit record: it is what the assumption rules were written against.
	credential.Target = target
	if credential.Provider == "" {
		credential.Provider = exchanger.Name()
	}

	return credential, nil
}

// Authorize resolves the credential for target and attaches it to req.
//
// This is how a task should reach a protected system: the secret goes from the
// broker to the request header, and the task never holds a value it could log or
// return. It fails for credential types that cannot be presented as a header, such
// as AWS session credentials, which have to sign the request instead.
func (b *Broker) Authorize(ctx context.Context, req *http.Request, identity WorkloadIdentity, ref StepRef, target string) error {
	credential, err := b.Credential(ctx, identity, ref, target)
	if err != nil {
		return err
	}

	// The credential exists, so the policy permitted it; a header that cannot
	// be built from it is the same "decided, then failed" case the exchange
	// paths above are, and is reported the same way.
	return assumptionFailed(target, credential.Apply(req))
}
