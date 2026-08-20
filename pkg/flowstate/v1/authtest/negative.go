package authtest

// This file mints the three tokens a resource-server negative test asks for
// most often: one addressed to the wrong resource, one from an issuer a
// policy does not name, and one that carries an OAuth delegation claim a
// resource server that accepts no delegation must refuse. Each is meant to
// have exactly the one defect its name promises and no other: a helper here
// mints a token that is otherwise exactly what [Issuer.MintToken] would
// produce — correctly signed, unexpired, in every other respect valid —
// because a negative test fed a token with two defects cannot tell which one
// it caught. See this file's tests for the proof of that, one per helper.

// Claim names an [Issuer] does not fill in itself, minted only through
// [WithDelegation] and [WithMayAct].
//
// Both are RFC 8693 (OAuth 2.0 Token Exchange) claims, spelled exactly as
// that RFC and the JWT profile built on it spell them, so a claims map a
// caller writes by hand for one of these and one this package builds agree by
// construction — the same reason claimIssuer and its neighbors in token.go
// are spelled out here rather than imported.
//
// Neither is auth.ClaimOnBehalfOf or auth.ClaimOnBehalfOfIssuer, and the
// difference is deliberate rather than cosmetic: those two are claims
// Flowstate's own issuer mints into an assertion it is producing
// (pkg/flowstate/v1/auth/issuer.go), recording who a workload Flowstate is
// vouching for acts on behalf of. "act" and "may_act" are claims an external
// identity provider mints into a token Flowstate only verifies, recording who
// the token's own subject acts on behalf of. Flowstate does not read either
// today — a trust policy has nowhere to map them, and #567's D2 amendment
// leaves that mapping for S8 — which is exactly why a resource server that
// has not built that mapping must refuse a token carrying one rather than
// silently admitting it as the bare, undelegated subject: refusing preserves
// what the token itself claims, where admission would let the request
// proceed under a story the audit trail no longer tells.
const (
	// claimAct is RFC 8693's "act" claim: the actor a token's subject was
	// exercised through. A JWT carrying it says its bearer is not the party
	// that authenticated to the issuer, but an agent (or a chain of them)
	// acting on that party's behalf — see [WithDelegation].
	claimAct = "act"

	// claimMayAct is RFC 8693's "may_act" claim: a grant of permission to
	// become an actor, present in a token minted for the party being acted
	// for rather than in the delegate's own token. It is a weaker claim than
	// "act" — permission to delegate, not a record that delegation
	// happened — and the two must never share a spelling; see [WithMayAct].
	claimMayAct = "may_act"
)

// WithDelegation sets the token's "act" claim (RFC 8693) to actor, recording
// that the token's bearer is not the party named by "sub" but an agent
// exercising that party's token on its behalf. actor is minted exactly as
// given — typically {"sub": "<agent identifier>"}, and nestable
// ({"sub": "...", "act": {"sub": "..."}}) to model a delegation chain — the
// same claims-are-a-map-the-caller-fills-in contract [Issuer.MintToken]
// documents package-wide.
//
// actor must not be empty: a token that claims delegation happened while
// saying nothing about whom to is not a shape any issuer mints, and a helper
// that minted one would let a negative test pass without ever exercising
// what the claim says. Use [WithMayAct] for the distinct claim that grants
// permission to delegate without recording that it occurred.
func WithDelegation(actor map[string]any) TokenOption {
	if len(actor) == 0 {
		panic("authtest: WithDelegation needs a non-empty actor; an \"act\" claim with nothing in it is not what any issuer mints")
	}
	return func(o *tokenOptions) {
		if o.extraClaims == nil {
			o.extraClaims = make(map[string]any, 2)
		}
		o.extraClaims[claimAct] = actor
	}
}

// WithMayAct sets the token's "may_act" claim (RFC 8693) to principal,
// recording that the token's own subject grants permission for principal to
// act on its behalf in a future token — permission to delegate, not a record
// that delegation happened. See [WithDelegation] for the claim this is not.
//
// principal must not be empty, for the same reason [WithDelegation] refuses
// one: an empty grant is not a shape any issuer mints.
func WithMayAct(principal map[string]any) TokenOption {
	if len(principal) == 0 {
		panic("authtest: WithMayAct needs a non-empty principal; a \"may_act\" claim with nothing in it is not what any issuer mints")
	}
	return func(o *tokenOptions) {
		if o.extraClaims == nil {
			o.extraClaims = make(map[string]any, 2)
		}
		o.extraClaims[claimMayAct] = principal
	}
}

// WrongAudienceToken mints a token from i that is valid in every respect a
// resource server checks except its audience: signed by one of i's published
// keys, issued by i, unexpired, and addressed to audience — a resource other
// than the one the caller's policy trusts — instead of that resource. It is
// [Issuer.MintToken] with [WithAudience] applied after options, so it is the
// audience a test using this helper is proving gets checked, and nothing
// else: the audience given here always wins over one named in options, the
// same "last option wins" rule every [TokenOption] in this package follows.
//
// audience must not be empty — an empty audience is the no-audience hole
// [WithoutAudience] exists to make deliberate, not a value this helper mints
// by accident.
func (i *Issuer) WrongAudienceToken(audience string, claims map[string]any, options ...TokenOption) string {
	if audience == "" {
		panic("authtest: WrongAudienceToken needs a non-empty audience naming the resource the token is wrongly addressed to")
	}
	opts := make([]TokenOption, 0, len(options)+1)
	opts = append(opts, options...)
	opts = append(opts, WithAudience(audience))
	return i.MintToken(claims, opts...)
}

// WrongIssuerToken mints a token from a freshly created, independently keyed
// [Issuer] standing in for an identity provider a deployment's trust policy
// does not name: a policy trusting only some other issuer refuses the result
// solely because of who signed it. The token is otherwise exactly what
// [Issuer.MintToken] on that foreign issuer would mint: correctly signed by a
// key it publishes, unexpired, addressed to whatever audience tokenOptions
// name.
//
// issuerOptions configure the foreign issuer itself, and a test whose
// verifier runs on a deterministic clock must pass that clock here
// ([WithClock]), for this file's exactly-one-defect contract: a foreign
// issuer left on the wall clock timestamps its token against a different
// "now" than the verifier's, and if the two disagree by more than the
// verifier's leeway the token carries a latent lifetime defect too. The
// verifier reports [auth.ErrUntrustedIssuer] either way — issuer lookup
// happens before lifetime validation — so the second defect hides behind the
// first rather than failing the test that fed it.
//
// The returned issuer is not closed by this call. The caller owns it and
// must Close it — typically with the same t.Cleanup pattern used for a
// policy's trusted issuer — which is also what lets a test go on to prove the
// same token would have verified had this issuer been the trusted one.
func WrongIssuerToken(claims map[string]any, tokenOptions []TokenOption, issuerOptions ...IssuerOption) (token string, foreign *Issuer) {
	foreign = NewIssuer(issuerOptions...)
	// MintToken panics on invalid options (no audience named, an empty
	// subject, and so on), and by then the issuer's HTTP server is already
	// listening. The caller never receives foreign on that path, so nobody
	// else can close it: a test that recovers — assert.Panics, say — would
	// leak the listener and its serving goroutine once per call. Close it
	// here and re-panic, so the panic keeps its meaning and the server dies
	// with it.
	defer func() {
		if r := recover(); r != nil {
			_ = foreign.Close()
			panic(r)
		}
	}()
	token = foreign.MintToken(claims, tokenOptions...)
	return token, foreign
}
