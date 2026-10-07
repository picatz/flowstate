package auth

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// Inbound RFC 8693 delegation: the claims an inbound token may carry, which of
// them Flowstate reads, and the one place the refusals are spelled.
//
// Both are RFC 8693 (OAuth 2.0 Token Exchange) delegation claims: "act"
// records that the bearer is an agent exercising some other party's
// authority, and "may_act" records that the token's subject permits some
// other party to do so in a future token.
//
// # "act" is read, on an entry that opts in; "may_act" is refused
//
// An "act" chain is read only when the trust policy entry that admits the token
// has a `delegation:` stanza ([Delegation]). Without one the token is refused,
// never stripped: admitting the request as the bare subject named by "sub"
// would let it proceed while the audit record said nothing was delegated,
// which is the confused-deputy shape. With one, the chain becomes
// [Principal.Actors], and nothing else about the caller changes except that
// its actions can only shrink (see [Delegation]).
//
// "may_act" is a statement about a token that does not exist yet and has no
// meaning for a relying party that is not the exchange endpoint, so it is
// still refused outright on every entry.
//
// # Actors are data, not authority
//
// The chain is vouched for by exactly one party: the issuer that signed the
// token. An actor never authenticated to Flowstate, so nothing about it is
// verified beyond "this issuer said so". The kind, actions and issuer entry of
// the resulting [Principal] still come from the admitting entry alone, and an
// actor can only subtract from the actions that entry grants. A chain longer
// than [MaxActorDepth], or one that does not parse, is refused whole; a
// truncated chain would say something other than what the issuer signed.
//
// # Why this lives beside the verifier rather than in one surface's adapter
//
// It shipped inside [MCPTokenVerifier], which meant exactly one of this
// repository's two bearer surfaces refused a delegated token: the Connect RPC
// surface ([Authenticator]) admitted the identical token, so a token denied at
// MCP walked in through RPC. A refusal that one surface performs and another
// does not is not a policy, it is an accident of which file it was written in.
//
// So the decision is made by [OIDCVerifier.Verify], the admission path the
// repository's own verifier runs for every request, *and* the claim refusal is
// re-checked by each surface on the [Principal] its [Verifier] returned
// ([refuseDelegationClaims], through admitBearer): a [Verifier] is an
// interface, and a custom one that returned a [Principal] still carrying an
// unread "act" or "may_act" claim would otherwise sail past a surface that
// trusted the verifier. Two call sites, one spelling.
//
// These are deliberately not [ClaimOnBehalfOf]: that one is a claim
// Flowstate's own issuer *mints* into an assertion it is vouching for, where
// these two are claims an external identity provider mints into a token
// Flowstate only verifies. See pkg/flowstate/v1/authtest/negative.go, whose
// WithDelegation and WithMayAct mint exactly the tokens this refuses.
const (
	// ClaimActor is RFC 8693 section 4.1's "act".
	ClaimActor = "act"

	// ClaimMayAct is RFC 8693 section 4.4's "may_act".
	ClaimMayAct = "may_act"
)

// Bounds on an inbound actor chain. The token as a whole is already bounded
// ([maxTokenBytes], [maxVerifiedClaimBytes], [maxVerifiedClaimDepth]); these
// are the chain's own, spent where the chain is read.
const (
	// MaxActorDepth is the longest "act" chain Flowstate reads: the current
	// actor and the one it acts for. A chain nested deeper is refused whole.
	// Two is what the standard's own examples need, and each further link is
	// another party whose behaviour a rule would have to name.
	MaxActorDepth = 2

	// MaxActorFieldBytes bounds an actor's issuer and subject, the same bound
	// the wire Principal's subject has.
	MaxActorFieldBytes = 1024

	// maxActMembers bounds the members of one "act" object. Only "iss", "sub"
	// and the nested "act" mean anything to Flowstate; the rest (RFC 8693
	// allows others, which a relying party ignores) are bounded so that a wide
	// object is refused rather than walked.
	maxActMembers = 16

	// maxDelegationActors bounds how many actors one entry's allowlist names.
	maxDelegationActors = 32
)

// DelegationClaimError reports which delegation claim a token carried and why
// it was refused. It wraps [ErrDelegatedToken], and names the claim's *key* and
// never its value: the key is one of two constants above, while the value is an
// object the issuer filled in.
type DelegationClaimError struct {
	// Claim is [ClaimActor] or [ClaimMayAct].
	Claim string

	// Reason says, in words fixed by this package, what was wrong with an
	// "act" chain: never a value from the token, and never the allowlist that
	// refused it. Empty for a claim refused by presence alone.
	Reason string
}

// Error implements error.
func (e *DelegationClaimError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("%s: the token carries a %q delegation claim, which this deployment "+
			"does not interpret and will not ignore", ErrDelegatedToken.Error(), e.Claim)
	}

	return fmt.Sprintf("%s: the token's %q delegation claim is refused: %s", ErrDelegatedToken.Error(), e.Claim, e.Reason)
}

// Unwrap reports [ErrDelegatedToken], so callers match the class with
// [errors.Is] and read the claim key with [errors.As].
func (e *DelegationClaimError) Unwrap() error { return ErrDelegatedToken }

// refuseActChain is the refusal of an "act" claim, with the reason.
func refuseActChain(reason string) error {
	return &DelegationClaimError{Claim: ClaimActor, Reason: reason}
}

// refuseDelegationClaims returns a [DelegationClaimError] when a claims set
// carries a delegation claim, and nil when it does not.
//
// Presence alone is enough: the value is never read. It is applied to the
// claims a [Principal] carries, which an admitting entry decides, so a
// principal holding an unread chain in them is refused by every surface.
func refuseDelegationClaims(claims map[string]any) error {
	// Checked in a fixed order rather than by ranging the map, so a token
	// carrying both is always refused by naming the same one and a test can
	// assert the message.
	for _, name := range []string{ClaimActor, ClaimMayAct} {
		if _, present := claims[name]; present {
			return &DelegationClaimError{Claim: name}
		}
	}

	return nil
}

// delegationClaimOf reports the claim key a delegation refusal named, for
// [publicReason]. It reports "" for any other error.
func delegationClaimOf(err error) string {
	if delegation, ok := errors.AsType[*DelegationClaimError](err); ok {
		return delegation.Claim
	}

	return ""
}

// ActorChain reads the RFC 8693 `act` claim of a verified claims set as the
// chain of actors it names, current actor first, and nil when there is no
// claim.
//
// Each link is an object with a non-empty string "iss" and "sub" of at most
// [MaxActorFieldBytes], and may nest the previous actor in its own "act". Any
// other member of a link is ignored, as RFC 8693 section 4.1 directs, but a
// link may hold only a handful of them. The chain is refused whole, with a
// [DelegationClaimError], when a link is not an object, lacks "iss" or "sub",
// has one of the wrong type or over the bound, or the chain nests more than
// [MaxActorDepth] deep. Nothing is truncated: a chain cut short would name a
// different party than the issuer signed.
//
// Reading the chain admits nothing. Whether this deployment accepts it is the
// admitting entry's decision ([TrustedIssuer.Delegation]). The walk is
// iterative and stops at the third link, so the work is bounded by the limit
// and not by anything the token chose.
func ActorChain(claims map[string]any) ([]principal.Actor, error) {
	value, present := claims[ClaimActor]
	if !present {
		return nil, nil
	}

	var chain []principal.Actor
	for link := value; ; {
		object, ok := link.(map[string]any)
		if !ok {
			return nil, refuseActChain("an act link is not a JSON object")
		}
		if len(object) > maxActMembers {
			return nil, refuseActChain(fmt.Sprintf("an act link has more than %d members", maxActMembers))
		}
		if len(chain) == MaxActorDepth {
			return nil, refuseActChain(fmt.Sprintf("the chain nests deeper than %d", MaxActorDepth))
		}

		issuer, err := actorField(object, "iss")
		if err != nil {
			return nil, err
		}
		subject, err := actorField(object, "sub")
		if err != nil {
			return nil, err
		}
		chain = append(chain, principal.Actor{Issuer: issuer, Subject: subject})

		next, nested := object[ClaimActor]
		if !nested {
			return chain, nil
		}
		link = next
	}
}

// actorField reads one required string member of an act link.
func actorField(link map[string]any, name string) (string, error) {
	value, ok := link[name]
	if !ok {
		return "", refuseActChain(fmt.Sprintf("an act link has no %q", name))
	}
	text, ok := value.(string)
	switch {
	case !ok:
		return "", refuseActChain(fmt.Sprintf("an act link's %q is not a string", name))
	case text == "":
		return "", refuseActChain(fmt.Sprintf("an act link's %q is empty", name))
	case len(text) > MaxActorFieldBytes:
		return "", refuseActChain(fmt.Sprintf("an act link's %q is over %d bytes", name, MaxActorFieldBytes))
	}

	return text, nil
}

// Delegation is a trust policy entry's opt-in to inbound RFC 8693 delegation:
// the actors a token it admits may name in its `act` chain, and what each
// leaves the caller able to do.
//
//	delegation:
//	  max_depth: 2           # 1 (default) or 2
//	  actors:
//	    - issuer: https://agents.example.com
//	      subject: triage-bot
//	      actions: [run.read, run.start]
//
// # Narrowing only
//
// A delegated caller's actions are the **intersection** of what the entry
// grants the subject (itself narrowed by the token's own scopes) and, for
// every actor in the chain, that actor's `actions`. Never a union: an actor
// can take authority away from the subject it acts for and can never add any,
// so a delegated request is at most what the subject could do alone, and at
// most what each actor is allowed to do on anyone's behalf. An actor whose
// `actions` names something the entry does not grant is refused when the policy
// loads, because it would read as a grant and be none.
//
// # Exact strings, fail closed
//
// An actor matches by exact `issuer` and `subject` strings, with no wildcard,
// prefix or pattern, as [ClaimRule] does. A token whose chain names any actor
// not listed, is deeper than max_depth, or is malformed is refused; so is a
// token carrying an `act` claim on an entry with no stanza at all.
//
// The listed actors are not trusted issuers: Flowstate never authenticates
// them. The allowlist is the operator saying which parties the admitting
// issuer may truthfully say are acting, and the chain is exactly as reliable as
// that issuer's signature. See THREAT_MODEL.md.
type Delegation struct {
	// MaxDepth is the longest chain accepted: 1 (the default when omitted) for
	// a single actor, or 2 to also accept the actor that actor acts for. It
	// cannot exceed [MaxActorDepth].
	MaxDepth int `json:"max_depth,omitempty" yaml:"max_depth,omitempty"`

	// Actors is the allowlist, at least one. Every actor in a token's chain
	// must be listed.
	Actors []DelegationActor `json:"actors" yaml:"actors"`
}

// DelegationActor is one party a [Delegation] allows in an `act` chain.
type DelegationActor struct {
	// Issuer is the exact `iss` the chain names for this actor. It is not a
	// URL that is fetched or a trusted issuer: it is compared as a string.
	Issuer string `json:"issuer" yaml:"issuer"`

	// Subject is the exact `sub` the chain names for this actor.
	Subject string `json:"subject" yaml:"subject"`

	// Actions is the most this actor leaves a caller able to do while acting
	// for them, from the same canonical scope vocabulary as
	// [TrustedIssuer.Actions], and a subset of it. Required, with [] granting
	// none: an omission that meant "no limit" is how an actor ends up holding
	// more than anyone decided.
	Actions ActionScopes `json:"actions,omitzero" yaml:"actions,omitempty"`
}

// depth is the longest chain the stanza accepts.
func (d *Delegation) depth() int {
	if d == nil {
		return 0
	}
	if d.MaxDepth == 0 {
		return 1
	}

	return d.MaxDepth
}

// clone returns a copy sharing no slice with d; nil for nil.
func (d *Delegation) clone() *Delegation {
	if d == nil {
		return nil
	}
	clone := *d
	clone.Actors = slices.Clone(d.Actors)
	for i := range clone.Actors {
		clone.Actors[i].Actions = slices.Clone(d.Actors[i].Actions)
	}

	return &clone
}

// actor finds the allowlist row for an actor by exact issuer and subject.
func (d *Delegation) actor(a principal.Actor) (DelegationActor, bool) {
	if d == nil {
		return DelegationActor{}, false
	}
	for _, row := range d.Actors {
		if row.Issuer == a.Issuer && row.Subject == a.Subject {
			return row, true
		}
	}

	return DelegationActor{}, false
}

// admitsActors reports whether this entry accepts a token naming the given
// chain. An empty chain is always accepted: a token that delegates nothing
// needs no stanza. The refusals say what is wrong with the chain and never
// what the allowlist holds.
func (t TrustedIssuer) admitsActors(actors []principal.Actor) error {
	switch {
	case len(actors) == 0:
		return nil
	case t.Delegation == nil:
		return refuseActChain("this trust policy entry does not accept delegated tokens")
	case len(actors) > t.Delegation.depth():
		return refuseActChain("the chain is deeper than this trust policy entry accepts")
	}

	for _, actor := range actors {
		if _, listed := t.Delegation.actor(actor); !listed {
			return refuseActChain("an actor in the chain is not one this trust policy entry accepts")
		}
	}

	return nil
}

// delegatedActions narrows granted by what each actor in an admitted chain is
// allowed, and returns the intersection, in granted's order. It can only
// remove: no element of the result is absent from granted, whatever the chain
// lists. A nil granted stays nil, since nil grants nothing to a verified
// principal. It assumes [TrustedIssuer.admitsActors] accepted the chain, and an
// actor it cannot find narrows to nothing rather than being skipped.
func (t TrustedIssuer) delegatedActions(actors []principal.Actor, granted ActionScopes) ActionScopes {
	if len(actors) == 0 || granted == nil {
		return granted
	}

	narrowed := slices.Clone(granted)
	for _, actor := range actors {
		row, _ := t.Delegation.actor(actor)
		narrowed = slices.DeleteFunc(narrowed, func(action string) bool {
			return !slices.Contains(row.Actions, action)
		})
	}

	return narrowed
}

// validateDelegation checks the stanza when the policy loads.
func (t TrustedIssuer) validateDelegation() error {
	d := t.Delegation
	if d == nil {
		return nil
	}

	if d.MaxDepth < 0 || d.MaxDepth > MaxActorDepth {
		return fmt.Errorf("delegation.max_depth %d is not supported: use 1 (the default) or %d", d.MaxDepth, MaxActorDepth)
	}
	if len(d.Actors) == 0 {
		return fmt.Errorf("delegation.actors is required: name each actor a token may list, or remove the delegation stanza")
	}
	if len(d.Actors) > maxDelegationActors {
		return fmt.Errorf("delegation.actors has %d entries, over the %d entry limit", len(d.Actors), maxDelegationActors)
	}

	seen := make(map[principal.Actor]struct{}, len(d.Actors))
	for i, actor := range d.Actors {
		where := fmt.Sprintf("delegation.actors[%d]", i)
		switch {
		case actor.Issuer == "" || actor.Subject == "":
			return fmt.Errorf("%s needs both issuer and subject: an actor is matched by the exact pair", where)
		case len(actor.Issuer) > MaxActorFieldBytes || len(actor.Subject) > MaxActorFieldBytes:
			return fmt.Errorf("%s: issuer and subject are at most %d bytes", where, MaxActorFieldBytes)
		case strings.Contains(actor.Issuer, "#"):
			// Rendered `issuer#subject` wherever an actor is named, as a caller is.
			return fmt.Errorf("%s: issuer contains '#', which separates the issuer from the subject", where)
		case actor.Actions == nil:
			return fmt.Errorf("%s: actions is required; list what this actor leaves a caller able to do, or use [] to leave none", where)
		}
		key := principal.Actor{Issuer: actor.Issuer, Subject: actor.Subject}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%s: this issuer and subject are listed twice", where)
		}
		seen[key] = struct{}{}

		if err := validateActionScopes(where+".actions", actor.Actions); err != nil {
			return err
		}
		for _, action := range actor.Actions {
			if !slices.Contains(t.Actions, action) {
				return fmt.Errorf("%s.actions names %q, which this entry does not grant: an actor can only narrow the "+
					"entry's actions, never add one", where, action)
			}
		}
	}

	return nil
}
