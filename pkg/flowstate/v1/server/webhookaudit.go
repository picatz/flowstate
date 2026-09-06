package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// What the webhook receiver writes to the audit trail (#1774, #1797).
//
// The receiver is the one entry path that is unauthenticated by design: a
// sender proves itself with a signature, and what the receiver decides about
// that proof is an authorization decision in every sense docs/DEPLOYMENT.md's
// "Audit trail" gives the phrase. It used to write none of them. The bridge
// to a parked gate tried, under an RPC verb no authorization action binds, so
// a deployment with a recorder answered every bridged delivery 503 (#1797);
// the run-starting path and every refusal before verification wrote nothing.
//
// # The vocabulary
//
// A delivery is recorded as an enforcement decision — AuditEnforcementPoint
// WEBHOOK_DELIVERY — rather than under an AuthorizationAction, for the reason
// audit.proto's "The worker's half" gives: that vocabulary is the OAuth scope
// list a caller can be granted, and a sender holds no scope. A refusal decided
// before anything was proven names the route as its resource; an acceptance
// names the run the delivery started or answered, with the delivery id
// provenance records, so the trail reads from a run back to the delivery that
// started it.
//
// # Refusals are bounded
//
// One record per class of refusal per route per interval, carrying how many
// refusals it stands for. The receiver answers every pre-verification refusal
// with one status and one sentence precisely so that a prober learns nothing
// per attempt, and a trail that wrote a record per attempt would hand them the
// amplifier the response denies them: an unauthenticated POST becomes a
// durable write to every sink. So the first refusal of a class on a route is
// written as it happens, the rest are counted for [DefaultWebhookRefusalInterval],
// and the next refusal after the interval carries the count. A flood is then
// one record a minute per class per route, each saying how big it was, and
// nothing is lost but the timestamp of the individual attempts.
//
// Acceptances are not bounded: each is one run started or one gate answered,
// work the deployment was going to do anyway, by a sender who holds the key.

// DefaultWebhookRefusalInterval is how often one class of refusal on one route
// may write a record. A minute: long enough that a signature-guessing flood is
// a handful of records an hour, short enough that "refusals per route per
// hour" can still be read off the trail.
const DefaultWebhookRefusalInterval = time.Minute

// WithWebhookRefusalInterval sets how often one class of refusal on one route
// writes a record. Zero writes every refusal, which a test that counts them
// wants and a deployment does not. A negative value is ignored.
func WithWebhookRefusalInterval(d time.Duration) WebhookOption {
	return func(r *WebhookReceiver) {
		if d < 0 {
			return
		}
		r.refusals = newRefusalLedger(d)
	}
}

// refusalLedger is the bound on refusal records: one open window per
// (route, class), counting what it suppressed.
//
// Bounded by construction rather than by eviction: a route is one this
// receiver serves (or the one empty key for "no route"), and a class is one
// deny code, so the map can hold no more than routes × codes entries however
// many deliveries arrive.
type refusalLedger struct {
	mu       sync.Mutex
	interval time.Duration
	windows  map[refusalClass]*refusalWindow
}

type refusalClass struct {
	route string
	code  v1.AuditDenyCode
}

type refusalWindow struct {
	until      time.Time
	suppressed uint32
}

func newRefusalLedger(interval time.Duration) *refusalLedger {
	return &refusalLedger{interval: interval, windows: map[refusalClass]*refusalWindow{}}
}

// admit decides whether this refusal writes a record, and how many refusals
// that record stands for: this one, plus every one of the same class on the
// same route that the previous window swallowed.
func (l *refusalLedger) admit(now time.Time, class refusalClass) (count uint32, write bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	window := l.windows[class]
	if window != nil && now.Before(window.until) {
		// Saturating rather than wrapping: a count that came back around to
		// one would say a flood was quiet.
		if window.suppressed < ^uint32(0) {
			window.suppressed++
		}

		return 0, false
	}

	count = 1
	if window != nil {
		count += window.suppressed
		if count < window.suppressed {
			count = ^uint32(0)
		}
	}
	l.windows[class] = &refusalWindow{until: now.Add(l.interval)}

	return count, true
}

// webhookRouteKey is the resource key a delivery record carries for a route:
// the two names the file declares, never the path the sender wrote. Empty for
// a delivery that addressed no route, which is how the record says so without
// quoting a prober.
func webhookRouteKey(route *webhookRoute) string {
	if route == nil {
		return ""
	}

	return route.workflow.GetName() + "/" + route.trigger.GetName()
}

// principalIdentity is the identity a delivery to route acts as: the trigger,
// established by this deployment's configuration and never taken from the
// request, in the receiver's own tenant. It is the identity a run started by
// the delivery is attested with, and the one a record about a delivery that
// proved its key carries. See [WebhookReceiver.start] for why the tenant is
// the receiver's.
func (r *WebhookReceiver) principalIdentity(ctx context.Context, route *webhookRoute) *v1.WorkloadIdentity {
	return r.server.identityFor(auth.ContextWithPrincipal(ctx, auth.Principal{
		Issuer:    webhookIssuer,
		Subject:   v1.WebhookTriggerSubject(route.workflow.GetName(), route.trigger.GetName()),
		Namespace: r.namespace,
	}))
}

// recordRefusal writes the bounded deny record for one refused delivery, and
// reports a required recorder's failure to write it.
//
// The refusal stands either way — there is no version of "could not record
// the refusal" that admits the delivery — so most callers ignore the error
// and the sender's answer is unchanged. The bridge's callers return it, so
// that a sender whose delivery was refused by a policy is told to retry when
// the deployment could not write that down, exactly as [FlowstateServer.Signal]
// answers under a required recorder.
func (r *WebhookReceiver) recordRefusal(ctx context.Context, route *webhookRoute, subject v1.EnforcementSubject, code v1.AuditDenyCode) error {
	count, write := r.refusals.admit(r.now(), refusalClass{route: webhookRouteKey(route), code: code})
	if !write {
		return nil
	}

	subject.Point = v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_WEBHOOK_DELIVERY
	subject.Count = count
	if err := r.server.audit.EnforcementDeny(ctx, subject, code); err != nil {
		r.log.ErrorContext(ctx, "a refused delivery could not be recorded", "route", webhookRouteKey(route), "error", err)

		return err
	}

	return nil
}

// refusedAtRoute records a delivery refused before, or instead of, reaching a
// run: the route is what it addressed. identity is nil for a delivery that
// proved nothing, and the trigger's own for one that verified and then did
// not map.
func (r *WebhookReceiver) refusedAtRoute(ctx context.Context, route *webhookRoute, identity *v1.WorkloadIdentity, code v1.AuditDenyCode) {
	_ = r.recordRefusal(ctx, route, v1.EnforcementSubject{
		Identity:     identity,
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_WEBHOOK_ROUTE,
		ResourceKey:  webhookRouteKey(route),
	}, code)
}

// denied is the bridge's refusal: recorded against what the delivery addressed,
// and handed back as the refusal the sender is told — or, when a required
// recorder could not record it, as the retryable failure that is the
// deployment's rather than the sender's. The two get different statuses, which
// is why the caller cannot simply return the record's error.
func (r *WebhookReceiver) denied(ctx context.Context, route *webhookRoute, identity *v1.WorkloadIdentity, kind v1.AuditResourceKind, key string, code v1.AuditDenyCode, refusal error) error {
	if err := r.recordRefusal(ctx, route, v1.EnforcementSubject{
		Identity:     identity,
		ResourceKind: kind,
		ResourceKey:  key,
	}, code); err != nil {
		return fmt.Errorf("%w: %w", errDeliveryNotStarted, err)
	}

	return refusal
}

// admitted records that a verified, bound delivery may start or answer the run
// it names — before anything is started or delivered, which is the order every
// control-plane allow is written in. A required recorder that cannot record
// refuses the delivery rather than proceeding unrecorded, which the sender may
// retry.
//
// joined says the run already existed. For the bridge that is every delivery;
// for a run start it is a redelivery recognized *after* the start was
// attempted, which is the one acceptance that cannot be known before the
// mutation and is therefore the one recorded after it, as a second record
// beside the admission — the shape [FlowstateServer.Run] gives a request id
// answered with the run it already started.
func (r *WebhookReceiver) admitted(ctx context.Context, identity *v1.WorkloadIdentity, workflowID, deliveryID string, joined bool) error {
	if err := r.server.audit.EnforcementAllow(ctx, v1.EnforcementSubject{
		Point:        v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_WEBHOOK_DELIVERY,
		Identity:     identity,
		ResourceKind: v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN,
		ResourceKey:  workflowID,
		DeliveryID:   deliveryID,
		Joined:       joined,
	}); err != nil {
		return fmt.Errorf("%w: %w", errDeliveryNotStarted, err)
	}

	return nil
}

// webhookDenyCode classifies a verification refusal for the trail. Anything
// the verifier does not name a class for is a signature that did not verify,
// which is the class a wrong key lands in and the safe reading of an unnamed
// one.
func webhookDenyCode(err error) v1.AuditDenyCode {
	switch {
	case errors.Is(err, v1.ErrWebhookSignatureMissing):
		return v1.AuditDenyCode_AUDIT_DENY_CODE_SIGNATURE_MISSING
	case errors.Is(err, v1.ErrWebhookReplayWindow):
		return v1.AuditDenyCode_AUDIT_DENY_CODE_REPLAY_WINDOW
	case errors.Is(err, v1.ErrWebhookTooManySignatures):
		return v1.AuditDenyCode_AUDIT_DENY_CODE_TOO_MANY_SIGNATURES
	case errors.Is(err, v1.ErrWebhookKeyUnresolved):
		return v1.AuditDenyCode_AUDIT_DENY_CODE_NOT_CONFIGURED
	default:
		return v1.AuditDenyCode_AUDIT_DENY_CODE_SIGNATURE_INVALID
	}
}
