package conformance

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The one question a policed gate asks, asked of both drivers: may this sender
// deliver this signal to this run?
//
// A local run exists to tell an author what production will do, and until #349
// it could not tell them the most important thing an approval gate does. Every
// `flow run local --signal` delivery attested nobody, no `allow:` rule a real
// deployment writes matches nobody, and so the only outcome a rehearsal could
// reach was the refusal. `--signal-as-subject` names the approver a delivery
// stands in for; these cases are what pins the two drivers to the same verdict
// about that name.
//
// # Where each driver enforces this
//
// Not in the same package, which is why a shared [Case] table cannot express
// it. Locally the enforcement point is [v1.LocalSignals.DeliverFrom], which
// checks a delivery before it is ever queued for a waiting step. Durably it is
// `FlowstateServer.authorizeSignal`, which checks the caller it just attested
// before Temporal ever sees the signal - the workflow side never decides this
// at all, deliberately, because a check the workflow performs is a check the
// workflow can skip. Both reach [v1.SignalPolicyCheck], and these cases are how
// "both reach it, and both answer with it" stays true rather than being a claim
// about today's call graph.
//
// # The sender is an identity here, not a [v1.SignalSender]
//
// The two drivers wrap it differently on purpose, and the difference is the
// feature: durably a sender is what the server attested, locally it is what the
// command asserted ([v1.RehearsalSignalSender], marked local). A case names the
// identity, and each driver builds the sender its own way - which is also what
// makes [AssertRehearsalSenderIsNeverAuthorizedDurably] meaningful, since the
// local wrapping must never be accepted by the durable enforcement point.

// RehearsalSignalCase is one sender, one policy, and the verdict both drivers
// must reach about the pair.
type RehearsalSignalCase struct {
	// Name says what the case is about, and becomes the subtest name.
	Name string

	// SignalName is the name delivered, and the key Policy is declared under.
	SignalName string

	// Policy is the resolved `signals:` entry governing SignalName - resolved,
	// as in past [v1.ResolveSignalPolicySubjects]: an enforcement path never
	// evaluates an expression, on either driver.
	Policy *v1.SignalPolicy

	// Starter is who started the run, which is what
	// `distinct_from_starter:` compares a sender against. Both drivers
	// know one: durably from the run's memo, locally from `--as-subject`
	// and its siblings.
	Starter *v1.WorkloadIdentity

	// Sender is who delivers, nil for a delivery that stands in for nobody -
	// locally the plain [v1.LocalSignalSender], durably an authenticated
	// caller a deployment configured no identity provider for. The two are
	// the same fact from a policy's point of view, which is why one case
	// covers both: nothing to match a rule against.
	Sender *v1.WorkloadIdentity

	// Admitted is whether the delivery reaches the waiting step.
	Admitted bool

	// Why says what the case is pinning, and is what a failure reports -
	// "refused" alone does not tell the next reader which rule they broke.
	Why string
}

// approver is the identity examples/approval-gate's own `signals:` rule
// admits, spelled here the way that file spells it so a case failing here
// and that example failing in CI are recognisably the same fact.
func approver() *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{
		Subject: "sre-lead@example.com",
		Issuer:  "https://issuer.example.com",
		Claims:  map[string]string{"team": "release-managers"},
	}
}

// webhookTrigger is the identity a delivery to one webhook attests as, built
// through [v1.WebhookTriggerPrincipal] rather than spelled out — the receiver
// mints it with that function and the validator composes the same value to
// decide whether a gate could admit it, so a case writing the string by hand
// would stop testing the thing that has to agree.
//
// The namespace is empty, which is a single-tenant deployment's own tenant and
// the value every rule in this table is silent about.
func webhookTrigger(workflow, trigger string) *v1.WorkloadIdentity {
	return v1.WebhookTriggerPrincipal("", workflow, trigger)
}

// bridgedGate is the policy `examples/webhook-approval-bridge` declares: one
// rule naming one webhook, which is the whole of what closes the signal zero
// case on a public route.
//
// No `distinct_from_starter:`, and its absence is the record's own line rather
// than an omission: an HMAC scheme attests a key holder, so that clause would
// separate triggers rather than the two humans it reads as promising.
func bridgedGate() *v1.SignalPolicy {
	return &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
		Subject: v1.QualifiedSubject(v1.WebhookPrincipalIssuer,
			v1.WebhookTriggerSubject("webhook-approval-bridge", "slack-approval")),
	}}}
}

// policedGate is the policy that example declares: one rule, a subject and a
// claim ANDed, plus the separation of duties a rule alone cannot express.
func policedGate(distinctFromStarter bool) *v1.SignalPolicy {
	return &v1.SignalPolicy{
		Allow: []*v1.SignalPolicyRule{{
			Subject: v1.QualifiedSubject("https://issuer.example.com", "sre-lead@example.com"),
			Claims:  map[string]string{"team": "release-managers"},
		}},
		DistinctFromStarter: distinctFromStarter,
	}
}

// RehearsalSignalCases is the shared table.
//
// Every case names an approver a real deployment could genuinely attest, so
// that "admitted" means the same thing on both drivers: locally the rehearsal
// stands in for exactly the caller production would have authenticated, and
// durably the caller is that person.
func RehearsalSignalCases() []RehearsalSignalCase {
	starter := &v1.WorkloadIdentity{
		Subject: "release-bot@example.com",
		Issuer:  "https://issuer.example.com",
	}

	return []RehearsalSignalCase{
		{
			Name:       "the approver the rule names",
			SignalName: "deploy-approved",
			Policy:     policedGate(false),
			Starter:    starter,
			Sender:     approver(),
			Admitted:   true,
			Why: "a rule's own subject and claim, satisfied exactly; a driver that refuses this " +
				"cannot rehearse an approval at all, only its refusal",
		},
		{
			Name:       "an approver with the team claim but another subject",
			SignalName: "deploy-approved",
			Policy:     policedGate(false),
			Starter:    starter,
			Sender: &v1.WorkloadIdentity{
				Subject: "someone-else@example.com",
				Issuer:  "https://issuer.example.com",
				Claims:  map[string]string{"team": "release-managers"},
			},
			Why: "the fields of one rule are ANDed, so half of it is not a match; this is the " +
				"case that would make a gate open for the whole team",
		},
		{
			Name:       "the named subject from another issuer",
			SignalName: "deploy-approved",
			Policy:     policedGate(false),
			Starter:    starter,
			Sender: &v1.WorkloadIdentity{
				Subject: "sre-lead@example.com",
				Issuer:  "https://other-idp.example.com",
				Claims:  map[string]string{"team": "release-managers"},
			},
			Why: "a subject is only unique within its issuer, and a rule matches the two joined; " +
				"a second identity provider minting the same local part is not the same person",
		},
		{
			Name:       "a delivery standing in for nobody",
			SignalName: "deploy-approved",
			Policy:     policedGate(false),
			Starter:    starter,
			Why: "nothing attested, and nothing asserted either; the rule names somebody, and " +
				"this is the pre-#349 shape every local delivery used to have",
		},
		{
			Name:       "the approver, who is also the run's own starter",
			SignalName: "deploy-approved",
			Policy:     policedGate(true),
			Starter: &v1.WorkloadIdentity{
				Subject: "sre-lead@example.com",
				Issuer:  "https://issuer.example.com",
			},
			Sender: approver(),
			Why: "distinct_from_starter: is ANDed onto whichever rule matched, so satisfying the " +
				"rule is not enough; an approver may not approve their own request on either driver",
		},
		{
			Name:       "the approver, distinct from the run's own starter",
			SignalName: "deploy-approved",
			Policy:     policedGate(true),
			Starter:    starter,
			Sender:     approver(),
			Admitted:   true,
			Why: "the same policy the case above refuses, with the one fact that decides it " +
				"changed; without this the case above would pass for a driver that refuses everything",
		},
		{
			Name:       "a sender in the namespace a rule names",
			SignalName: "release-approved",
			Policy: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Namespace: "release-managers"},
			}},
			Starter:  starter,
			Sender:   &v1.WorkloadIdentity{Subject: "anyone@example.com", Namespace: "release-managers"},
			Admitted: true,
			Why:      "a rule constrains only the fields it sets, so a namespace rule matches on namespace alone",
		},
		{
			Name:       "the webhook trigger a bridged gate's rule names",
			SignalName: "stage-approved",
			Policy:     bridgedGate(),
			Starter:    starter,
			Sender:     webhookTrigger("webhook-approval-bridge", "slack-approval"),
			Admitted:   true,
			Why: "a `signal:` on a webhook trigger answers a gate as the trigger itself, and the " +
				"rule names that principal; a driver that refuses it cannot bridge a delivery at " +
				"all, only refuse one",
		},
		{
			Name:       "another trigger on the same workflow",
			SignalName: "stage-approved",
			Policy:     bridgedGate(),
			Starter:    starter,
			Sender:     webhookTrigger("webhook-approval-bridge", "pagerduty-ack"),
			Why: "a webhook principal is qualified by *which* webhook, so one deployment's second " +
				"integration does not inherit the first's gate; this is the case a subject of " +
				"`flowstate://webhook` alone would wrongly admit",
		},
		{
			Name:       "the same trigger name on another workflow",
			SignalName: "stage-approved",
			Policy:     bridgedGate(),
			Starter:    starter,
			Sender:     webhookTrigger("some-other-workflow", "slack-approval"),
			Why: "the workflow is half of the subject, and a delivery is addressed " +
				"`/webhooks/<workflow>/<trigger>`; two files whose triggers are both called " +
				"`slack-approval` are two principals",
		},
		{
			Name:       "a person carrying the webhook subject as their own",
			SignalName: "stage-approved",
			Policy:     bridgedGate(),
			Starter:    starter,
			Sender: &v1.WorkloadIdentity{
				Issuer:  "https://issuer.example.com",
				Subject: "webhook-approval-bridge/slack-approval",
			},
			Why: "an issuer is half of every rule, and `flowstate://webhook` is a scheme no " +
				"identity provider can mint; a caller whose IdP hands out the trigger's subject " +
				"still is not the trigger",
		},
		{
			Name:       "a sender in another namespace",
			SignalName: "release-approved",
			Policy: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
				{Namespace: "release-managers"},
			}},
			Starter: starter,
			Sender:  &v1.WorkloadIdentity{Subject: "anyone@example.com", Namespace: "team-a"},
			Why:     "the negative direction of the case above, which is the one a tenant boundary is made of",
		},
	}
}

// AssertRehearsalSignalCases runs every case through admit - one driver's own
// enforcement point, which returns nil exactly when the delivery reaches the
// waiting step.
//
// Called by both drivers: locally from wait_local_rehearsal_test.go through
// [v1.LocalSignals.DeliverFrom], durably from the server package through
// `authorizeSignal`. A case added here is answered by both or by neither.
func AssertRehearsalSignalCases(t testing.TB, admit func(t testing.TB, c RehearsalSignalCase) error) {
	t.Helper()

	for _, c := range RehearsalSignalCases() {
		err := admit(t, c)

		switch {
		case c.Admitted && err != nil:
			t.Errorf("%s: the delivery was refused, and this driver must admit it: %v\n  %s",
				c.Name, err, c.Why)
		case !c.Admitted && err == nil:
			t.Errorf("%s: the delivery was admitted, and this driver must refuse it\n  %s",
				c.Name, c.Why)
		}
	}
}

// AssertRehearsalSenderIsNeverAuthorizedDurably is the negative direction the
// shared table above cannot state, because it is true of one driver only: a
// sender marked as a local rehearsal ([v1.RehearsalSignalSender]) is refused by
// the durable enforcement point, whatever identity it carries and whatever the
// policy says about that identity.
//
// Every case is offered, including the ones the table admits and the ones
// carrying no identity at all - an admitted case is the one worth having,
// because it proves the refusal is about the marker rather than about the
// identity failing a rule on its own merits.
//
// The marker is structural: `local` set beside a populated `identity` is a
// shape the durable path has no constructor for and no wire field a caller
// could set. This pins the *refusal* anyway, so that stays a rule the durable
// driver enforces rather than an accident of which constructors exist today -
// the same reason [v1.LocalSignalSender] was made a distinct value in the first
// place instead of an empty [v1.SignalSender] that would merely read as one.
func AssertRehearsalSenderIsNeverAuthorizedDurably(t testing.TB, admit func(t testing.TB, c RehearsalSignalCase, sender *v1.SignalSender) error) {
	t.Helper()

	for _, c := range RehearsalSignalCases() {
		for _, sender := range []*v1.SignalSender{
			v1.RehearsalSignalSender(c.Sender),
			v1.LocalSignalSender(),
		} {
			if err := admit(t, c, sender); err == nil {
				t.Errorf("%s: the durable driver authorized a sender marked as a local rehearsal "+
					"(local=%v, identity=%v); a rehearsal identity stands in for an approver on a "+
					"local run and must never authorize a durable one",
					c.Name, sender.GetLocal(), sender.GetIdentity() != nil)
			}
		}
	}
}
