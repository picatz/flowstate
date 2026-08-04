package tests

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// AssertSignalSenderShape checks that a wait's `sender` output has the shape
// every driver must produce, and that it correctly identifies itself as either
// an attested, production-grade sender or an unattested local one.
//
// This is the one property both drivers are required to agree on for #194 —
// see docs/ARCHITECTURE.md's "Both execution drivers must agree" — and it is
// deliberately narrower than [WaitCases]'s shared [Case] table. A [Case] compares
// one fixed expected output against whatever a workflow produces, but a signal's
// sender is *supposed* to differ in content between the two drivers: the durable
// driver attests a real caller (established by `FlowstateServer.Signal`, or by a
// test simulating it), while the local driver has no authenticated caller at all
// and reports [v1.LocalSignalSender] instead. What must not differ is the shape —
// every driver's gate output has an `identity` sub-map with the same four fields,
// an `accepted_at`, and a `local` flag — and that `local` never lies: true only
// for a delivery nothing authenticated, false only for one a driver attests.
//
// Called from both driver's own test files (engine/wait_test.go durably,
// wait_local_test.go locally) rather than folded into [WaitCases], because
// building the delivery itself is driver-specific in exactly the way [WaitCases]'s
// own comment says a shared [Case] cannot express yet: durably a signal travels
// as a [v1.SignalDelivery] over a Temporal channel, locally as whatever
// [v1.LocalSignals.Deliver] wraps automatically. What can be shared, and is, is
// the assertion made about whatever each driver produced.
func AssertSignalSenderShape(t testing.TB, outputs *v1.Node_Outputs, wantLocal bool) {
	t.Helper()

	sender := outputs.GetNamedValues()[v1.SenderOutput].GetLiteral().GetMapValue()
	if sender == nil {
		t.Fatalf("the wait produced no %q mapping", v1.SenderOutput)
	}

	var (
		haveLocal       bool
		sawLocal        bool
		identity        map[string]string
		sawAcceptedAt   bool
		acceptedAtBlank bool
	)

	for _, entry := range sender.GetEntries() {
		switch entry.GetKey().GetStringValue() {
		case "local":
			sawLocal = true
			haveLocal = entry.GetValue().GetBoolValue()
		case "accepted_at":
			sawAcceptedAt = true
			acceptedAtBlank = entry.GetValue().GetStringValue() == ""
		case "identity":
			identity = map[string]string{}
			for _, field := range entry.GetValue().GetMapValue().GetEntries() {
				identity[field.GetKey().GetStringValue()] = field.GetValue().GetStringValue()
			}
		}
	}

	if !sawLocal {
		t.Fatalf("the sender mapping has no %q field", "local")
	}
	if !sawAcceptedAt {
		t.Fatalf("the sender mapping has no %q field", "accepted_at")
	}
	if identity == nil {
		t.Fatalf("the sender mapping has no %q field", "identity")
	}

	for _, field := range []string{"subject", "issuer", "namespace", "deployment"} {
		if _, ok := identity[field]; !ok {
			t.Fatalf("the sender's identity mapping has no %q field", field)
		}
	}

	if haveLocal != wantLocal {
		t.Fatalf("sender.local = %v, want %v — a local run must never look like an "+
			"attested production one, and an attested one must never be reported as local",
			haveLocal, wantLocal)
	}

	if wantLocal {
		// A local delivery has no authenticated caller at all, so nothing here
		// should look attested — an empty subject is what [v1.LocalSignalSender]
		// produces, and anything else would mean a local run started claiming a
		// production identity.
		if identity["subject"] != "" {
			t.Fatalf("a local sender carries an identity (%q), which nothing authenticated to produce",
				identity["subject"])
		}
	} else {
		// An attested sender is expected to actually say who, or the test that
		// built the delivery did not attest anything and this assertion would
		// pass for the wrong reason.
		if identity["subject"] == "" {
			t.Fatalf("an attested sender carries no identity.subject")
		}
		if acceptedAtBlank {
			t.Fatalf("an attested sender carries no accepted_at")
		}
	}
}
