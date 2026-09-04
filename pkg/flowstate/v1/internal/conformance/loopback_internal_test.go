package conformance

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTheLoopbackExemptionOutlastsItsFirstHolder is the counting rule stated as
// a test, because the defect it replaces was invisible in every test of one
// holder and only appeared with two.
//
// Not written as two real parallel tests racing: that reproduces the old bug
// about one run in four, which is a test that passes when it should fail three
// times out of four — and it fails as somebody *else's* egress denial, which is
// how the bug survived this long. What is asserted instead is the property that
// makes the race impossible: the exemption is installed while at least one
// holder has it, and restored exactly when the last one lets go.
func TestTheLoopbackExemptionOutlastsItsFirstHolder(t *testing.T) {
	// Deliberately not parallel. It swaps entries in the process-wide task
	// registry, which is the very thing it is about.

	if loopbackExemptionHeld() {
		t.Fatal("something already holds the exemption, so this test cannot observe it being taken")
	}

	registry := v1.DefaultRegistry()

	// A marked copy of whatever is registered, so "what was there before" is
	// identifiable by inspection. Comparing the definitions themselves would
	// not work: two calls to [v1.HTTPTaskDef] with different policies produce
	// closures over different state at the same code address.
	original, existed := registry.Lookup("http")
	if !existed {
		t.Fatal("no http task is registered, so there is no restoration to observe")
	}
	const mark = "sentinel · the loopback exemption test"
	sentinel := original
	sentinel.Summary = mark
	if err := registry.Replace(sentinel); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Replace(original) })

	registered := func() string {
		def, _ := registry.Lookup("http")

		return def.Summary
	}

	// Two holders whose lifetimes overlap, which is what a pair of parallel
	// tests is. The first finishes while the second still needs it, which is
	// precisely the ordering that used to restore the shipped default under a
	// running test.
	first, second := &deferredCleanup{TB: t}, &deferredCleanup{TB: t}

	allowLoopback(first)
	allowLoopback(second)

	if registered() == mark {
		t.Fatal("the exemption was taken and the registered http task did not change")
	}

	first.runCleanups()

	if !loopbackExemptionHeld() {
		t.Error("the first holder let go and the exemption was released, so a test still " +
			"using it now runs under the shipped deny-loopback default")
	}
	if registered() == mark {
		t.Error("the first holder's cleanup put the previous http task back while the second " +
			"holder was still running: that is the defect, and it surfaces as `denied by " +
			"egress policy: 127.0.0.1` in whichever test was unlucky")
	}

	second.runCleanups()

	if loopbackExemptionHeld() {
		t.Error("the last holder let go and the exemption is still held, so every later test in " +
			"this process runs with loopback permitted and the shipped default under test nowhere")
	}
	if registered() != mark {
		t.Error("the exemption was released and what came back is not what was registered before it was taken")
	}
}

// deferredCleanup runs a helper's [testing.TB.Cleanup] on demand rather than
// when the test ends, which is how two holders' lifetimes are made to overlap
// inside one test function.
type deferredCleanup struct {
	testing.TB

	cleanups []func()
}

func (d *deferredCleanup) Cleanup(fn func()) { d.cleanups = append(d.cleanups, fn) }

func (d *deferredCleanup) runCleanups() {
	for i := len(d.cleanups) - 1; i >= 0; i-- {
		d.cleanups[i]()
	}
	d.cleanups = nil
}
