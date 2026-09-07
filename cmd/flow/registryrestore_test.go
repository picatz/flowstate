package main

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// restoreDefaultRegistryAfter puts [v1.DefaultRegistry] back the way this
// test found it when the test ends: a task registered meanwhile is
// unregistered, and one replaced (the `http` task under an `--egress-policy`)
// is restored to the definition captured here.
//
// The registry is process-wide and registration into it has no undo of its
// own, so a test that launches a plugin or applies an egress policy leaves
// every later test in the binary seeing its task or its policy. Under
// `-shuffle=on` that is an order-dependent failure in the tests that read the
// registry as this build shipped it (#1727); this is the undo, taken under
// [v1.LockDefaultRegistry] so the capture-then-restore window is whole
// against any other such sequence.
//
// A test that calls this must not be parallel: its cleanup would put the
// registry back while a sibling running beside it still relies on what it
// registered, which is the same coupling on a shorter fuse. The lock is held
// for the capture and for the restore, not across the test between them:
// the mutation in between is the code under test's own single Register or
// Replace, which takes the registry's own mutex and not this compound one,
// and a test holding the compound lock across its run would not deadlock
// that code but would serialise nothing it does not already serialise by
// being non-parallel. Two serial tests cannot interleave; that is the whole
// of what keeps the capture-then-restore window whole.
func restoreDefaultRegistryAfter(t *testing.T) {
	t.Helper()

	unlock := v1.LockDefaultRegistry()
	before := v1.DefaultRegistry().All()
	unlock()

	t.Cleanup(func() {
		unlock := v1.LockDefaultRegistry()
		defer unlock()

		registry := v1.DefaultRegistry()
		kept := make(map[string]struct{}, len(before))
		for _, def := range before {
			kept[def.Name] = struct{}{}
		}
		for _, name := range registry.Names() {
			if _, ok := kept[name]; !ok {
				registry.Unregister(name)
			}
		}
		for _, def := range before {
			if err := registry.Replace(def); err != nil {
				t.Errorf("restoring %q in the default registry: %v", def.Name, err)
			}
		}
	})
}

// unregisterTasksAddedAfter removes, when the test ends, every task the
// process-wide registry gained since this was called, and touches nothing
// that was already there.
//
// This is the form a test that may run in parallel can use: it never restores
// a definition, so it cannot put the permissive default `http` task back
// under a parallel sibling that installed the MCP posture's deny-everything
// one and is relying on it (#1727 review). Names are only ever added by a
// plugin launch, and the tests that launch one are serial, so the names this
// removes are the caller's own.
func unregisterTasksAddedAfter(t *testing.T) {
	t.Helper()

	unlock := v1.LockDefaultRegistry()
	before := v1.DefaultRegistry().Names()
	unlock()

	t.Cleanup(func() {
		unlock := v1.LockDefaultRegistry()
		defer unlock()

		registry := v1.DefaultRegistry()
		kept := make(map[string]struct{}, len(before))
		for _, name := range before {
			kept[name] = struct{}{}
		}
		for _, name := range registry.Names() {
			if _, ok := kept[name]; !ok {
				registry.Unregister(name)
			}
		}
	})
}
