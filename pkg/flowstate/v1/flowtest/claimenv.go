package flowtest

import (
	"slices"
	"sync"

	"github.com/google/cel-go/cel"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The one place `flow test` decides what vocabulary a `check:` claim is read in.
//
// Every parse of a claim used to call `Env()` with no libraries — the base
// environment — while the run evaluated the same claim with the libraries the
// workflow's profile names. So a claim written in the idiom STYLE.md teaches as
// canonical was refused at load:
//
//	expect.check[0]: ERROR: <input>:1:14: unsupported syntax '.?'
//
// while the identical expression in the workflow it tests validates and runs
// (#1512). A macro was worse than a refusal: `steps.l.value.sum()` *parses*
// against the base environment, because a receiver-style macro reads as an
// ordinary call, so the witness walkers saw a tree the run never evaluated and
// were right only by luck.
//
// Two environments, chosen by what is known where:
//
//   - Where the workflow is resolved, the claim is read in that spec's own
//     profile. That is the environment the run evaluates in, so parse and
//     evaluation cannot disagree.
//   - At load, no workflow is resolved and none can be: [LoadSource] takes
//     bytes with no path at all, which is what an editor, the MCP surface and
//     the fuzzer hand it. There the claim is read in every library any known
//     profile has, so a claim some run could accept is never refused before
//     anything has said which run — while a claim no profile could parse is
//     still refused at load, with its position, as it always was.

// claimEnv is the environment a claim is parsed in for a workflow whose profile
// resolved to these libraries.
//
// Named rather than inlined at the four call sites it replaces so that "which
// vocabulary is a claim read in" has one answer in this package. The libraries
// come from [v1.ProfileLibraries] over the spec's own `profile:`, never over
// this build's [v1.CurrentProfile]: reading the build's would give a spec
// compiled by an older one a vocabulary it was never checked against, which is
// the shape #1465 found in `must:`.
func claimEnv(ev *v1.Evaluator, libs []string) (*cel.Env, error) {
	return ev.Env(libs...)
}

// loadClaimEnv is the environment a claim is parsed in before any workflow is
// resolved.
//
// Everything any profile can say, so that load refuses only what no run could
// have accepted. It errs toward accepting: a claim whose vocabulary this spec's
// profile happens to lack is refused when the run reads it, in that profile,
// rather than here where the profile is unknown.
func loadClaimEnv(ev *v1.Evaluator) (*cel.Env, error) {
	return ev.Env(everyProfileLibrary()...)
}

// everyProfileLibrary is the union of every known profile's libraries, sorted
// and computed once.
//
// Derived from [v1.ProfileNames] and [v1.ProfileLibraries] rather than written
// out, because a second list would be a second answer to what a profile
// contains — and this one exists precisely to survive a profile being added.
var everyProfileLibrary = sync.OnceValue(func() []string {
	var union []string
	for _, name := range v1.ProfileNames() {
		libs, err := v1.ProfileLibraries(name)
		if err != nil {
			// A name ProfileNames just handed back, so this cannot happen; an
			// empty contribution rather than a panic if it ever does, since the
			// caller's job is to report a claim's syntax, not this.
			continue
		}
		for _, lib := range libs {
			if !slices.Contains(union, lib) {
				union = append(union, lib)
			}
		}
	}
	slices.Sort(union)

	return union
})
