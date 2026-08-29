// Package fuzztargets reads targets.txt, the one written source of this
// module's fuzz target list.
//
// Four things used to hold their own copy of that list — the Makefile, the PR
// lane's fuzz-smoke job, the weekly deep tier's fuzz-deep job, and the package
// set tools/gate uses to decide whether a diff can reach a fuzz target — and
// they had already drifted apart: the deep tier ran four of the ten targets the
// smoke tier ran (#857). They all read this package or the file behind it now.
// The shell side reads it through list.sh; Go callers read it here, and the
// package's test holds the two readers to the same answer and the file itself
// to the tree.
package fuzztargets

import (
	_ "embed"
	"fmt"
	"strings"
)

// Tiers. A target names the tiers that run it in targets.txt's third column.
const (
	// TierSmoke is the bounded per-push tier: 30s per target, run by
	// `make fuzz-smoke` locally and by CI's required fuzz-smoke job.
	TierSmoke = "smoke"

	// TierDeep is the weekly scheduled tier: 10m per target, run by
	// deep.yml's fuzz-deep job.
	TierDeep = "deep"
)

//go:embed targets.txt
var targetsFile string

// A Target is one fuzz target and the package it lives in.
type Target struct {
	// Name is the target function's name, as `go test -fuzz` spells it.
	Name string

	// Dir is the package's directory relative to the module root, without
	// a leading "./" or a trailing slash.
	Dir string

	// Tiers are the tiers that run this target, in file order.
	Tiers []string
}

// InTier reports whether this target runs in the named tier.
func (t Target) InTier(tier string) bool {
	for _, have := range t.Tiers {
		if have == tier {
			return true
		}
	}
	return false
}

// ImportPath is the target's package as Go names it. modulePath is the module
// the directory is relative to; tools/gate passes its own constant rather than
// this package holding a second spelling of it.
func (t Target) ImportPath(modulePath string) string {
	return modulePath + "/" + t.Dir
}

// All returns every target in targets.txt, in file order.
//
// The file is embedded, so this cannot fail on a missing file at runtime, and a
// malformed line is a compile-time-adjacent failure: parse panics, because
// every caller is a build tool for which an unreadable target list is not a
// condition to handle but a broken checkout.
func All() []Target {
	targets, err := parse(targetsFile)
	if err != nil {
		panic("fuzztargets: " + err.Error())
	}
	return targets
}

// InTier returns every target the named tier runs, in file order.
func InTier(tier string) []Target {
	var out []Target
	for _, t := range All() {
		if t.InTier(tier) {
			out = append(out, t)
		}
	}
	return out
}

// Dirs returns the distinct package directories holding a target, in file
// order. tools/gate maps these to import paths to decide whether a diff can
// reach a fuzz target at all, which is why it is every tier's targets and not
// one tier's: a package holding a deep-only target is still a package whose
// change moves what a fuzzer explores.
func Dirs() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range All() {
		if !seen[t.Dir] {
			seen[t.Dir] = true
			out = append(out, t.Dir)
		}
	}
	return out
}

func parse(contents string) ([]Target, error) {
	var out []Target
	for i, line := range strings.Split(contents, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) != 3 {
			return nil, fmt.Errorf("targets.txt:%d: want 3 fields (target, directory, tiers), got %d: %q", i+1, len(fields), trimmed)
		}
		t := Target{Name: fields[0], Dir: fields[1], Tiers: strings.Split(fields[2], ",")}
		for _, tier := range t.Tiers {
			if tier != TierSmoke && tier != TierDeep {
				return nil, fmt.Errorf("targets.txt:%d: unknown tier %q", i+1, tier)
			}
		}
		out = append(out, t)
	}
	return out, nil
}
