package flowfile

import (
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// attachPaths sets [Diagnostic.Path] on every diagnostic that names a step whose
// id is declared exactly once in wf.
//
// The path comes from the workflow, not from the source, so a specification
// built by hand or received over the API names its steps the way one compiled
// from a file does. It is [v1.DebugOwnSites]' site path, the same chain the
// debugger and the source map address a step by, rather than a second
// spelling of it.
//
// A diagnostic keeps an empty Path when it is not on a step, when the id is
// declared in more than one scope (a bare id then names no one site), or when
// the walk stopped at [v1.MaxDebugStaticSites] and so cannot vouch that the id
// is unique. A `call:` step is a site of its own; diagnostics about the callee's
// steps are reported against the call step's id and take its path.
func attachPaths(ds Diagnostics, wf *v1.Workflow) {
	if !slices.ContainsFunc(ds, func(d Diagnostic) bool { return d.Step != "" && d.Path == nil }) {
		return
	}

	sites, truncated := v1.DebugOwnSites(wf)
	if truncated {
		return
	}
	paths := make(map[string][]string, len(sites))
	for _, site := range sites {
		path := site.Site.GetPath()
		if len(path) == 0 {
			continue
		}
		id := path[len(path)-1]
		if _, again := paths[id]; again {
			paths[id] = nil

			continue
		}
		paths[id] = path
	}

	for i := range ds {
		if ds[i].Step == "" || ds[i].Path != nil {
			continue
		}
		ds[i].Path = slices.Clone(paths[ds[i].Step])
	}
}
