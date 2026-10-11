package flowfile

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// An Env is the environment a Flowfile is compiled, validated, rewritten and
// written back out in: the one thing those entry points read that is not the
// document, which is the registry of tasks a step may name.
//
// The zero value is the build's own environment, whose registry is
// [v1.DefaultRegistry], and every package-level function in this package is the
// zero Env's method of the same name. The CLI keeps it. An embedder or editor
// that holds its own task set (a language server answering each document from
// that document's plugin lock, a host compiling on behalf of several tenants)
// builds an Env with [WithRegistry] and calls the methods instead, so a task
// registered for one caller is never visible to another and nothing has to be
// added to the process default, and removed again, around a compile.
//
// An Env is a small immutable value, safe to copy and to share between
// goroutines; the registry it holds is safe for concurrent use. Which registry
// an entry point resolves task names, input shapes, output names and enum
// values against is decided here and nowhere else: nothing below an Env method
// reads [v1.DefaultRegistry] for a non-default Env.
type Env struct {
	// registry is the task set to resolve against. Nil means the process
	// default, read at the moment of each lookup exactly as before Env existed.
	registry *v1.Registry
}

// WithRegistry returns the Env that resolves tasks against r. A nil r is the
// zero Env: the process default registry.
//
// r is read, never written. The caller owns it and may keep registering into it;
// a lookup sees what it holds at that moment, as a lookup in the default does.
func WithRegistry(r *v1.Registry) Env {
	return Env{registry: r}
}

// Registry returns the registry this Env resolves tasks against: the one given
// to [WithRegistry], or [v1.DefaultRegistry] for the zero Env.
func (e Env) Registry() *v1.Registry {
	if e.registry == nil {
		return v1.DefaultRegistry()
	}
	return e.registry
}

// tasks is the nil-means-default form the registry-taking helpers in [v1]
// accept.
func (e Env) tasks() *v1.Registry { return e.registry }

// lookup returns the definition of the named task in this Env's registry.
func (e Env) lookup(name string) (v1.TaskDef, bool) {
	return v1.LookupTaskFrom(e.registry, name)
}

// taskNames returns the sorted names of this Env's tasks.
func (e Env) taskNames() []string {
	return v1.TaskNamesFrom(e.registry)
}
