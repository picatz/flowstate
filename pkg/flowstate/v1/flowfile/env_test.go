package flowfile_test

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// An Env resolves tasks against the registry it was given and nothing else.
//
// Every test here builds two registries that each define a task of one name with
// a different shape, and asks the same entry point of both. A registry the entry
// point should not have read is told apart from the one it should by the answer:
// the shapes are chosen so that what is valid under one is a diagnostic under
// the other and under the process default, which has neither.

// envFetch is a plugin task both registries define, differently.
//
// In registry A it takes the log task's inputs (`message`) and answers with the
// exec task's outputs (`exit_code`, `stdout`, ...). In registry B it takes the
// exec task's inputs (`argv`, `dir`) and answers with the http task's outputs
// (`status_code`, `body`, ...).
const envFetch = "acme.fetch"

func noopTask(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
	return &v1.Node_Outputs{}, nil
}

// envRegistry is a registry holding the built-in tasks and defs: a plugin host's
// view, without touching the process default.
func envRegistry(t testing.TB, defs ...v1.TaskDef) *v1.Registry {
	t.Helper()

	r := v1.NewRegistry()
	for _, def := range v1.DefaultRegistry().All() {
		if v1.IsBuiltinTask(def.Name) {
			require.NoError(t, r.Register(def))
		}
	}
	for _, def := range defs {
		def.Fn = noopTask
		require.NoError(t, r.Register(def))
	}

	return r
}

func fetchA(name string) v1.TaskDef {
	return v1.TaskDef{
		Name:    name,
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&v1.Task_Exec_Outputs{}).ProtoReflect().Descriptor(),
	}
}

func fetchB(name string) v1.TaskDef {
	return v1.TaskDef{
		Name:    name,
		Inputs:  (&v1.Task_Exec_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&v1.Task_HTTP_Outputs{}).ProtoReflect().Descriptor(),
	}
}

// sourceA is valid exactly where name is registered as [fetchA].
func sourceA(name string) string {
	return "edition: " + flowfile.CurrentEdition + `
name: probe
steps:
  - id: f
    ` + name + `:
      message: hi
  - id: use
    log:
      message: ${string(steps.f.exit_code)}
`
}

// sourceB is valid exactly where name is registered as [fetchB].
func sourceB(name string) string {
	return "edition: " + flowfile.CurrentEdition + `
name: probe
steps:
  - id: f
    ` + name + `:
      argv: [echo]
      dir: /tmp
  - id: use
    log:
      message: ${string(steps.f.status_code)}
`
}

func validateIn(t testing.TB, env flowfile.Env, src string) string {
	t.Helper()

	ds, err := env.ValidateSource([]byte(src))
	if err != nil {
		return err.Error()
	}

	return ds.Error()
}

func TestEnvValidatesAgainstItsOwnRegistry(t *testing.T) {
	t.Parallel()

	a := flowfile.WithRegistry(envRegistry(t, fetchA(envFetch)))
	b := flowfile.WithRegistry(envRegistry(t, fetchB(envFetch)))

	assert.Empty(t, validateIn(t, a, sourceA(envFetch)), "A reads its own definition")
	assert.Empty(t, validateIn(t, b, sourceB(envFetch)), "B reads its own definition")

	// The negative direction, and the point of the exercise: each registry's
	// definition is a diagnostic under the other's, so neither Env can be reading
	// the other's task.
	crossedA := validateIn(t, b, sourceA(envFetch))
	assert.Contains(t, crossedA, "message", "B does not take `message`")
	assert.Contains(t, crossedA, "exit_code", "B's task has no `exit_code` output")
	crossedB := validateIn(t, a, sourceB(envFetch))
	assert.Contains(t, crossedB, "argv", "A does not take `argv`")
	assert.Contains(t, crossedB, "status_code", "A's task has no `status_code` output")

	// The zero Env, and an Env built on a nil registry, are the process default,
	// which was never told about the task.
	for name, env := range map[string]flowfile.Env{"zero": {}, "nil registry": flowfile.WithRegistry(nil)} {
		got := validateIn(t, env, sourceA(envFetch))
		assert.Contains(t, got, `no plugin task "`+envFetch+`" is registered here`, name)
		assert.Equal(t, validateIn(t, flowfile.Env{}, sourceA(envFetch)), got, name)
	}
	assert.Equal(t, validateSourceTheOldWay(t, sourceA(envFetch)), validateIn(t, flowfile.Env{}, sourceA(envFetch)),
		"the package-level function is the zero Env's method")
}

func validateSourceTheOldWay(t testing.TB, src string) string {
	t.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		return err.Error()
	}

	return ds.Error()
}

func TestEnvEntryPointsAllReadTheirOwnRegistry(t *testing.T) {
	t.Parallel()

	a := flowfile.WithRegistry(envRegistry(t, fetchA(envFetch), v1.TaskDef{Name: "acme.alpha"}))
	b := flowfile.WithRegistry(envRegistry(t, fetchB(envFetch)))
	path := filepath.Join(t.TempDir(), "flow.yaml")

	t.Run("ParseAndValidateSourceAt", func(t *testing.T) {
		t.Parallel()

		_, _, ds, err := a.ParseAndValidateSourceAt([]byte(sourceA(envFetch)), path)
		require.NoError(t, err)
		assert.Empty(t, ds)
		_, _, ds, err = b.ParseAndValidateSourceAt([]byte(sourceA(envFetch)), path)
		require.NoError(t, err)
		assert.NotEmpty(t, ds)
	})

	t.Run("ValidateSourceAt and ValidateSourceFile", func(t *testing.T) {
		t.Parallel()

		ds, err := a.ValidateSourceAt([]byte(sourceA(envFetch)), path)
		require.NoError(t, err)
		assert.Empty(t, ds)
		ds, err = b.ValidateSourceAt([]byte(sourceA(envFetch)), path)
		require.NoError(t, err)
		assert.NotEmpty(t, ds)

		file := filepath.Join(t.TempDir(), "flow.yaml")
		write(t, filepath.Dir(file), "flow.yaml", sourceA(envFetch))
		ds, err = a.ValidateSourceFile(file)
		require.NoError(t, err)
		assert.Empty(t, ds)
		ds, err = b.ValidateSourceFile(file)
		require.NoError(t, err)
		assert.NotEmpty(t, ds)
		wf, ds, err := a.ParseAndValidateFile(file)
		require.NoError(t, err)
		assert.Empty(t, ds)
		assert.NotNil(t, wf)
		_, ds, err = b.ParseAndValidateFile(file)
		require.NoError(t, err)
		assert.NotEmpty(t, ds)
	})

	t.Run("Validate of a compiled workflow", func(t *testing.T) {
		t.Parallel()

		// Compiled under the default, where the task is merely unknown and the
		// compiler does not mind; judged under each Env.
		wf, err := flowfile.Unmarshal([]byte(sourceA(envFetch)))
		require.NoError(t, err)
		assert.Empty(t, a.Validate(wf))
		assert.NotEmpty(t, b.Validate(wf))
		assert.NotEmpty(t, flowfile.Validate(wf), "the default does not know the task")
	})

	t.Run("StepKinds and StepKindList", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, a.StepKinds(), "acme.alpha")
		assert.NotContains(t, b.StepKinds(), "acme.alpha", "B's registry never held it")
		assert.NotContains(t, flowfile.StepKinds(), "acme.alpha")
		assert.Contains(t, a.StepKindList(), "acme.alpha")
		assert.NotContains(t, flowfile.StepKindList(), "acme.alpha")
	})

	t.Run("Parse compiles a shaping task by what its registry declares", func(t *testing.T) {
		t.Parallel()

		shapes := envRegistry(t, v1.TaskDef{Name: "acme.shape", ShapesOutputs: true, Inputs: (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor()})
		plain := envRegistry(t, v1.TaskDef{Name: "acme.shape", Inputs: (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor()})
		src := "edition: " + flowfile.CurrentEdition + `
name: t
steps:
  - id: s
    acme.shape:
      url: https://example.com
      outputs:
        code: ${response.status_code}
`
		shaped := func(env flowfile.Env) *v1.Value {
			wf, _, err := env.Parse([]byte(src))
			require.NoError(t, err)

			return wf.GetSteps()[0].GetTask().GetInputs()[v1.ShapingInput]
		}
		assert.NotNil(t, shaped(flowfile.WithRegistry(shapes)).GetStructure(), "a shaping task keeps the names")
		assert.Nil(t, shaped(flowfile.WithRegistry(plain)).GetStructure(), "an ordinary input of that name does not")
		assert.Nil(t, shaped(flowfile.Env{}).GetStructure(), "the default has never heard of the task")

		wf, err := flowfile.WithRegistry(shapes).Unmarshal([]byte(src))
		require.NoError(t, err)
		assert.NotNil(t, wf.GetSteps()[0].GetTask().GetInputs()[v1.ShapingInput].GetStructure())
		wf, _, err = flowfile.WithRegistry(shapes).ParseAt([]byte(src), path)
		require.NoError(t, err)
		assert.NotNil(t, wf.GetSteps()[0].GetTask().GetInputs()[v1.ShapingInput].GetStructure())
	})

	t.Run("Fix promotes only for a task its registry says shapes", func(t *testing.T) {
		t.Parallel()

		shapes := flowfile.WithRegistry(envRegistry(t, v1.TaskDef{Name: "acme.shape", ShapesOutputs: true, Inputs: (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor()}))
		plain := flowfile.WithRegistry(envRegistry(t, v1.TaskDef{Name: "acme.shape", Inputs: (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor()}))
		src := "edition: " + flowfile.CurrentEdition + `
name: t
steps:
  - id: s
    acme.shape:
      url: https://example.com
      outputs: '${ {"id": response.json.id} }'
`
		fixed, err := shapes.Fix([]byte(src))
		require.NoError(t, err)
		assert.Contains(t, string(fixed.Source), "        id: ${response.json.id}\n", "the shaping registry promotes the map literal")

		for name, env := range map[string]flowfile.Env{"plain": plain, "default": {}} {
			left, err := env.Fix([]byte(src))
			require.NoError(t, err, name)
			assert.Equal(t, src, string(left.Source), "%s leaves the input alone", name)
		}
	})

	t.Run("Marshal and Format order inputs by the registry's descriptor", func(t *testing.T) {
		t.Parallel()

		described := flowfile.WithRegistry(envRegistry(t, v1.TaskDef{Name: "acme.get", Inputs: (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor()}))
		bare := flowfile.WithRegistry(envRegistry(t, v1.TaskDef{Name: "acme.get"}))
		src := "edition: " + flowfile.CurrentEdition + `
name: t
steps:
  - id: g
    acme.get:
      method: GET
      url: https://example.com
`
		wf, err := flowfile.Unmarshal([]byte(src))
		require.NoError(t, err)

		order := func(out []byte) []string {
			var got []string
			for line := range strings.SplitSeq(string(out), "\n") {
				if k, _, ok := strings.Cut(strings.TrimSpace(line), ":"); ok && (k == "url" || k == "method") {
					got = append(got, k)
				}
			}

			return got
		}

		out, err := described.Marshal(wf)
		require.NoError(t, err)
		assert.Equal(t, []string{"url", "method"}, order(out), "the descriptor lists url before method")
		for name, env := range map[string]flowfile.Env{"bare": bare, "default": {}} {
			out, err := env.Marshal(wf)
			require.NoError(t, err, name)
			assert.Equal(t, []string{"method", "url"}, order(out), "%s does not know the task, so its inputs are sorted", name)
		}

		formatted, err := described.Format([]byte(src), wf)
		require.NoError(t, err)
		assert.Equal(t, []string{"url", "method"}, order(formatted))
		formatted, err = bare.Format([]byte(src), wf)
		require.NoError(t, err)
		assert.Equal(t, []string{"method", "url"}, order(formatted))
	})

	t.Run("ResultEntryOf and IteratorType read the Env's outputs", func(t *testing.T) {
		t.Parallel()

		src := "edition: " + flowfile.CurrentEdition + `
name: t
steps:
  - id: loop
    for_each:
      items: ${["a", "b"]}
      steps:
        - id: f
          ` + envFetch + `:
            message: hi
  - id: after
    value: ${steps.loop.results}
`
		wf, err := flowfile.Unmarshal([]byte(src))
		require.NoError(t, err)

		names := func(env flowfile.Env) []string {
			entry, ok := env.ResultEntryOf(wf, "after", "steps.loop.results")
			if !ok {
				return nil
			}
			var out []string
			for _, step := range entry.Steps {
				for _, o := range step.Outputs {
					out = append(out, o.Name)
				}
			}

			return out
		}
		assert.Contains(t, names(a), "exit_code")
		assert.NotContains(t, names(a), "status_code")
		assert.Contains(t, names(b), "status_code")
		assert.NotContains(t, names(b), "exit_code")
		assert.Empty(t, names(flowfile.Env{}), "the default's step is open: it knows no outputs")

		typed, ok := a.IteratorType(wf, "f", "item")
		assert.True(t, ok)
		assert.Equal(t, "string", typed)
	})

	t.Run("a call's callee is judged by the caller's registry", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", sourceA(envFetch))
		caller := writeFile(t, dir, "caller.yaml", "edition: "+flowfile.CurrentEdition+`
name: caller
steps:
  - id: provision
    call: ./callee.yaml
`)

		ds, err := a.ValidateSourceFile(caller)
		require.NoError(t, err)
		assert.Empty(t, ds, "the callee's task is A's")
		ds, err = b.ValidateSourceFile(caller)
		require.NoError(t, err)
		assert.Contains(t, ds.Error(), "message", "B finds the callee's input unknown")
		ds, err = flowfile.ValidateSourceFile(caller)
		require.NoError(t, err)
		assert.Contains(t, ds.Error(), `no plugin task "`+envFetch+`"`, "the default has never heard of it")
	})

	t.Run("positions locate a plugin task's input by the compile's registry", func(t *testing.T) {
		t.Parallel()

		_, known, err := a.Parse([]byte(sourceA(envFetch)))
		require.NoError(t, err)
		span, ok := known.Locate("f", "message")
		require.True(t, ok)
		assert.Equal(t, 6, span.Start.Line, "the input sits under the task's key, which A's registry names")

		_, unknown, err := flowfile.Parse([]byte(sourceA(envFetch)))
		require.NoError(t, err)
		span, ok = unknown.Locate("f", "message")
		require.True(t, ok)
		assert.Equal(t, 4, span.Start.Line, "the default cannot name the key, so the diagnostic falls back to the step")
	})

	t.Run("StepTaskKeys", func(t *testing.T) {
		t.Parallel()

		// A registered name settles which key is the task, so a second unknown key
		// is not promoted beside it; under an Env that has never heard of the
		// name, it is the one promoted.
		keys := []string{"id", "mystery", "acme.alpha"}
		assert.Equal(t, []string{"acme.alpha"}, a.StepTaskKeys(keys))
		assert.Equal(t, []string{"mystery"}, b.StepTaskKeys(keys))
		assert.Equal(t, flowfile.StepTaskKeys(keys), flowfile.Env{}.StepTaskKeys(keys))
	})
}

// The process default is neither consulted for a task another registry holds,
// nor changed by compiling against one.
//
// Not parallel on purpose: it snapshots the default registry, and a sequential
// top-level test runs while the parallel ones are paused.
func TestEnvNeverReadsOrWritesTheProcessDefault(t *testing.T) {
	const clash = "acme.clash"

	before := v1.DefaultRegistry().Names()
	a := flowfile.WithRegistry(envRegistry(t, fetchA(clash)))
	b := flowfile.WithRegistry(envRegistry(t, fetchB(clash)))

	// The default holds a *third* definition of the same name, one under which
	// both sources below are wrong. An Env that read it, even as a fallback, would
	// report diagnostics where its own registry reports none.
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:    clash,
		Inputs:  (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&v1.Task_HTTP_Outputs{}).ProtoReflect().Descriptor(),
		Fn:      noopTask,
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(clash) })
	withClash := v1.DefaultRegistry().Names()

	assert.NotEmpty(t, validateSourceTheOldWay(t, sourceA(clash)), "the default really does hold the conflicting task")
	assert.NotEmpty(t, validateIn(t, flowfile.Env{}, sourceB(clash)))

	for range 3 {
		assert.Empty(t, validateIn(t, a, sourceA(clash)))
		assert.Empty(t, validateIn(t, b, sourceB(clash)))
		_, _, ds, err := a.ParseAndValidateSourceAt([]byte(sourceA(clash)), "")
		require.NoError(t, err)
		assert.Empty(t, ds)
		wf, err := a.Unmarshal([]byte(sourceA(clash)))
		require.NoError(t, err)
		_, err = a.Marshal(wf)
		require.NoError(t, err)
		_, err = a.Format([]byte(sourceA(clash)), wf)
		require.NoError(t, err)
		_, err = a.Fix([]byte(sourceA(clash)))
		require.NoError(t, err)
	}

	assert.Equal(t, withClash, v1.DefaultRegistry().Names(), "nothing was added to or removed from the default")
	def, ok := v1.DefaultRegistry().Lookup(clash)
	require.True(t, ok)
	assert.Equal(t, "url", def.Inputs.Fields().ByNumber(1).TextName(), "the default's own definition is untouched")
	v1.DefaultRegistry().Unregister(clash)
	assert.Equal(t, before, v1.DefaultRegistry().Names())
}

// One module cache serves Envs holding different registries without handing a
// module compiled against one to a compile against another.
//
// A module declares only types, functions and errors, so its diagnostics cannot
// currently differ by registry; what keeps a module cleared against one task set
// from being reused against another is the key, and the counters are how it shows.
func TestModuleCacheDoesNotShareEntriesAcrossRegistries(t *testing.T) {
	t.Parallel()

	dir := chainTree(t)
	path := filepath.Join(dir, "bill.yaml")
	cache := flowfile.NewModuleCache(0, 0)
	a := flowfile.WithRegistry(envRegistry(t, fetchA(envFetch)))
	b := flowfile.WithRegistry(envRegistry(t, fetchB(envFetch)))
	b2 := flowfile.WithRegistry(envRegistry(t, fetchB(envFetch)))

	check := func(name string, env flowfile.Env) flowfile.ModuleCacheStats {
		t.Helper()
		ds, err := env.ValidateSourceFileCached(cache, path)
		require.NoError(t, err, name)
		require.Empty(t, ds, name)

		return cache.Stats()
	}

	cold := check("A cold", a)
	require.Positive(t, cold.Compiles, "the first compile does the work")
	require.Zero(t, cold.Hits)

	// Another registry must compile the modules itself.
	crossB := check("B", b)
	assert.Zero(t, crossB.Hits, "B was handed a module compiled against A")
	assert.Equal(t, 2*cold.Compiles, crossB.Compiles)
	crossDefault := check("default", flowfile.Env{})
	assert.Zero(t, crossDefault.Hits, "the default was handed a module compiled against A or B")
	assert.Equal(t, 3*cold.Compiles, crossDefault.Compiles)
	// Two registries that merely look alike are still two registries.
	assert.Zero(t, check("B2", b2).Hits)

	// And an Env's own entries are still reused: the cache saves work within a
	// registry, which is the reason it exists.
	warm := check("A warm", a)
	assert.Positive(t, warm.Hits)
	assert.Equal(t, crossDefault.Compiles+cold.Compiles, warm.Compiles, "B2 compiled; A's second pass compiled nothing")

	// The nil cache stays the no-cache caller.
	ds, err := a.ValidateSourceFileCached(nil, path)
	require.NoError(t, err)
	assert.Empty(t, ds)
	assert.Equal(t, warm.Compiles, cache.Stats().Compiles)

	assert.NotEmpty(t, a.ModuleInterface(cache, filepath.Join(dir, "lib", "core.yaml")))
	assert.Equal(t, a.ModuleInterface(cache, filepath.Join(dir, "lib", "core.yaml")),
		cache.Interface(filepath.Join(dir, "lib", "core.yaml")), "an interface digest does not depend on the registry")
}

// Two Envs used from many goroutines at once each see only their own registry.
// The point is the race detector: nothing in a compile may share state between
// Envs, and a compile reads no process-wide table that another Env's registry
// could be mutating.
func TestEnvsAreIndependentUnderConcurrency(t *testing.T) {
	t.Parallel()

	a := flowfile.WithRegistry(envRegistry(t, fetchA(envFetch)))
	b := flowfile.WithRegistry(envRegistry(t, fetchB(envFetch)))

	var wg sync.WaitGroup
	failures := make(chan string, 64)
	for worker := range 8 {
		env, own, other := a, sourceA(envFetch), sourceB(envFetch)
		if worker%2 == 1 {
			env, own, other = b, sourceB(envFetch), sourceA(envFetch)
		}
		wg.Go(func() {
			for range 10 {
				if got := validateIn(t, env, own); got != "" {
					failures <- "own source reported: " + got
				}
				if got := validateIn(t, env, other); got == "" {
					failures <- "the other registry's source reported nothing"
				}
				wf, err := env.Unmarshal([]byte(own))
				if err != nil {
					failures <- err.Error()
					continue
				}
				if _, err := env.Marshal(wf); err != nil {
					failures <- err.Error()
				}
				if _, err := env.Fix([]byte(own)); err != nil {
					failures <- err.Error()
				}
				if _, err := env.Format([]byte(own), wf); err != nil {
					failures <- err.Error()
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	assert.Empty(t, slices.Collect(func(yield func(string) bool) {
		for f := range failures {
			if !yield(f) {
				return
			}
		}
	}))
}
