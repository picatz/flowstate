package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const navigationSource = `edition: v2026.3
name: navigation
steps:
  - id: web
    http:
      url: https://example.com
  - id: status
    log:
      message: ${string(steps.web.status_code)}
  - id: shout
    shell:
      message: hi
`

// TestDefinition checks that a reference jumps to the step that declares it.
func TestDefinition(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///navigation.yaml"
	c.open(uri, navigationSource)

	t.Run("reference resolves to the step id", func(t *testing.T) {
		pos := positionOf(t, navigationSource, "web.status_code", 1)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, lsp.DocumentURI(uri), got[0].URI)
		assert.Equal(t, "web", textInRange(navigationSource, got[0].Range))
		assert.Equal(t, 3, got[0].Range.Start.Line, "should point at the id declaration")
	})

	t.Run("the output name also resolves to the step", func(t *testing.T) {
		pos := positionOf(t, navigationSource, "status_code)", 2)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, "web", textInRange(navigationSource, got[0].Range))
	})

	t.Run("the root segment also resolves to the step", func(t *testing.T) {
		// The root is part of the reference rather than a token of its own, so
		// landing on it navigates to the step the reference names — the same
		// answer as from either segment after it.
		pos := positionOf(t, navigationSource, "steps.web.status_code", 1)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, "web", textInRange(navigationSource, got[0].Range))
	})

	t.Run("nothing to go to outside an expression", func(t *testing.T) {
		pos := positionOf(t, navigationSource, "https://example.com", 2)
		assert.Empty(t, c.definition(uri, pos.Line, pos.Character))
	})

	t.Run("a forward reference does not resolve", func(t *testing.T) {
		const src = `name: fwd
steps:
  - id: a
    log:
      message: ${steps.later.status_code}
  - id: later
    http:
      url: https://example.com
`
		c.open("file:///fwd.yaml", src)
		pos := positionOf(t, src, "${steps.later.status_code}", len("${steps."))
		assert.Empty(t, c.definition("file:///fwd.yaml", pos.Line, pos.Character),
			"jumping to a step that has not run would suggest the reference works")
	})

	t.Run("the retired spelling does not resolve", func(t *testing.T) {
		// A bare `later.result` names no step in this grammar, so there is
		// nowhere to jump to. Following it to the step anyway would tell an
		// author the spelling still works while `flow validate` tells them to run
		// `flow fix`.
		const src = `name: bare
steps:
  - id: earlier
    http:
      url: https://example.com
  - id: b
    log:
      message: ${earlier.status_code}
edition: v2026.3
`
		params := c.open("file:///bare-nav.yaml", src)
		require.Len(t, params.Diagnostics, 1, "premise: the compiler refuses the bare spelling")

		pos := positionOf(t, src, "${earlier.status_code}", 3)
		assert.Empty(t, c.definition("file:///bare-nav.yaml", pos.Line, pos.Character))
	})
}

// TestDocumentSymbols checks the outline an editor shows.
func TestDocumentSymbols(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///symbols.yaml"
	c.open(uri, navigationSource)

	got := c.symbols(uri)
	require.Len(t, got, 3)

	// The step's key is the task, so the outline's second column names the task
	// with nothing in the document having to spell it out.
	assert.Equal(t, "web", got[0].Name)
	assert.Equal(t, "http", got[0].ContainerName)
	assert.Equal(t, lsp.SKFunction, got[0].Kind)
	assert.Equal(t, lsp.DocumentURI(uri), got[0].Location.URI)

	assert.Equal(t, "status", got[1].Name)
	assert.Equal(t, "log", got[1].ContainerName)

	// An unregistered task is labelled as such rather than presented as real.
	assert.Equal(t, "shout", got[2].Name)
	assert.Equal(t, "shell (unknown task)", got[2].ContainerName)

	// Each symbol's range must cover its whole step and nothing beyond it, or an
	// editor's breadcrumb flickers as the cursor moves through the file.
	//
	// assignStepRanges ends a step on the line before the next step's dash, and
	// walks the last step's end back over trailing blank lines. The dashes in
	// navigationSource are on lines 3, 6 and 9, so the steps own 3-5, 6-8 and
	// 9-11 — the whole file below `steps:`, partitioned with no line left over.
	//
	// One more than they read before the `edition:` marker was required, which is
	// also why that marker is written *first* in this fixture where the rest of the
	// package appends it: a top-level key written after the steps would extend the
	// last step's range past its own content.
	assert.Equal(t, 3, got[0].Location.Range.Start.Line)
	assert.Equal(t, 5, got[0].Location.Range.End.Line)
	assert.Equal(t, 6, got[1].Location.Range.Start.Line)
	assert.Equal(t, 8, got[1].Location.Range.End.Line)
	assert.Equal(t, 9, got[2].Location.Range.Start.Line)
	// The last step is the one with no successor to bound it, so it is the only
	// one that can run off the end of the file. It must stop at its last content
	// line rather than at the empty line the trailing newline leaves behind.
	assert.Equal(t, 11, got[2].Location.Range.End.Line)
}

func TestDocumentSymbolsAcceptQuotedAndEscapedKeysLikeTheLoader(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	c.open("file:///quoted-symbols.yaml", `"edition": v2026.3
"name": quoted
"st\u0065ps":
  - "i\u0064": first
    "l\u006fg":
      "message": hi
`)

	got := c.symbols("file:///quoted-symbols.yaml")
	require.Len(t, got, 1)
	assert.Equal(t, "first", got[0].Name)
	assert.Equal(t, "log", got[0].ContainerName)
}

// TestDocumentSymbolsNamesAnUnnamedStep checks that a step with no id still appears
// in the outline, since an empty row is worse than a placeholder.
//
// The step was written `task:`/`name:`/`inputs:` before verb-key flattening. The
// question it asked — what an editor shows for a step whose id is missing — is
// unchanged, but the new grammar states it more sharply: the task key is now the
// step's only key, so there is nothing else the outline could have fallen back to.
// The container is asserted alongside the name for that reason, because a
// placeholder name on its own would look identical if the step had not been
// recognized as a step at all.
func TestDocumentSymbolsNamesAnUnnamedStep(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	c.open("file:///noid.yaml", `name: noid
steps:
  - log:
      message: hi
`)
	got := c.symbols("file:///noid.yaml")
	require.Len(t, got, 1)
	assert.Equal(t, "(step with no id)", got[0].Name)
	assert.Equal(t, "log", got[0].ContainerName)
}

// TestDocumentSymbolsLeaveTheDescriptionOut pins the choice symbols.go argues for,
// because a step's prose is the obvious thing to put in an outline row.
//
// A SymbolInformation has two fields a reader sees and no third to grow into, and
// both are spent on facts with nowhere else to appear: the id a reference in
// another step spells, and what kind of work the step does plus which block it is
// inside. Prose is unbounded and author-written, so spending either on it costs a
// fact to repeat something hover already says with room to say it.
func TestDocumentSymbolsLeaveTheDescriptionOut(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///described-outline.yaml"
	c.open(uri, `name: outline
steps:
  - id: loop
    description: Fan out over everyone on the rota.
    for_each:
      items: "${['a']}"
      steps:
        - id: body
          description: Greet one person.
          log:
            message: hi
`)

	got := c.symbols(uri)
	require.Len(t, got, 2)

	assert.Equal(t, "loop", got[0].Name)
	assert.Equal(t, "for_each", got[0].ContainerName)
	assert.Equal(t, "body", got[1].Name)
	// The nesting is what prose would have displaced, and it is the only place a
	// flat outline can say this step runs inside the loop.
	assert.Equal(t, "log in loop", got[1].ContainerName)

	for _, s := range got {
		for _, word := range []string{"rota", "Greet"} {
			assert.NotContains(t, s.Name, word)
			assert.NotContains(t, s.ContainerName, word)
		}
	}
}

// TestSymbolsAndDefinitionOnUnparseableDocument checks that both features return an
// empty result rather than stale or invented positions.
func TestSymbolsAndDefinitionOnUnparseableDocument(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	// The tab in the last line's indentation is what YAML refuses; the step's key
	// is incidental, and is spelled the way a step spells one so that the fixture
	// stops being valid Flowfile for exactly one reason. Character 5 of that line
	// lands inside the key either way.
	c.open("file:///broken.yaml", "name: x\nsteps:\n  - id: a\n  \tlog: y\n")

	assert.Empty(t, c.symbols("file:///broken.yaml"))
	assert.Empty(t, c.definition("file:///broken.yaml", 3, 5))
}

// callerSource is a one-step Flowfile whose step calls target.
func callerSource(target string) string {
	return `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ` + target + `
    with:
      tenant: acme
`
}

// calleeSource is a Flowfile that takes the argument callerSource binds, so that
// a call to it is a *valid* one and nothing under test is deciding on the
// strength of a diagnostic somewhere else.
func calleeSource(name string) string {
	return `edition: v2026.3
name: ` + name + `
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: announce
    log:
      message: hello
`
}

// callTargetPosition is a position inside a call's target value, wherever the
// fixture put it.
func callTargetPosition(t *testing.T, src, target string) lsp.Position {
	t.Helper()
	return positionOf(t, src, "call: "+target, len("call: ")+1)
}

// TestDefinitionFollowsACall checks the one definition this language has that
// lives in another file.
//
// A `call:` is what makes a set of Flowfiles a graph rather than a pile, and the
// whole risk in following one is that the editor resolves the path by a different
// rule than the compiler — so these assert on the URI that comes back, not merely
// that something did.
func TestDefinitionFollowsACall(t *testing.T) {
	t.Parallel()

	t.Run("resolves to the file the call names", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		callee := filepath.Join(dir, "callee.yaml")
		require.NoError(t, os.WriteFile(callee, []byte(calleeSource("provision-tenant")), 0o644))

		src := callerSource("./callee.yaml")
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		params := c.open(uri, src)
		require.Empty(t, messages(params.Diagnostics), "premise: the call itself is valid")

		pos := callTargetPosition(t, src, "./callee.yaml")
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, fileURI(callee), got[0].URI, "the called file, not the calling one")

		// The callee's `name:`, which is what someone opening the file went there
		// to see — asserted by reading the range out of the callee's own text.
		assert.Equal(t, "provision-tenant", textInRange(calleeSource("provision-tenant"), got[0].Range))
	})

	t.Run("a callee that does not parse still has a first line to arrive at", func(t *testing.T) {
		t.Parallel()

		// Landing on `name:` is the better answer, not the condition for giving
		// one: a callee too broken to read a name out of is exactly the file an
		// author is trying to get to, and refusing to navigate there would make
		// the feature stop working when it is most wanted.
		dir := t.TempDir()
		callee := filepath.Join(dir, "callee.yaml")
		require.NoError(t, os.WriteFile(callee, []byte("name: [unclosed\n"), 0o644))

		src := callerSource("./callee.yaml")
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, src)

		pos := callTargetPosition(t, src, "./callee.yaml")
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, fileURI(callee), got[0].URI)
		assert.Equal(t, documentStart, got[0].Range)
	})

	t.Run("a callee that is not there is no location at all", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src := callerSource("./missing.yaml")
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		params := c.open(uri, src)
		require.NotEmpty(t, params.Diagnostics, "premise: the compiler reports the missing callee")

		pos := callTargetPosition(t, src, "./missing.yaml")
		assert.Empty(t, c.definition(uri, pos.Line, pos.Character),
			"opening an editor on a path that does not exist is worse than answering nothing")
	})

	t.Run("a target climbing out of the caller's directory is no location at all", func(t *testing.T) {
		t.Parallel()

		// The case a second path rule gets wrong, in the direction this language
		// actually decided: a call may reach anything at or below its own file's
		// directory and nothing above it, so `../other/workflow.yaml` is refused
		// by the compiler even when a file is sitting right there. An editor that
		// resolved it by plain path joining would navigate to a file the run
		// refuses to compile — which is exactly the disagreement sharing
		// flowfile.ResolveCallTarget exists to prevent.
		root := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(root, "other"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "other", "workflow.yaml"),
			[]byte(calleeSource("sibling")), 0o644))
		require.NoError(t, os.MkdirAll(filepath.Join(root, "here"), 0o755))

		src := callerSource("../other/workflow.yaml")
		caller := filepath.Join(root, "here", "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		params := c.open(uri, src)
		require.NotEmpty(t, params.Diagnostics, "premise: the compiler refuses a call that climbs")

		pos := callTargetPosition(t, src, "../other/workflow.yaml")
		assert.Empty(t, c.definition(uri, pos.Line, pos.Character))
	})

	t.Run("a target containing an expression is no location at all", func(t *testing.T) {
		t.Parallel()

		for _, target := range []string{"${'callee.yaml'}", "./callee${'.yaml'}"} {
			t.Run(target, func(t *testing.T) {
				t.Parallel()

				dir := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(dir, target),
					[]byte(calleeSource("decoy")), 0o644))

				src := callerSource(target)
				caller := filepath.Join(dir, "workflow.yaml")
				require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

				c := newClient(t)
				c.initialize()
				uri := "file://" + caller
				params := c.open(uri, src)
				require.NotEmpty(t, params.Diagnostics,
					"premise: the compiler refuses expressions in a call target")

				pos := callTargetPosition(t, src, target)
				assert.Empty(t, c.definition(uri, pos.Line, pos.Character),
					"invalid source must not navigate to a literal file with the same name")
			})
		}
	})

	t.Run("nowhere else on the step answers with the callee", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"),
			[]byte(calleeSource("provision-tenant")), 0o644))

		src := callerSource("./callee.yaml")
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, src)

		for _, where := range []struct {
			name   string
			needle string
			offset int
		}{
			{"on the step's id", "id: provision", len("id: ")},
			{"on the call key itself", "call: ./callee.yaml", 0},
			{"on a with: argument", "tenant: acme", 0},
			{"on the with: value", "tenant: acme", len("tenant: ")},
		} {
			t.Run(where.name, func(t *testing.T) {
				pos := positionOf(t, src, where.needle, where.offset)
				assert.Empty(t, c.definition(uri, pos.Line, pos.Character),
					"only the target value navigates to the callee")
			})
		}
	})

	t.Run("a document with no filesystem location resolves nothing", func(t *testing.T) {
		t.Parallel()

		// An untitled buffer has no directory for a relative path to mean
		// anything against, so there is nothing to guess at — the same answer the
		// diagnostics give it.
		src := callerSource("./callee.yaml")

		c := newClient(t)
		c.initialize()
		const uri = "untitled:Untitled-1"
		c.open(uri, src)

		pos := callTargetPosition(t, src, "./callee.yaml")
		assert.Empty(t, c.definition(uri, pos.Line, pos.Character))
	})
}

// TestDefinitionResolvesACallAgainstTheCallingFile is the case a second path rule
// gets wrong: a call is relative to the calling *file's* directory, and to
// nothing else — not the process's working directory, which in an editor is
// wherever the editor happened to be started.
//
// Not parallel, and top-level rather than a subtest, because it changes the
// working directory, which is process-wide and refused inside a parallel tree.
func TestDefinitionResolvesACallAgainstTheCallingFile(t *testing.T) {
	// Not parallel: it changes the working directory, which is process-wide.

	// Two files at the same relative path, one under the caller's directory
	// and a decoy under the working directory. Only a rule anchored to the
	// *calling file* picks the first, and only asserting on the URI can tell
	// the two apart.
	elsewhere := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(elsewhere, "workflows"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(elsewhere, "workflows", "callee.yaml"),
		[]byte(calleeSource("decoy")), 0o644))
	t.Chdir(elsewhere)

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "workflows"), 0o755))
	callee := filepath.Join(dir, "workflows", "callee.yaml")
	require.NoError(t, os.WriteFile(callee, []byte(calleeSource("the-real-one")), 0o644))

	src := callerSource("./workflows/callee.yaml")
	caller := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

	c := newClient(t)
	c.initialize()
	uri := "file://" + caller
	c.open(uri, src)

	pos := callTargetPosition(t, src, "./workflows/callee.yaml")
	got := c.definition(uri, pos.Line, pos.Character)
	require.Len(t, got, 1)
	assert.Equal(t, fileURI(callee), got[0].URI)
	assert.Equal(t, "the-real-one", textInRange(calleeSource("the-real-one"), got[0].Range),
		"the decoy under the working directory must not win")

	// And the compiler, reading the same file from the same place, embedded
	// that same callee — the agreement the shared resolution exists for,
	// asserted rather than assumed.
	compiled, _, err := flowfile.ParseAt([]byte(src), caller)
	require.NoError(t, err)
	require.Len(t, compiled.GetSteps(), 1)
	assert.Equal(t, "the-real-one", compiled.GetSteps()[0].GetCall().GetWorkflow().GetName())
}
