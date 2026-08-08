package lsp

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A call's `with:` block is the one place a Flowfile's keys are declared in
// another file, so every case here is written over the wire against files on
// disk: what is under test is the join of the path rule, the read, and the menu,
// and a test that builds the callee in memory would prove none of it.

// withCalleeSource is a callee declaring one input of each shape a `with:` menu
// has to describe: required with a constraint, optional with a default, and
// optional with neither.
func withCalleeSource() string {
	return `edition: v2026.2
name: provision-tenant
inputs:
  tenant:
    type: string
    required: true
    description: Name of the tenant to provision.
    min_len: 3
    max_len: 63
    must: this.size() > 2
  region:
    type: string
    default: us-east-1
    description: Where to provision it.
  dry_run:
    type: bool
steps:
  - id: announce
    log:
      message: hello
`
}

// withCallerSource is a call binding the arguments named, with the cursor marker
// on a fresh line inside the `with:` block.
func withCallerSource(target string, bound ...string) string {
	var b strings.Builder
	b.WriteString("edition: v2026.2\nname: caller\nsteps:\n  - id: provision\n    call: " + target + "\n    with:\n")
	for _, name := range bound {
		b.WriteString("      " + name + ": value-for-" + name + "\n")
	}
	b.WriteString("      |\n")
	return b.String()
}

// writeCall writes a caller and a callee into dir and returns the caller's path
// and the source that was written, with the cursor marker removed.
func writeCall(t *testing.T, dir, calleeRel, callerSrc string) (string, string, lsp.Position) {
	t.Helper()

	callee := filepath.Join(dir, filepath.FromSlash(calleeRel))
	require.NoError(t, os.MkdirAll(filepath.Dir(callee), 0o755))
	require.NoError(t, os.WriteFile(callee, []byte(withCalleeSource()), 0o644))

	src, pos := splitCursor(t, callerSrc)
	caller := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))
	return caller, src, pos
}

// TestCompletionOffersACalleesDeclaredInputs is the feature: an author writing
// `with:` is offered what the called workflow takes, read from the called
// workflow.
func TestCompletionOffersACalleesDeclaredInputs(t *testing.T) {
	t.Parallel()

	t.Run("every declared input, required first", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		caller, _, pos := writeCall(t, dir, "callee.yaml", withCallerSource("./callee.yaml"))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, mustRead(t, caller))

		got := c.complete(uri, pos.Line, pos.Character)
		assert.Equal(t, []string{"tenant", "region", "dry_run"}, labels(got.Items),
			"the callee's own inputs, the one that must be bound first")

		tenant := findItem(got.Items, "tenant")
		require.NotNil(t, tenant)
		assert.Contains(t, tenant.Detail, "string (required)")
		assert.Contains(t, tenant.Detail, "Name of the tenant to provision.")
		assert.Contains(t, tenant.Documentation, "provision-tenant")
		assert.Contains(t, tenant.Documentation, "this.size() > 2")
		require.NotNil(t, tenant.TextEdit)
		assert.Equal(t, "tenant: ", tenant.TextEdit.NewText, "a key is never written without its colon")

		region := findItem(got.Items, "region")
		require.NotNil(t, region)
		assert.Contains(t, region.Detail, `string (default "us-east-1")`)

		dryRun := findItem(got.Items, "dry_run")
		require.NotNil(t, dryRun)
		assert.Equal(t, "bool", dryRun.Detail, "no default and not required is just the type")
	})

	t.Run("arguments already bound are left out", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		caller, _, pos := writeCall(t, dir, "callee.yaml",
			withCallerSource("./callee.yaml", "tenant", "region"))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, mustRead(t, caller))

		got := c.complete(uri, pos.Line, pos.Character)
		assert.Equal(t, []string{"dry_run"}, labels(got.Items),
			"the menu is what is left to write")
	})

	t.Run("the name being typed narrows the menu", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		caller, _, pos := writeCall(t, dir, "callee.yaml",
			strings.Replace(withCallerSource("./callee.yaml"), "      |", "      re|", 1))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, mustRead(t, caller))

		got := c.complete(uri, pos.Line, pos.Character)
		assert.Equal(t, []string{"region"}, labels(got.Items))
		require.NotNil(t, got.Items[0].TextEdit)
		assert.Equal(t, "re", textInRange(mustRead(t, caller), got.Items[0].TextEdit.Range),
			"the partial word is replaced, not appended to")
	})

	t.Run("a callee in another directory resolves against the calling file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		caller, _, pos := writeCall(t, dir, "workflows/nested/callee.yaml",
			withCallerSource("./workflows/nested/callee.yaml"))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, mustRead(t, caller))

		got := c.complete(uri, pos.Line, pos.Character)
		assert.Equal(t, []string{"tenant", "region", "dry_run"}, labels(got.Items))
	})

	t.Run("a caller whose path needs percent-encoding is still read", func(t *testing.T) {
		t.Parallel()

		// A directory whose name a real editor sends percent-encoded. Resolution
		// starts from the caller's own path, so a URI trimmed rather than parsed
		// resolves the callee against a directory that does not exist and the
		// menu silently empties.
		dir := filepath.Join(t.TempDir(), "my workflows #1")
		require.NoError(t, os.MkdirAll(dir, 0o755))
		caller, _, pos := writeCall(t, dir, "callee.yaml", withCallerSource("./callee.yaml"))

		uri := (&url.URL{Scheme: "file", Path: filepath.ToSlash(caller)}).String()
		require.Contains(t, uri, "%20", "premise: the URI is percent-encoded")
		require.Contains(t, uri, "%231", "premise: the URI escapes the fragment character")

		c := newClient(t)
		c.initialize()
		c.open(uri, mustRead(t, caller))

		got := c.complete(uri, pos.Line, pos.Character)
		assert.Equal(t, []string{"tenant", "region", "dry_run"}, labels(got.Items))
	})
}

// TestCompletionOffersNothingForAnUnreadableCallee covers the direction that
// matters more: a name offered here is a name the compiler will type-check, so a
// target the compiler cannot read must produce an empty menu rather than a guess.
func TestCompletionOffersNothingForAnUnreadableCallee(t *testing.T) {
	t.Parallel()

	t.Run("a callee that is not there", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		src, pos := splitCursor(t, withCallerSource("./missing.yaml"))
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		params := c.open(uri, src)
		require.NotEmpty(t, params.Diagnostics, "premise: the compiler reports the missing callee")

		assert.Empty(t, c.complete(uri, pos.Line, pos.Character).Items)
	})

	t.Run("a target climbing out of the caller's directory", func(t *testing.T) {
		t.Parallel()

		// A file is sitting right there, and the compiler still refuses to read
		// it. A menu built by plain path joining would offer its inputs.
		root := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(root, "other"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "other", "callee.yaml"),
			[]byte(withCalleeSource()), 0o644))
		here := filepath.Join(root, "here")
		require.NoError(t, os.MkdirAll(here, 0o755))

		src, pos := splitCursor(t, withCallerSource("../other/callee.yaml"))
		caller := filepath.Join(here, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		params := c.open(uri, src)
		require.NotEmpty(t, params.Diagnostics, "premise: the compiler refuses a call that climbs")

		assert.Empty(t, c.complete(uri, pos.Line, pos.Character).Items)
	})

	t.Run("a callee that does not compile", func(t *testing.T) {
		t.Parallel()

		// Go-to-definition still navigates into a broken callee, because arriving
		// in the file being fixed is useful. Offering names read out of one is
		// not: there is nothing there to be sure of.
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), []byte("name: [unclosed\n"), 0o644))

		src, pos := splitCursor(t, withCallerSource("./callee.yaml"))
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, src)

		assert.Empty(t, c.complete(uri, pos.Line, pos.Character).Items)
	})

	t.Run("a buffer with no filesystem location", func(t *testing.T) {
		t.Parallel()

		src, pos := splitCursor(t, withCallerSource("./callee.yaml"))

		c := newClient(t)
		c.initialize()
		const uri = "untitled:Untitled-1"
		c.open(uri, src)

		assert.Empty(t, c.complete(uri, pos.Line, pos.Character).Items,
			"an untitled buffer has no directory a relative path could mean anything against")
	})

	t.Run("a step that is not a call", func(t *testing.T) {
		t.Parallel()

		// `with:` beside no `call:` names nothing, so there is nothing to read.
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), []byte(withCalleeSource()), 0o644))

		src, pos := splitCursor(t, `edition: v2026.2
name: caller
steps:
  - id: provision
    with:
      |
`)
		caller := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, src)

		assert.Empty(t, c.complete(uri, pos.Line, pos.Character).Items)
	})
}

// TestHoverDescribesACallArgument answers the other half of the same question:
// what an argument already written means, from the declaration it is checked
// against.
func TestHoverDescribesACallArgument(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	callee := filepath.Join(dir, "callee.yaml")
	require.NoError(t, os.WriteFile(callee, []byte(withCalleeSource()), 0o644))

	src := `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
      region: eu-west-1
      surprise: nobody asked for this
`
	caller := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

	c := newClient(t)
	c.initialize()
	uri := "file://" + caller
	c.open(uri, src)

	t.Run("a required argument", func(t *testing.T) {
		pos := positionOf(t, src, "tenant: acme", 1)
		got := hoverText(c.hover(uri, pos.Line, pos.Character))
		assert.Contains(t, got, "**`tenant`** · `string` · required")
		assert.Contains(t, got, "Input of workflow `provision-tenant`, declared in `"+callee+"`.",
			"the answer names the file it came from, which is the file the reader cannot see")
		assert.Contains(t, got, "Name of the tenant to provision.")
		assert.Contains(t, got, "Held to at least 3 characters, at most 63 characters.",
			"a declared bound is checked at validation exactly as must: is, so a hover showing one and not the other tells half the contract")
		assert.Contains(t, got, "Must satisfy `this.size() > 2`.")
	})

	t.Run("an optional argument with a default", func(t *testing.T) {
		pos := positionOf(t, src, "region: eu-west-1", 1)
		got := hoverText(c.hover(uri, pos.Line, pos.Character))
		assert.Contains(t, got, "**`region`** · `string` · optional · default `\"us-east-1\"`")
		assert.Contains(t, got, "Where to provision it.")
	})

	t.Run("an argument the callee does not declare says nothing", func(t *testing.T) {
		// The compiler already reports it, naming what the workflow does take. A
		// popup inventing a meaning would contradict the diagnostic on the same
		// key.
		pos := positionOf(t, src, "surprise: nobody", 1)
		assert.Nil(t, c.hover(uri, pos.Line, pos.Character))
	})

	t.Run("the value beside an argument says nothing", func(t *testing.T) {
		pos := positionOf(t, src, "tenant: acme", len("tenant: "))
		assert.Nil(t, c.hover(uri, pos.Line, pos.Character),
			"the key is what the callee declares; the value is this file's own")
	})
}

// TestHoverAndCompletionAnswerThroughADigestMismatch fixes the behaviour a pin
// must not have.
//
// A `digest:` that no longer matches means the callee changed since the author
// reviewed it, which is the validator's diagnostic to raise. The author reading
// that diagnostic is looking at the file as it is now, so an editor that went
// quiet would withdraw the help exactly when it is being used.
func TestHoverAndCompletionAnswerThroughADigestMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), []byte(withCalleeSource()), 0o644))

	const stalePin = "sha256:" + "0000000000000000000000000000000000000000000000000000000000000000"
	src, completeAt := splitCursor(t, `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    digest: `+stalePin+`
    with:
      tenant: acme
      |
`)
	caller := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(caller, []byte(src), 0o644))

	// The premise, from the compiler rather than from this test's opinion of it.
	_, _, err := flowfile.ParseAt([]byte(src), caller)
	require.Error(t, err, "premise: the pin does not match the callee")

	c := newClient(t)
	c.initialize()
	uri := "file://" + caller
	params := c.open(uri, src)
	require.NotEmpty(t, params.Diagnostics, "premise: the mismatch is reported")

	pos := positionOf(t, src, "tenant: acme", 1)
	assert.Contains(t, hoverText(c.hover(uri, pos.Line, pos.Character)), "**`tenant`** · `string` · required")

	got := c.complete(uri, completeAt.Line, completeAt.Character)
	assert.Equal(t, []string{"region", "dry_run"}, labels(got.Items),
		"the menu is what is left to write, unchanged by a pin nobody is asking about")
}

// mustRead returns a file's contents, for opening the document the test just
// wrote.
func mustRead(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

// TestCompletionDecodesTheCallTargetAsYAML pins the scanner's reading of the
// `call:` line to YAML's rather than to substring surgery. Each spelling below
// is a valid way to write the same target, and each defeated the old trimming
// in a different way: a comment after a quoted scalar left the quotes on, an
// anchor stayed glued to the path, and an escape in a double-quoted scalar
// stayed escaped. The wrong reading is not a missing menu, it is
// ResolveCallTarget handed a filename that does not exist.
func TestCompletionDecodesTheCallTargetAsYAML(t *testing.T) {
	t.Parallel()

	for name, target := range map[string]string{
		"quoted then a comment": `"./callee.yaml" # provisions the tenant`,
		"anchored":              `&provision ./callee.yaml`,
		"single quoted":         `'./callee.yaml'`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			caller, _, pos := writeCall(t, dir, "callee.yaml", withCallerSource(target))

			c := newClient(t)
			c.initialize()
			uri := "file://" + caller
			c.open(uri, mustRead(t, caller))

			got := c.complete(uri, pos.Line, pos.Character)
			assert.Equal(t, []string{"tenant", "region", "dry_run"}, labels(got.Items),
				"a decoded target reaches the same callee however it is spelled")
		})
	}

	t.Run("a flow collection is not a path", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		caller, _, pos := writeCall(t, dir, "callee.yaml", withCallerSource(`["./callee.yaml"]`))

		c := newClient(t)
		c.initialize()
		uri := "file://" + caller
		c.open(uri, mustRead(t, caller))

		got := c.complete(uri, pos.Line, pos.Character)
		assert.Empty(t, got.Items, "a shape that cannot be a path offers nothing rather than a guess")
	})
}
