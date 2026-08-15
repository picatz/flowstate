package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/docsgen"
	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
)

// referenceDir is the committed reference, from this package's directory.
const referenceDir = "../../docs/reference"

// referenceGenerator is the generator over this binary's real sources — the
// cobra tree, the MCP registration, the address default.
//
// These tests live here rather than in the docsgen package for that reason:
// what they check is that the *committed* reference matches what this binary
// generates, and only this package can hand the generator what this binary
// dispatches on.
func referenceGenerator(t *testing.T) *docsgen.Generator {
	t.Helper()

	generator, err := newReferenceGenerator()
	require.NoError(t, err)

	return generator
}

// cliReference renders one named document, for the test that is about that
// document rather than about the set.
func cliReference(t *testing.T, generator *docsgen.Generator) string {
	t.Helper()

	for _, doc := range generator.Documents() {
		if doc.Name == "cli.md" {
			return doc.Render()
		}
	}

	t.Fatal("the CLI reference is no longer one of the generated documents")

	return ""
}

// The generated reference is only worth having if it cannot quietly stop being
// true, so the tests here are all about the ways it could.
//
// Three of them, one per way. It has to be reproducible, or CI's `git diff
// --exit-code` pin fails for reasons that are about the machine rather than the
// change. It has to be committed, so the pin is checkable at all — and checking
// it here as well means an author finds out from `go test` rather than from a
// round trip through CI. And the one table that is written by hand, the
// environment variables, has to be held to the tree in both directions — that
// last pair travels with the table, in the docsgen package.

// TestGeneratedDocsAreStable is the property the CI pin rests on.
//
// A generator that ranges over a map produces a document that is correct and
// different every time, which makes `git diff --exit-code` a coin flip and
// teaches everyone to re-run it until it passes. Rendering twice and comparing
// bytes is the cheapest way to catch an unsorted iteration the moment it is
// introduced, and it fails deterministically enough to be believed — Go
// randomizes map order per range, so an unsorted listing of any size loses this
// almost immediately.
func TestGeneratedDocsAreStable(t *testing.T) {
	for _, doc := range referenceGenerator(t).Documents() {
		t.Run(doc.Name, func(t *testing.T) {
			assert.Equal(t, doc.Render(), doc.Render(),
				"generating %s twice produced two different documents; something iterates unsorted", doc.Name)
		})
	}
}

// TestGeneratedDocsAreCommitted is the same check CI makes, made here first.
//
// CI is a backstop rather than the feedback loop: a task added, a flag renamed or
// an RPC introduced changes these files, and finding that out from a red build is
// a round trip that bought nothing. The failure says what to run, because the fix
// is one command and the alternative is somebody hand-editing a generated file.
func TestGeneratedDocsAreCommitted(t *testing.T) {
	for _, doc := range referenceGenerator(t).Documents() {
		t.Run(doc.Name, func(t *testing.T) {
			committed, err := os.ReadFile(filepath.Join(referenceDir, doc.Name))
			require.NoError(t, err, "the generated reference moved and this test did not")

			assert.Equal(t, doc.Render(), string(committed),
				"docs/reference/%s is out of date; run `flow docs generate` and commit the result", doc.Name)
		})
	}
}

// TestGeneratingRestoresTheEnvironment keeps the generator from being a command
// with a side effect.
//
// It has to clear the environment to be reproducible — flag defaults are read
// from it when the command tree is built — and clearing it is exactly the kind of
// thing that gets left cleared. In-process that would be invisible here and
// catastrophic in a test binary, where the next test would run against an
// environment this one emptied.
func TestGeneratingRestoresTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_ADDRESS", "example.test:9999")
	t.Setenv("FLOWSTATE_TOKEN", "not-a-real-token")

	rendered := cliReference(t, referenceGenerator(t))

	assert.Equal(t, "example.test:9999", os.Getenv("FLOWSTATE_ADDRESS"),
		"generating the CLI reference left FLOWSTATE_ADDRESS cleared")
	assert.Equal(t, "not-a-real-token", os.Getenv("FLOWSTATE_TOKEN"),
		"generating the CLI reference left FLOWSTATE_TOKEN cleared")

	assert.NotContains(t, rendered, "example.test:9999",
		"the generated CLI reference recorded the environment it was generated in")
}

// TestEnvironmentMirrorsAreDerived pins the derivation the CLI reference's
// Environment column depends on.
//
// The mapping from a variable to the flag it feeds exists nowhere as data —
// pflag sees a default string and cannot know where it came from — so it is
// recovered by setting a sentinel and looking for it. That is clever enough to
// deserve a test that it still works: a change to how a default is composed
// (wrapping it in a `cmp.Or`, say, which is already how two of them are written)
// could silently empty the whole column, and an empty column reads as "no flag
// takes a variable" rather than as a broken derivation.
func TestEnvironmentMirrorsAreDerived(t *testing.T) {
	mirrors := referenceGenerator(t).EnvironmentMirrors()

	assert.Equal(t, "FLOWSTATE_ADDRESS", mirrors["flow get address"],
		"--address no longer reads FLOWSTATE_ADDRESS, or the derivation stopped working")
	assert.Equal(t, "TEMPORAL_TASK_QUEUE", mirrors["flow worker task-queue"],
		"--task-queue no longer reads TEMPORAL_TASK_QUEUE, or the derivation stopped working")
}

// TestEveryMCPToolHasALocality holds the one hand-kept fact on the MCP reference
// to the tools that are actually registered.
//
// Where a tool answers — in this process or against a server — is carried by a Go
// func value in [flowmcp.WorkflowServiceMethods]'s dispatch table, which nothing
// outside can inspect. So it is written down, and written down once, in
// [flowmcp.LocalTools]; this is what stops it being written down wrongly. Both
// directions, because each fails differently: a method with no entry is
// documented as needing a server when it does not, and an entry for a method
// that is gone is a line about a tool nobody can call.
func TestEveryMCPToolHasALocality(t *testing.T) {
	t.Parallel()

	methods := map[string]bool{}
	for _, method := range flowmcp.WorkflowServiceMethods() {
		methods[method.Name] = true
	}

	for name := range flowmcp.LocalTools {
		assert.True(t, methods[name],
			"flowmcp.LocalTools names %q, which is not a service method any more", name)
	}

	// The other direction is not "every method is local" — most are not — but that
	// every method was *considered*. A method absent from the map is documented as
	// remote, which is right today only because the map was written when those
	// methods were. So the assertion is that the tool set the reference renders
	// covers the registered set exactly.
	documented := map[string]bool{}
	for _, tool := range mcpToolDocs() {
		documented[tool.Name] = true
	}
	for _, method := range flowmcp.WorkflowServiceMethods() {
		assert.True(t, documented[flowmcp.ToolName(method.Name)],
			"the MCP reference does not document %q", flowmcp.ToolName(method.Name))
	}
	// Both of this surface's non-RPC tools, individually — not just any local
	// tool — so a third one added the way flowstate_test was (#241) and left
	// out of [mcpTools] fails here rather than shipping mute in the reference.
	for name := range documentedLocalTools {
		assert.True(t, documented[name],
			"the MCP reference does not document %q, which flow mcp registers with no RPC behind it", name)
	}
}
