package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The consolidation rewrite is proved byte for byte and by meaning: the rewritten
// file must compile to the very steps the repeated one did, because the reason it
// is safe is that a binding expands into exactly what it replaced. The fixture
// plugin `bound` has one task, `bound.use`, whose `token` input claims the
// credential `api_token`.

const repeatedHeader = "edition: v2026.4\nname: bound\n"

// fixBound runs Fix with the fixture plugin registered. Not parallel: it writes the
// process-wide registry Fix reads.
func fixBound(t *testing.T, in string) flowfile.FixResult {
	t.Helper()

	registerBoundPlugin(t)
	got, err := flowfile.Fix([]byte(in))
	require.NoError(t, err)

	return got
}

// sameSteps proves the rewritten file compiles to the steps the original did.
func sameSteps(t *testing.T, before, after string) {
	t.Helper()

	was, _, err := flowfile.Parse([]byte(before))
	require.NoError(t, err)
	now, _, err := flowfile.Parse([]byte(after))
	require.NoError(t, err)
	require.Len(t, now.GetSteps(), len(was.GetSteps()))
	for i := range was.GetSteps() {
		require.True(t, proto.Equal(was.GetSteps()[i], now.GetSteps()[i]), "step %d compiles differently after the rewrite", i)
	}
}

func TestFixBindsARepeatedCredentialOnceAndKeepsAnOverride(t *testing.T) {
	in := repeatedHeader + `plugins:
  bound: v0.1.0 # the plugin
steps:
  - id: a
    bound.use:
      note: one
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: "${secret('env:BOUND_TOKEN')}"
      note: two
  - id: partner
    bound.use:
      note: three
      token: ${secret('env:OVERRIDE_TOKEN')}
  - id: c
    bound.use:
      note: four
      token: ${secret('env:BOUND_TOKEN')}
`
	want := repeatedHeader + `plugins:
  bound:
    version: v0.1.0 # the plugin
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: a
    bound.use:
      note: one
  - id: b
    bound.use:
      note: two
  - id: partner
    bound.use:
      note: three
      token: ${secret('env:OVERRIDE_TOKEN')}
  - id: c
    bound.use:
      note: four
`
	got := fixBound(t, in)
	require.True(t, got.Complete())
	require.Equal(t, want, string(got.Source))
	require.Len(t, got.Changes, 4, "one binding and three removed repetitions")
	sameSteps(t, in, want)

	again, err := flowfile.Fix(got.Source)
	require.NoError(t, err)
	require.False(t, again.Changed(), "the rewritten file is a fixed point")
	require.Equal(t, want, string(again.Source))
}

func TestFixBindsInsideNestedStepsAndExtendsAMappingEntry(t *testing.T) {
	in := repeatedHeader + `plugins:
  bound:
    version: v0.1.0
steps:
  - id: each
    for_each:
      items: [a, b]
      steps:
        - id: inner
          bound.use:
            token: ${secret('env:BOUND_TOKEN')}
  - id: outer
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`
	want := repeatedHeader + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: each
    for_each:
      items: [a, b]
      steps:
        - id: inner
          bound.use: {}
  - id: outer
    bound.use: {}
`
	got := fixBound(t, in)
	require.Equal(t, want, string(got.Source))
	sameSteps(t, in, want)
}

func TestFixLeavesWhatItCannotMoveWithoutLosingSomething(t *testing.T) {
	cases := map[string]string{
		"one use is not a repetition": repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`,
		"a tie has no repeated reference": repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      token: ${secret('env:ONE')}
  - id: b
    bound.use:
      token: ${secret('env:TWO')}
`,
		"the plugin is not listed": repeatedHeader + `steps:
  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`,
		"the credential is bound already": repeatedHeader + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`,
		"an expression is not a reference": repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      token: ${secret('env:' + 'BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:' + 'BOUND_TOKEN')}
`,
		"flow style has no line to remove": repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use: {token: "${secret('env:BOUND_TOKEN')}"}
  - id: b
    bound.use: {token: "${secret('env:BOUND_TOKEN')}"}
`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			got := fixBound(t, in)
			require.Equal(t, in, string(got.Source))
			require.False(t, got.Changed())
		})
	}
}

func TestFixKeepsACommentedSiteAsItsOwnReference(t *testing.T) {
	in := repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      # the token for the first call
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: c
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`
	want := repeatedHeader + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: a
    bound.use:
      # the token for the first call
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use: {}
  - id: c
    bound.use: {}
`
	got := fixBound(t, in)
	require.Equal(t, want, string(got.Source), "the commented site keeps its reference, which is the binding's value")
	sameSteps(t, in, want)
}

func TestFixCheckModeDescribesTheConsolidationWithoutWritingIt(t *testing.T) {
	in := repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`
	got := fixBound(t, in)
	require.True(t, got.Changed())
	require.Len(t, got.Changes, 3)
	for _, change := range got.Changes {
		require.NotContains(t, change.Message, "BOUND_TOKEN", "a change names the credential and never the reference")
		require.NotContains(t, change.Pending, "BOUND_TOKEN")
		require.Contains(t, change.Pending, "would")
	}
}

func TestFixDoesNotConsolidateWithoutThePluginLoaded(t *testing.T) {
	in := repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`
	got, err := flowfile.Fix([]byte(in))
	require.NoError(t, err)
	require.Equal(t, in, string(got.Source), "with no plugin there is no credential to read, so nothing moves")
}

// A binding is inherited by every task that claims the credential and does not
// write it, wherever it sits. These are the shapes that must stop the rewrite
// rather than start handing a secret to a task that never wrote one.
func TestFixDoesNotBindWhereAnotherTaskWouldInheritIt(t *testing.T) {
	const repeated = `  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
`
	cases := map[string]string{
		"a step that omits the input (unbound today)": repeated + `  - id: c
    bound.use:
      note: omitted
`,
		"an undo step that omits the input": repeated + `  - id: c
    log:
      message: x
    undo:
      bound.use:
        note: omitted
`,
		"an undo step with a flow-style body": repeated + `  - id: c
    log:
      message: x
    undo:
      bound.use: {note: omitted}
`,
		"a step with no body": repeated + `  - id: c
    bound.use:
`,
	}
	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			in := repeatedHeader + "plugins:\n  bound: v0.1.0\nsteps:\n" + steps
			got := fixBound(t, in)
			require.Equal(t, in, string(got.Source), "a binding would have been inherited by a task that never wrote one")
			require.False(t, got.Changed())
		})
	}
}

func TestFixStillBindsWhenAnUndoStepWritesItsOwn(t *testing.T) {
	in := repeatedHeader + `plugins:
  bound: v0.1.0
steps:
  - id: a
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
  - id: b
    bound.use:
      token: ${secret('env:BOUND_TOKEN')}
    undo:
      bound.use:
        token: ${secret('env:OVERRIDE_TOKEN')}
`
	got := fixBound(t, in)
	require.True(t, got.Changed(), "an undo step that writes its own reference inherits nothing")
	require.Contains(t, string(got.Source), "api_token: ${secret('env:BOUND_TOKEN')}")
	require.Contains(t, string(got.Source), "token: ${secret('env:OVERRIDE_TOKEN')}")
}

// The equality guard is what withdraws a consolidation nothing else caught: steps
// that differ once compiled, or a document that does not compile at all, prove
// nothing and so are not the same.
func TestFixCredentialGuardComparesCompiledSteps(t *testing.T) {
	doc := func(message string) []byte {
		return []byte(repeatedHeader + "steps:\n  - id: a\n    log:\n      message: " + message + "\n")
	}
	require.True(t, flowfile.SameCompiledStepsForTest(doc("one"), doc("one")))
	require.False(t, flowfile.SameCompiledStepsForTest(doc("one"), doc("two")), "different steps were called the same")
	require.False(t, flowfile.SameCompiledStepsForTest(doc("one"), []byte("not: [a flowfile")), "a document that does not compile proved equality")
}
