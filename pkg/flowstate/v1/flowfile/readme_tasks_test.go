package flowfile_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestREADMEPointsAtGeneratedCapabilityReferences keeps the front door connected
// to the catalogs that own these lists.
//
// The README used to duplicate every built-in task field and CEL extension name.
// Tests held those copies to the registry, but the generated references already do
// that from their real sources and CI verifies regeneration leaves no diff. Keeping
// the same lists in the front door made it longer without creating another check.
// The useful invariant here is discoverability: a reader can reach both canonical
// catalogs, and the targets exist.
func TestREADMEPointsAtGeneratedCapabilityReferences(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..", "..")
	data, err := os.ReadFile(filepath.Join(root, "README.md"))
	require.NoError(t, err)

	for _, target := range []string{"docs/reference/tasks.md", "docs/reference/cel.md"} {
		assert.Contains(t, string(data), "]("+target+")",
			"the README does not link the generated %s reference", filepath.Base(target))
		assert.FileExists(t, filepath.Join(root, filepath.FromSlash(target)))
	}
}

// exampleLink matches a link to an example directory in examples/README.md.
//
// The whole markdown link, not just a parenthesised word. It used to be the
// latter, which meant any ordinary prose in a round bracket was read as a link to
// an example — describing a task as built on `(go-git)`, or a verb as `(read)`,
// failed this test with a complaint about a stale link to a directory nobody had
// ever mentioned. The diagnostic was confidently wrong, which is worse than
// silence, and it punished writing rather than catching a mistake.
//
// Nested targets are matched too. They did not used to be: the pattern stopped at
// the first slash, on the reasoning that `[plugins/greet](plugins/greet/)` sits a
// directory deeper on purpose and the glob only walked one level. Both halves of
// that were true and the conclusion was still wrong — it meant the index's own
// completeness check could not see the deeper half of the index, and
// `plugins/codex` and `plugins/sql` were absent from the table for exactly as
// long as nobody read it by hand. A guard that walks one level of a two-level
// tree reports on a page rather than on the walk.
var exampleLink = regexp.MustCompile(`\[[^\]]+\]\(([^)\s#]+)\)`)

// exampleLinkTarget normalizes a matched link into the example path it names, or
// "" for a link that names something else.
//
// Links out of the directory (`../docs/USE_CASES.md`) and to the web are not
// claims about an example and are dropped. A link to an example's README
// (`[embedding](embedding/README.md)`) is a link to that example, because that is
// the file a reader is being sent to read.
func exampleLinkTarget(target string) string {
	if strings.HasPrefix(target, "../") || strings.Contains(target, "://") {
		return ""
	}

	target = strings.TrimSuffix(target, "/")
	target = strings.TrimSuffix(target, "/README.md")

	if target == "" || strings.HasSuffix(target, ".md") {
		return ""
	}

	return target
}

// TestExamplesREADMEListsEveryExample keeps the examples index complete.
//
// Nothing is missing from it today. This is the unusual case where the audit item
// was already stale — a note said two examples were absent and both are listed —
// so there is nothing to fix, and a test is the only thing worth adding.
//
// It earns its place because the index is exactly the kind of file that goes
// wrong quietly. Adding an example is a directory and a workflow; updating a
// README is a separate act nothing forces, and the failure is invisible from
// inside the change that causes it. An example nobody links is an example nobody
// runs, which is the same defect as a capability with no example, one surface
// further out.
//
// Both directions, per the lesson from the two tests above. A stale row survives a
// directory being renamed or removed, and sends a reader to a 404 — which is worse
// than an omission, because the omission merely hides something while the stale
// link actively wastes their time.
func TestExamplesREADMEListsEveryExample(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..", "..")

	data, err := os.ReadFile(filepath.Join(root, "examples", "README.md"))
	require.NoError(t, err, "examples/README.md moved and this test did not")

	linked := map[string]bool{}
	for _, m := range exampleLink.FindAllStringSubmatch(string(data), -1) {
		if target := exampleLinkTarget(m[1]); target != "" {
			linked[target] = true
		}
	}
	require.NotEmpty(t, linked,
		"no example links found; either the index changed shape or this pattern stopped matching it")

	// Every example, at whatever depth it sits — `plugins/sql` and
	// `operations/worker-versioning` are examples in exactly the sense
	// `hello-world` is, and the reason they were ever exempt was that the
	// pattern above could not name them.
	var onDisk []string
	examplesDir := filepath.Join(root, "examples")
	require.NoError(t, filepath.WalkDir(examplesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "workflow.yaml" {
			return nil
		}
		rel, err := filepath.Rel(examplesDir, filepath.Dir(path))
		if err != nil {
			return err
		}
		onDisk = append(onDisk, filepath.ToSlash(rel))
		return nil
	}))
	require.NotEmpty(t, onDisk, "no examples found; the walk is wrong")

	for _, name := range onDisk {
		// Its own row, or an ancestor's. `embedding/flowfile` is the Flowfile
		// belonging to the `embedding` example rather than an example in its
		// own right, and a reader sent to `embedding` has been sent to it —
		// demanding a separate row would be asking the index to list parts.
		covered := false
		for path := name; ; {
			if linked[path] {
				covered = true
				break
			}
			parent := filepath.ToSlash(filepath.Dir(path))
			if parent == path || parent == "." {
				break
			}
			path = parent
		}

		assert.True(t, covered,
			"examples/%s exists and examples/README.md links neither it nor a directory above it\n"+
				"  an example nobody links is an example nobody runs", name)
	}

	for name := range linked {
		info, err := os.Stat(filepath.Join(examplesDir, name))
		assert.NoError(t, err,
			"examples/README.md links %q, which is not there\n"+
				"  a stale link sends a reader somewhere that is not", name)
		if err == nil {
			assert.True(t, info.IsDir(),
				"examples/README.md links %q, which is not a directory", name)
		}
	}
}
