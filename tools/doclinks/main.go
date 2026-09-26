// Command doclinks checks that every doc link in this repository's Go doc
// comments resolves the way pkg.go.dev and gopls will render it.
//
//	go run ./tools/doclinks        # the whole repository
//	go run ./tools/doclinks ./pkg  # one subtree
//
// A doc link is a bracketed name in a doc comment, such as a type, a
// Type.Method, a pkg.Name, or an import path. The doc comment syntax turns it
// into a hyperlink only when the name resolves from the package the comment
// lives in. One that does not is not an error anywhere in the toolchain: it
// renders as literal brackets, and the reader finds out on the published
// page. The usual causes, each spelled below as it appears in a comment:
//
//	[flowfile.Parse]           a package this file does not import
//	[TestRoundTrip]            a test or unexported function
//	[pkg/flowstate/v1/engine]  a slash fragment, read as a nonexistent import path
//	[Tasks.Install]ed          a link glued to a suffix
//
// A cross-package link that cannot import its target spells the full import
// path, [github.com/picatz/flowstate/pkg/flowstate/v1/engine.Run]; a name that
// is not a link at all is written without brackets.
//
// Three checks, over the package comment and the doc comment of every
// exported top-level declaration, in every package under the root:
//
//   - a bracketed Go name left as plain text is a link that did not resolve;
//   - a link to an import path must name a standard-library package or a
//     directory that exists, and one inside this repository must name an
//     exported symbol of that package;
//   - a link through an imported package inside this repository must name an
//     exported symbol of that package.
//
// This command changes nothing. TestTheRepositoryDocLinksResolve holds the
// repository to zero findings under go test ./..., the way tools/citations
// holds the Markdown's file citations.
package main

import (
	"fmt"
	"os"
)

// main prints every doc link that does not resolve and exits non-zero on any.
func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	findings, links, err := Check(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "doclinks: %v\n", err)
		os.Exit(2)
	}
	for _, f := range findings {
		fmt.Println(f)
	}
	fmt.Printf("doclinks: %d links checked, %d do not resolve\n", links, len(findings))
	if len(findings) > 0 {
		os.Exit(1)
	}
}
