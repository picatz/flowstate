// Command dupbodies reports function bodies that appear more than once.
//
//	go run ./tools/dupbodies           # every duplicated body, largest first
//	go run ./tools/dupbodies ./pkg/... # one subtree
//
// A body written twice is two places a fix has to land, and the second copy
// is the one that misses it. #1708 counted four copies of one CEL rule
// evaluator that had drifted on what an unevaluable rule means, and #1709
// counted the test helpers every package wrote for itself; each was found by
// a person reading, months after the second copy arrived. This command finds
// the next one in the diff that adds it.
//
// It compares bodies after printing them without comments or positions, so
// only the same code matches. A renamed copy is missed on purpose: a report
// that names near-misses is one people learn to ignore, and this one is meant
// to be believed. Generated files are skipped, since the copy there is the
// generator's to make.
//
// This command changes nothing. Its findings are held by
// [TestTheRepositoryDuplicateBodiesOnlyGoDown], a ratchet: a body that
// appears twice fails it until the copy is shared or recorded there with its
// reason, and a copy removed fails it until the table shrinks, so the table
// cannot keep stale entries.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: go run ./tools/dupbodies [path]\n\n")
		fmt.Fprintf(os.Stderr, "Reports function bodies that appear more than once. Changes nothing.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	root := "."
	if flag.NArg() > 0 {
		root = strings.TrimSuffix(strings.TrimSuffix(flag.Arg(0), "..."), "/")
		if root == "" {
			root = "."
		}
	}

	groups, functions, err := Analyze(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dupbodies: %v\n", err)
		os.Exit(2)
	}

	writeReport(os.Stdout, groups, functions, root)
}

// writeReport prints every group, largest body first.
func writeReport(out io.Writer, groups []Group, functions int, root string) {
	fmt.Fprintf(out, "dupbodies: %d body(ies) of %d statements or more appear in more than one of %d function(s) under %s\n\n",
		len(groups), MinStatements, functions, root)

	for _, group := range groups {
		fmt.Fprintf(out, "    %d statements, %d copies:\n", group.Statements, len(group.Members))
		for _, member := range group.Members {
			fmt.Fprintf(out, "        %s\n", member)
		}
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "Each is two places a fix has to land. Share the body, or record why it is written twice.\n")
}
