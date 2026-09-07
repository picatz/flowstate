package main

import (
	"fmt"
	"os"
)

// main prints every citation that does not hold and exits non-zero on any.
// It changes nothing: which line a sentence should point at is a claim about
// the sentence, and only its author can move it.
func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	docs, err := Documents(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "citations: %v\n", err)
		os.Exit(2)
	}
	findings, total, err := Check(root, docs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "citations: %v\n", err)
		os.Exit(2)
	}
	for _, f := range findings {
		fmt.Println(f)
	}
	fmt.Printf("citations: %d checked in %d document(s), %d do not hold\n", total, len(docs), len(findings))
	if len(findings) > 0 {
		os.Exit(1)
	}
}
