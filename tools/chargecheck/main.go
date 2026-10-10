// Command chargecheck proves that every CEL evaluation reachable from the
// workflow-side engine is either charged to the workflow-slice budget or says,
// at the call, why it is not (#1970).
//
//	go run ./tools/chargecheck          # fail on an unlabelled uncharged site
//	go run ./tools/chargecheck -sites   # list every uncharged site and its status
//
// The rule is mechanical. An [v1.Evaluator] entry point that returns a cost
// (`...WithCost`) is the charged form: its caller owns adding the cost to the
// budget. One that returns none (Eval, EvalParsed, EvalParsedBase, EvalString)
// is uncharged, and a call to it from any function reachable from engine.Run
// must carry a `charge:exempt <reason>` comment on its line or just above.
// The reason is the claim about pacing, such as "an activity follows
// immediately"; it is reviewed where the code is, not in this tool.
//
// The reachability is name-based over the two packages that make up the
// workflow side (pkg/flowstate/v1 and its engine), which over-approximates:
// a method call on a receiver of unknown type reaches every method of that
// name. That is the safe direction, and a false positive is answered by the
// site's own exemption. What it does not see is code in other packages, a call
// through an interface whose implementation is elsewhere, and a charge that is
// computed and then dropped; the last is held by the engine's slice-cost tests.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	list := flag.Bool("sites", false, "list every uncharged site and its status")
	root := flag.String("root", ".", "repository root")
	flag.Parse()

	sites, err := Analyze(*root, "Run")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	bad := Unlabelled(sites)
	if *list {
		for _, s := range sites {
			state := "exempt: " + s.Exempt
			if s.Exempt == "" {
				state = "UNCHARGED"
			}
			fmt.Printf("%s:%d %s %s\n", s.File, s.Line, s.Callee, state)
		}
	}
	for _, s := range bad {
		fmt.Fprintf(os.Stderr, "%s:%d: %s is uncharged and reachable from engine.Run (%s); charge it through a WithCost entry point or add `// %s <reason>`\n",
			s.File, s.Line, s.Callee, strings.Join(s.Path, " -> "), exemptMarker)
	}
	if len(bad) > 0 {
		os.Exit(1)
	}
}

// Unlabelled returns the sites that declared no exemption.
func Unlabelled(sites []Site) []Site {
	var out []Site
	for _, s := range sites {
		if s.Exempt == "" {
			out = append(out, s)
		}
	}
	return out
}
