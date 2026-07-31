package flowstatev1

import (
	"fmt"
	"net/url"
	"strings"
)

// `flow validate` said ok on a workflow this build cannot run:
//
//	$ flow validate w.yaml         # url: ftp://example.com/secret.txt
//	w.yaml: ok
//	$ flow run local w.yaml
//	... denied by egress policy: ftp://... (scheme: "ftp" is not one of http, https)
//
// The obvious repair is to ask the egress policy, which exports
// [netpolicy.Policy.CheckURL] and documents it as "meant for validating a workflow
// definition before it runs". That is the wrong question here, for three reasons
// that are all the same reason: a policy is a property of a *deployment*, and this
// runs in an author's editor.
//
//   - [netpolicy.WithSchemes] can make a policy https-only. Reporting an `http://`
//     URL against it would tell an author their file is wrong on the strength of
//     configuration the machine they are typing on may not share.
//   - [netpolicy.WithAllowPorts] is the same shape for ports.
//   - And CheckURL is not free of I/O. With [netpolicy.WithProxy] configured it
//     reaches `checkProxiedTarget`, which calls `net.DefaultResolver.LookupNetIP` —
//     so a language server would resolve every hostname an author typed, on every
//     keystroke, and block on the resolver's timeouts. (CheckURL's own doc says it
//     applies "every check that does not need a resolved address", which that path
//     makes untrue.)
//
// So the question is not "would this deployment's policy permit it" but "is this a
// URL the http task could ever request". That one is decidable from the file
// alone, is the same answer in every deployment, and needs no policy: the http
// task speaks HTTP, and `ftp://`, `ws://` and `file://` are wrong for it wherever
// it runs.
//
// What that gives up is real and correct to give up. Under an https-only policy an
// `http://` URL is refused at run time and not reported here. That is deployment
// configuration, exactly like a CEL rule, and a validator that reported it would be
// guessing about a machine it cannot see.

// httpTaskSchemes are the two protocols the http task speaks.
//
// Not the policy's allowlist, which may be narrower. This is what the task is, and
// it is what makes the diagnostic below true everywhere.
var httpTaskSchemes = map[string]bool{"http": true, "https": true}

// checkHTTPLiteral is what the http task can say about a literal input before
// anything runs.
//
// Only `url:`, because it is the only input whose value the task can judge without
// a request having happened.
func checkHTTPLiteral(input string, value *Value) error {
	if input != "url" {
		return nil
	}

	raw := value.GetLiteral().GetStringValue()
	if raw == "" {
		// Not a string, or an empty one. The schema's own rules have something to
		// say about both, and a second diagnostic on the same line would answer a
		// question nobody asked.
		return nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		// A URL that will not parse is already reported by the field's `uri` rule,
		// from the source, where there is a position to point at.
		return nil
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" || httpTaskSchemes[scheme] {
		// An empty scheme cannot reach here: a relative reference fails the `uri`
		// rule, and this is asked only of inputs that passed it. Checked anyway
		// because the alternative is a diagnostic saying the http task cannot
		// request "" — and being wrong in a way that reads confidently is the
		// failure mode this whole area keeps producing.
		return nil
	}

	return fmt.Errorf("the http task cannot request a %s:// URL, because it speaks HTTP; "+
		"write an http:// or https:// URL, or use a task that speaks the protocol you want",
		scheme)
}
