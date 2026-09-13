package main

import (
	"maps"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestExamplesREADMEFirstRunCommands executes the short, offline path promised
// to somebody starting from a repository checkout. The marked block is narrow on
// purpose: durable server commands and plugin prerequisites do not belong in a
// unit test, while a stale verb or flag in the first four commands should fail on
// the same command tree a user reaches.
func TestExamplesREADMEFirstRunCommands(t *testing.T) {
	root := "../.."
	data, err := os.ReadFile(root + "/examples/README.md")
	require.NoError(t, err)

	_, section, ok := strings.Cut(string(data), "<!-- first-run-smoke:start -->")
	require.True(t, ok, "examples/README.md lost the first-run smoke start marker")
	section, _, ok = strings.Cut(section, "<!-- first-run-smoke:end -->")
	require.True(t, ok, "examples/README.md lost the first-run smoke end marker")

	const prefix = "$ go run ./cmd/flow "
	var commands [][]string
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "$ ") {
			continue
		}
		require.True(t, strings.HasPrefix(line, prefix),
			"first-run command %q is not runnable from a repository checkout", line)

		invocation := strings.TrimPrefix(line, prefix)
		if before, after, found := strings.Cut(invocation, " | "); found {
			require.Equal(t, "jq -r .name", after,
				"the first-run compile pipeline gained an untested shell command")
			invocation = before
		}
		args := strings.Fields(invocation)
		require.NotEmpty(t, args, "first-run command %q has no flow invocation", line)
		commands = append(commands, args)
	}

	require.Equal(t, 4, len(commands),
		"the first-run block must stay a bounded validate/compile/test/local journey")
	assert.Equal(t, []string{"validate", "compile", "test", "run"}, []string{
		commands[0][0], commands[1][0], commands[2][0], commands[3][0],
	})
	require.GreaterOrEqual(t, len(commands[3]), 2, "the first run must name its execution venue")
	require.Equal(t, "local", commands[3][1], "the first run must remain local and offline")

	t.Chdir(root)
	for _, args := range commands {
		result := runFlow(t, args...)
		require.NoError(t, result.Err, "documented `flow %s` failed:\n%s", strings.Join(args, " "), result.Stderr)
	}
}

// liveExampleHosts are the hosts a Flowfile in the example corpus may name that
// actually answer, each against the reason it is worth a real request.
//
// Everything else has to be reserved for documentation, which is the property
// examples/README.md's closing section turns into advice: an example pointed at
// a live host runs and shows you something, and one pointed at `example.com`
// stops at name resolution and is read, validated and exercised with `flow test`
// instead. A reader chooses between those two, so the difference has to be true.
//
// An addition here is a decision to make a real request from a file people are
// invited to paste and run. `httpbin.org` earns it by being a request echo with
// no state, no account, and no side effect worth causing; a marketing homepage
// fetched to read one status code — which is what `simple-http-multi-step`
// reached for until this test existed — does not.
var liveExampleHosts = map[string]string{
	"httpbin.org": "the request echo the runnable HTTP examples are written against",
}

// offlineExampleHosts spells the loopback interface: a request to one of these
// leaves nothing, so an example naming one is still offline and still honestly
// marked `no`. A definition rather than a permission, which is why the entries
// no example happens to use today stay.
var offlineExampleHosts = map[string]string{
	"localhost": "`conditional-and-retry` dials a closed port on it on purpose",
	"127.0.0.1": "the same, written as an address",
	"::1":       "the same, over IPv6",
}

// reservedForDocumentation reports whether a host is one the DNS does not answer
// for: a name *under* one of the three second-level domains RFC 2606 [§3]
// reserves, or a name in the `.example` top-level domain [§2] reserves.
//
// The three second-level names themselves are deliberately not in it, which is
// the whole subtlety. They are reserved *and* delegated: IANA publishes address
// records for `example.com`, `example.net` and `example.org` and serves a page
// at each, so an example pointed at one of those apexes makes a real request,
// while every subdomain of them is NXDOMAIN. Checked rather than assumed —
// `example.com` resolves here and `api.example.com` does not — and pinned by
// [TestReservedForDocumentationExcludesTheServedApexes], because the difference
// is invisible in the name and is exactly what this predicate exists to decide.
//
// [§2]: https://www.rfc-editor.org/rfc/rfc2606#section-2
// [§3]: https://www.rfc-editor.org/rfc/rfc2606#section-3
func reservedForDocumentation(host string) bool {
	if host == "example" || strings.HasSuffix(host, ".example") {
		return true
	}
	for _, reserved := range []string{"example.com", "example.net", "example.org"} {
		if strings.HasSuffix(host, "."+reserved) {
			return true
		}
	}

	return false
}

// TestReservedForDocumentationExcludesTheServedApexes pins the boundary
// [reservedForDocumentation] turns on, since nothing in the corpus reaches an
// apex today and so nothing else would notice it moving.
func TestReservedForDocumentationExcludesTheServedApexes(t *testing.T) {
	t.Parallel()

	for host, reserved := range map[string]bool{
		// Delegated and served, so a request to one leaves the machine.
		"example.com":     false,
		"example.net":     false,
		"example.org":     false,
		"www.example.com": true,

		// Subdomains of the same three: NXDOMAIN, which is the property the
		// corpus relies on.
		"api.example.com":             true,
		"ledger.internal.example.com": true,
		"flowstate.peer.example.com":  true,

		// The reserved top-level domain, which is not delegated at all.
		"example":     true,
		"foo.example": true,

		// Neither reserved nor ours to permit silently.
		"httpbin.org":      false,
		"microsoft.com":    false,
		"notexample.com":   false,
		"example.com.evil": false,
	} {
		assert.Equal(t, reserved, reservedForDocumentation(host), "reservedForDocumentation(%q)", host)
	}
}

// exampleInventoryNetwork reads the Network column of examples/README.md's
// complete inventory, keyed by the directory each row links.
func exampleInventoryNetwork(t *testing.T, readme string) map[string]bool {
	t.Helper()

	_, inventory, ok := strings.Cut(readme, "## Complete inventory")
	require.True(t, ok, "examples/README.md lost its complete inventory heading")

	row := regexp.MustCompile(`(?m)^\|\s*\[[^\]]+\]\(([^)]+)\)\s*\|.*\|\s*(yes|no)\s*\|\s*$`)
	network := map[string]bool{}
	for _, match := range row.FindAllStringSubmatch(inventory, -1) {
		dir := strings.TrimSuffix(strings.Split(match[1], "#")[0], "/")
		if strings.HasSuffix(dir, ".md") {
			dir = path.Dir(dir)
		}
		network[dir] = match[2] == "yes"
	}
	require.NotEmpty(t, network, "no inventory rows parsed; the row pattern is wrong")

	return network
}

// exampleLiteralHosts returns every host named by a literal string anywhere in a
// compiled workflow — a URL a task will fetch, but equally an issuer written into
// a `signals:` subject, which names an authority and requests nothing.
//
// Over the message rather than over the shapes this package knows about, for the
// reason the charter's own walk gives: a URL sits in a task input here, inside a
// list-of-maps `vars:` entry there, and inside a CEL constant in a third place. A
// walk that named those three would be a fourth place to keep in step.
//
// What it recognises is a string that is entirely an `http(s)` URL, which is every
// spelling the corpus uses. A host written with no scheme is invisible to it, so
// this is a floor on where a live host can hide rather than a proof there is none.
func exampleLiteralHosts(msg protoreflect.Message) []string {
	var hosts []string

	collect := func(value string) {
		if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") {
			return
		}
		parsed, err := url.Parse(value)
		if err != nil || parsed.Hostname() == "" {
			// An expression that builds a URL from parts leaves a hostless
			// prefix behind as a constant. There is no host to judge.
			return
		}
		hosts = append(hosts, parsed.Hostname())
	}

	var walk func(protoreflect.Message)
	walk = func(m protoreflect.Message) {
		m.Range(func(field protoreflect.FieldDescriptor, val protoreflect.Value) bool {
			switch {
			case field.IsMap():
				val.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
					switch field.MapValue().Kind() {
					case protoreflect.MessageKind:
						walk(entry.Message())
					case protoreflect.StringKind:
						collect(entry.String())
					}

					return true
				})
			case field.IsList():
				list := val.List()
				for i := range list.Len() {
					switch field.Kind() {
					case protoreflect.MessageKind:
						walk(list.Get(i).Message())
					case protoreflect.StringKind:
						collect(list.Get(i).String())
					}
				}
			case field.Kind() == protoreflect.MessageKind:
				walk(val.Message())
			case field.Kind() == protoreflect.StringKind:
				collect(val.String())
			}

			return true
		})
	}
	walk(msg)

	return hosts
}

// exampleRequestHosts returns what a run of a compiled workflow would actually
// request: one entry per `http` step, the host of its `url:` where that is a
// literal and [requestHostUnknown] where it is an expression.
//
// The unknown is deliberately not skipped. A `url:` computed at run time is a
// host this test cannot see, and the claim it is checked against — that a row
// marked `no` reaches nothing — is the one where being unable to see is the same
// as not knowing, so it counts as a request and the row has to say `yes`.
func exampleRequestHosts(msg protoreflect.Message) []string {
	var hosts []string

	var walk func(protoreflect.Message)
	walk = func(m protoreflect.Message) {
		if !strings.HasPrefix(string(m.Descriptor().FullName()), "flowstate.v1.") {
			return
		}
		if task, ok := m.Interface().(*v1.Task); ok && task.GetName() == "http" {
			literal := task.GetInputs()["url"].GetLiteral().GetStringValue()
			if parsed, err := url.Parse(literal); err == nil && parsed.Hostname() != "" {
				hosts = append(hosts, parsed.Hostname())
			} else {
				hosts = append(hosts, requestHostUnknown)
			}
		}
		m.Range(func(field protoreflect.FieldDescriptor, val protoreflect.Value) bool {
			switch {
			case field.IsMap():
				if field.MapValue().Kind() == protoreflect.MessageKind {
					val.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
						walk(entry.Message())

						return true
					})
				}
			case field.IsList():
				if field.Kind() == protoreflect.MessageKind {
					list := val.List()
					for i := range list.Len() {
						walk(list.Get(i).Message())
					}
				}
			case field.Kind() == protoreflect.MessageKind:
				walk(val.Message())
			}

			return true
		})
	}
	walk(msg)

	return hosts
}

// requestHostUnknown stands for the host of an `http` step whose `url:` is an
// expression, and so is not decided until the step runs.
const requestHostUnknown = "<computed>"

// TestExamplesREADMENetworkClaims derives what examples/README.md says about the
// network from the corpus it says it about.
//
// Two claims, and each one is advice a reader acts on rather than trivia. The
// first is which hosts the corpus reaches: `httpbin.org` answers, so an example
// pointed at it runs as written, and every other host is reserved for
// documentation, so an example pointed at one stops at name resolution. The
// second is the Network column, which is how a reader finds out which of those
// they are about to run.
//
// Only the `no` direction of the column is derivable, and that is the direction
// worth holding: a row promising a run touches nothing must be telling the
// truth. The converse is not checkable here — a plugin task, a secret backend
// and a `call:` into a plugin file all reach the network with no URL in any
// Flowfile — so a row marked `yes` is a claim this test takes at its word.
//
// The corpus is the charter's, for the charter's reason: `examples/plugins/` and
// `examples/embedding/` name tasks a stock `flow` cannot resolve, so their files
// do not compile in this process at all.
func TestExamplesREADMENetworkClaims(t *testing.T) {
	t.Parallel()

	// Absolute, because five tests in this package call [testing.T.Chdir] and
	// this one runs in parallel with none of them only by the scheduling rule
	// that parallel tests resume after the serial phase. Resolving once removes
	// the dependency rather than relying on it.
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(root, "examples", "README.md"))
	require.NoError(t, err)
	network := exampleInventoryNetwork(t, string(data))

	// The same two globs the charter reads, spelled again because one package's
	// `_test` identifiers are not visible from another. If the charter's globs
	// move, this diverges silently.
	var paths []string
	for _, glob := range [][]string{
		{"examples", "*", "workflow.yaml"},
		{"examples", "*", "workflows", "*.yaml"},
	} {
		matched, globErr := filepath.Glob(filepath.Join(append([]string{root}, glob...)...))
		require.NoError(t, globErr)
		paths = append(paths, matched...)
	}
	require.NotEmpty(t, paths, "no examples found; the globs are wrong")

	named := map[string][]string{}    // example directory -> every host it writes down
	requests := map[string][]string{} // example directory -> the hosts a run of it would fetch
	for _, workflow := range paths {
		example := filepath.Base(filepath.Dir(workflow))
		if example == "workflows" {
			example = filepath.Base(filepath.Dir(filepath.Dir(workflow)))
		}

		wf, _, parseErr := flowfile.ParseFile(workflow)
		require.NoError(t, parseErr, "%s does not compile", workflow)

		for _, host := range exampleLiteralHosts(wf.ProtoReflect()) {
			if !slices.Contains(named[example], host) {
				named[example] = append(named[example], host)
			}
		}
		for _, host := range exampleRequestHosts(wf.ProtoReflect()) {
			if _, offline := offlineExampleHosts[host]; offline {
				continue
			}
			if !slices.Contains(requests[example], host) {
				requests[example] = append(requests[example], host)
			}
		}
	}
	require.NotEmpty(t, named, "no example names any host; the walk found nothing")
	require.NotEmpty(t, requests, "no example requests anything; the walk found nothing")

	for example, hosts := range named {
		for _, host := range hosts {
			if _, offline := offlineExampleHosts[host]; offline {
				continue
			}
			if _, live := liveExampleHosts[host]; live || reservedForDocumentation(host) {
				continue
			}
			t.Errorf("examples/%s names %s, which neither answers by agreement nor is reserved for "+
				"documentation; point it at a host RFC 2606 reserves, or add it to liveExampleHosts "+
				"with the reason a file people paste and run should make a real request to it",
				example, host)
		}
	}

	for example, hosts := range requests {
		hosts = slices.Sorted(slices.Values(hosts))
		marked, listed := network[example]
		if !assert.True(t, listed, "examples/%s requests %v and has no row in the complete inventory", example, hosts) {
			continue
		}
		assert.True(t, marked,
			"examples/%s is marked Network `no`, but a run of it requests %v; either the column is "+
				"wrong or the example stopped being offline", example, hosts)
	}

	// A live host nobody reaches any more is a standing permission for a real
	// request that nothing asked for.
	for host, reason := range liveExampleHosts {
		assert.NotEmpty(t, reason, "%s is allowed with no reason; an entry is a decision, not an entry", host)

		used := slices.ContainsFunc(slices.Collect(maps.Values(requests)), func(hosts []string) bool {
			return slices.Contains(hosts, host)
		})
		assert.True(t, used, "%s is allowed as a live host no example requests; remove the entry", host)
	}
}
