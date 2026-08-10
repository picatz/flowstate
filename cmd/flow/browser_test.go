package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

// A browser, driven over the Chrome DevTools Protocol, with nothing added to
// go.mod.
//
// Why hand-written rather than chromedp or playwright-go. The card is a document
// that has to be *executed* to be tested, and executing it needs a browser and a
// way to reach inside a cross-origin frame: a host cannot script a view it has
// correctly sandboxed, so the DOM query and the click have to arrive from
// outside the page's origin model entirely. That is what a devtools client is
// for. What it does not have to be is a dependency. `flow` is a binary an
// operator installs and this module is that binary's supply chain, so a
// browser-automation library is a large graph to take on for one test file:
// chromedp brings generated protocol bindings and a websocket stack of its own,
// and playwright-go brings a Node driver it downloads at run time, which is a
// network fetch inside a suite whose claim here is hermeticity.
//
// What is actually needed is small: launch a browser, open one websocket to it,
// send JSON with an id, read JSON with that id back. The transport is
// [websocket] from `golang.org/x/net`, already a direct dependency of this
// module and already used by the card's HTML-parsing tests next door, so the
// harness costs zero new modules. The protocol surface it uses is seven methods:
//
//   - Target.setAutoAttach:     see into frames the browser puts in their own process
//   - Target.getTargets:        name those frames, once they have navigated
//   - Page.enable, Runtime.enable
//   - Page.navigate:            load the host double
//   - Page.getFrameTree:        find a frame by URL
//   - Page.createIsolatedWorld: a context inside that frame
//   - Runtime.evaluate:         read the DOM, click a button
//
// Nothing here inspects the network, sets a cookie, or grants a permission: the
// browser is being asked to be a JavaScript engine with a DOM attached, and
// nothing else.

// devtoolsMaxPayloadBytes bounds a single protocol message read into memory.
//
// The peer is a browser this test launched, so this is a guard against a runaway
// rather than against an adversary. Even so, a reader with no bound is a reader
// with no bound, and every other one in this repository has one.
const devtoolsMaxPayloadBytes = 8 << 20

// chromiumCandidates are the places a Chromium-shaped browser is looked for, in
// order.
//
// FLOWSTATE_TEST_CHROMIUM first, so a machine can say where it keeps one without
// editing this list. Playwright's layout next, because that is what this
// repository's development containers have. Then the names a distribution or a
// hosted runner installs. GitHub's ubuntu-latest image ships
// google-chrome-stable, and naming it is the difference between this test
// running in CI and skipping there forever.
func chromiumCandidates() []string {
	var candidates []string

	if named := os.Getenv("FLOWSTATE_TEST_CHROMIUM"); named != "" {
		candidates = append(candidates, named)
	}

	if root := os.Getenv("PLAYWRIGHT_BROWSERS_PATH"); root != "" {
		candidates = append(candidates, playwrightCandidates(root)...)
	}

	for _, name := range []string{
		"chromium",
		"chromium-browser",
		"google-chrome-stable",
		"google-chrome",
	} {
		if found, err := exec.LookPath(name); err == nil {
			candidates = append(candidates, found)
		}
	}

	return candidates
}

// playwrightCandidates are the executables a Playwright browsers directory may
// hold, in the order they are tried.
//
// `<root>/chromium` first, because that is the symlink this repository's
// development containers put there, and a plain path costs nothing to try. It is
// not what `playwright install` writes, though: a real install nests the
// executable under a versioned directory, `chromium-1194/chrome-linux/chrome` on
// Linux and `chrome-linux64` on the builds that use that name, so a lookup that
// only knows the flat path misses every standard install and the test skips on a
// machine that has a browser sitting right there.
//
// The headless shell Playwright installs beside it, `chromium_headless_shell-*`,
// is deliberately not looked at. It speaks the devtools protocol and launches
// happily, and it is still the wrong browser for these tests: the shell does not
// put a sandboxed opaque-origin frame in a target of its own, so the view never
// appears as an attachable target and every test here fails on its own budget
// rather than skipping. Pointing this harness at one produces, three times over:
//
//	no attached target whose URL ends with "/view" ever appeared; the browser
//	has [http://127.0.0.1:46081/?sandbox=http%3A%2F%2F127.0.0.1%3A45635]
//	(context deadline exceeded)
//
// The frame boundary is the thing under test, so a browser that does not give
// the harness a way through it is not a browser these tests can run in.
//
// Any full build present is good enough, so the matches are taken in the order
// [filepath.Glob] returns them rather than sorted by version: this picks a
// browser, it does not pick the newest one.
func playwrightCandidates(root string) []string {
	candidates := []string{filepath.Join(root, "chromium")}

	for _, pattern := range []string{
		filepath.Join(root, "chromium-*", "chrome-linux", "chrome"),
		filepath.Join(root, "chromium-*", "chrome-linux64", "chrome"),
	} {
		// The only error [filepath.Glob] reports is a malformed pattern, and
		// these patterns are literals with one `*` in them. A root that does not
		// exist is simply no matches.
		matches, _ := filepath.Glob(pattern)
		candidates = append(candidates, matches...)
	}

	return candidates
}

// findChromium returns the browser to drive, or the reason there is none.
func findChromium() (string, error) {
	candidates := chromiumCandidates()
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf(
		"no Chromium-shaped browser found; looked at %v. Set FLOWSTATE_TEST_CHROMIUM to one, or "+
			"PLAYWRIGHT_BROWSERS_PATH to a Playwright browsers directory",
		candidates)
}

// browser is one browser process and the one websocket driving it.
type browser struct {
	conn *websocket.Conn

	mu       sync.Mutex
	nextID   int
	waiting  map[int]chan json.RawMessage
	attached map[string]string // target id -> session id
	problems []string
}

// session is one attached target: the page itself, or a frame the browser chose
// to put in a process of its own.
//
// Sessions are flat, which is what makes this cheap: every command carries a
// sessionId and every reply comes back on the same socket, so there is one
// connection, one reader and one correlation table no matter how many processes
// the page turns out to span.
type session struct {
	browser *browser
	id      string
}

// browserProfile makes the directory the browser keeps its profile in, and takes
// responsibility for removing it afterwards.
//
// Deliberately not [testing.T.TempDir], which was the first thing tried and
// which made the browser tests flaky in a way that had nothing to do with the
// card. A browser is not one process: killing the one that was launched leaves
// its zygote, its renderers and its crash handler running for a moment longer,
// and they are still writing into the profile while `TempDir`'s cleanup walks
// it. `RemoveAll` then fails with "directory not empty", and `TempDir` reports
// that as a test failure, so a passing test fails on a race in its own tidying
// up. Removed here instead: retried briefly, and never fatal, because a
// leftover directory under the system temp is a tidiness problem and not a
// wrong answer about the card.
func browserProfile(t *testing.T) string {
	t.Helper()

	profile, err := os.MkdirTemp("", "flowstate-card-browser-")
	require.NoError(t, err)

	// Registered before the cleanup that kills the browser, so it runs after it:
	// cleanups run in reverse.
	t.Cleanup(func() {
		for attempt := 0; attempt < 40; attempt++ {
			if os.RemoveAll(profile) == nil {
				return
			}

			time.Sleep(25 * time.Millisecond)
		}

		t.Logf("the browser profile %s outlived the browser and could not be removed", profile)
	})

	return profile
}

// startBrowser launches a browser and attaches to its first page, or skips the
// test naming the piece that is missing.
//
// The flags are all subtractions. Hermeticity is the important one:
// --host-resolver-rules refuses every name but the loopback the test servers are
// on, and --no-proxy-server keeps the browser off any HTTPS_PROXY the
// environment exports. Without the second, a container that proxies outbound
// traffic would let the browser reach the internet from inside a test whose
// whole claim is that it cannot.
//
// What is *not* here is as deliberate: no --disable-web-security, and no
// --disable-features=IsolateSandboxedIframes. Either would have made the view's
// document reachable from the page and saved the session plumbing below, and
// either would have meant asserting the card's behaviour in a browser configured
// not to enforce the boundary the card is written to live behind.
func startBrowser(t *testing.T, ctx context.Context) *browser {
	t.Helper()

	binary, err := findChromium()
	if err != nil {
		t.Skip("skipping: " + err.Error())
	}

	profile := browserProfile(t)

	cmd := exec.Command(binary,
		"--headless=new",
		"--remote-debugging-port=0",
		"--remote-allow-origins=*",
		"--user-data-dir="+profile,
		// The browser runs as root in this repository's containers, where the
		// setuid sandbox refuses to start. It is driving a test double on
		// loopback with name resolution disabled, and the browser's own sandbox
		// is not one of the properties under test.
		"--no-sandbox",
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-gpu",
		"--disable-sync",
		"--disable-background-networking",
		"--disable-component-update",
		"--disable-default-apps",
		"--metrics-recording-only",
		"--mute-audio",
		"--no-proxy-server",
		"--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1",
		"about:blank",
	)

	require.NoError(t, cmd.Start(), "launching %s", binary)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	port := waitForDevtoolsPort(t, ctx, filepath.Join(profile, "DevToolsActivePort"))
	pageURL := waitForPageTarget(t, ctx, port)

	config, err := websocket.NewConfig(pageURL, "http://127.0.0.1:"+port)
	require.NoError(t, err)

	conn, err := config.DialContext(ctx)
	require.NoError(t, err, "opening a devtools websocket to the browser")
	conn.MaxPayloadBytes = devtoolsMaxPayloadBytes
	t.Cleanup(func() { _ = conn.Close() })

	b := &browser{
		conn:     conn,
		nextID:   1,
		waiting:  map[int]chan json.RawMessage{},
		attached: map[string]string{},
	}
	go b.read()

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}

		// A card that threw is a card that renders nothing, and a DOM assertion
		// reporting "no sections" says nothing about why. The page's own
		// exceptions are the answer, kept always and printed on failure only.
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, problem := range b.problems {
			t.Logf("browser: %s", problem)
		}
	})

	b.page().enable(t, ctx)

	return b
}

// page is the session on the page target itself.
func (b *browser) page() *session { return &session{browser: b} }

// enable turns on the domains this harness uses, and asks to be attached to
// whatever further targets the browser creates underneath this one.
//
// The auto-attach is not optional. A frame sandboxed without allow-same-origin
// gets an opaque origin, and Chromium puts opaque-origin frames in a process of
// their own, so the view does not appear in the page's frame tree at all. It is
// a target, reached through a session, exactly as the browser models it.
func (s *session) enable(t *testing.T, ctx context.Context) {
	t.Helper()

	s.call(t, ctx, "Page.enable", nil)
	s.call(t, ctx, "Runtime.enable", nil)
	s.call(t, ctx, "Target.setAutoAttach", map[string]any{
		"autoAttach":             true,
		"waitForDebuggerOnStart": false,
		"flatten":                true,
	})
}

// waitForDevtoolsPort reads the port the browser chose out of its profile.
//
// The file rather than the "DevTools listening on" line on stderr: the file is
// written once the endpoint is accepting, so reading it is not racing a log line
// against a connect.
func waitForDevtoolsPort(t *testing.T, ctx context.Context, path string) string {
	t.Helper()

	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(lines) >= 2 && lines[0] != "" {
				return lines[0]
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("the browser never wrote %s: %v", path, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// waitForPageTarget returns the websocket URL of the browser's first page.
func waitForPageTarget(t *testing.T, ctx context.Context, port string) string {
	t.Helper()

	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"http://127.0.0.1:"+port+"/json/list", nil)
		require.NoError(t, err)

		response, err := http.DefaultClient.Do(request)
		if err == nil {
			var targets []struct {
				Type                 string `json:"type"`
				WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&targets)
			_ = response.Body.Close()

			if decodeErr == nil {
				for _, target := range targets {
					if target.Type == "page" && target.WebSocketDebuggerURL != "" {
						return target.WebSocketDebuggerURL
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("the browser never offered a page target: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// read demultiplexes the one websocket: replies go to whoever is waiting on
// their id, and the events worth keeping are kept.
func (b *browser) read() {
	for {
		var raw json.RawMessage
		if err := websocket.JSON.Receive(b.conn, &raw); err != nil {
			b.mu.Lock()
			for id, waiter := range b.waiting {
				close(waiter)
				delete(b.waiting, id)
			}
			b.mu.Unlock()

			return
		}

		var envelope struct {
			ID     int             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			continue
		}

		if envelope.ID == 0 {
			b.event(envelope.Method, envelope.Params)

			continue
		}

		b.mu.Lock()
		waiter, ok := b.waiting[envelope.ID]
		delete(b.waiting, envelope.ID)
		b.mu.Unlock()

		if ok {
			waiter <- raw
			close(waiter)
		}
	}
}

// event records the events this harness reads: which session belongs to which
// target, and anything the page complained about.
func (b *browser) event(method string, params json.RawMessage) {
	switch method {
	case "Target.attachedToTarget":
		// By target id rather than by URL. A target is attached the moment it is
		// created, which is before it has navigated anywhere: the URL on this
		// event is empty and only a later Target.targetInfoChanged carries the
		// real one. Keying on the identity that does not change, and asking for
		// the URLs separately, is what makes the lookup below deterministic
		// rather than a race against a second event.
		var attached struct {
			SessionID  string `json:"sessionId"`
			TargetInfo struct {
				TargetID string `json:"targetId"`
			} `json:"targetInfo"`
		}
		if err := json.Unmarshal(params, &attached); err != nil {
			return
		}

		b.mu.Lock()
		b.attached[attached.TargetInfo.TargetID] = attached.SessionID
		b.mu.Unlock()

	case "Runtime.exceptionThrown", "Runtime.consoleAPICalled":
		b.mu.Lock()
		b.problems = append(b.problems, method+" "+string(params))
		b.mu.Unlock()
	}
}

// sessionFor waits for the target whose URL ends with suffix and returns a
// session on it, with the domains this harness uses turned on.
func (b *browser) sessionFor(t *testing.T, ctx context.Context, suffix string) *session {
	t.Helper()

	for {
		raw := b.page().call(t, ctx, "Target.getTargets", nil)

		var targets struct {
			TargetInfos []struct {
				TargetID string `json:"targetId"`
				Type     string `json:"type"`
				URL      string `json:"url"`
			} `json:"targetInfos"`
		}
		require.NoError(t, json.Unmarshal(raw, &targets))

		var known []string
		for _, target := range targets.TargetInfos {
			known = append(known, target.URL)

			if !strings.HasSuffix(target.URL, suffix) {
				continue
			}

			b.mu.Lock()
			id := b.attached[target.TargetID]
			b.mu.Unlock()

			if id == "" {
				continue
			}

			s := &session{browser: b, id: id}
			s.enable(t, ctx)

			return s
		}

		select {
		case <-ctx.Done():
			t.Fatalf("no attached target whose URL ends with %q ever appeared; the browser has %v (%v)",
				suffix, known, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// call sends one command on this session and returns its result, failing the
// test on a protocol error or on the context expiring.
//
// Ids are allocated from one counter for the whole connection rather than per
// session, so a reply can be routed by its id alone whatever session it came
// back on.
func (s *session) call(t *testing.T, ctx context.Context, method string, params map[string]any) json.RawMessage {
	t.Helper()

	b := s.browser

	b.mu.Lock()
	id := b.nextID
	b.nextID++
	waiter := make(chan json.RawMessage, 1)
	b.waiting[id] = waiter
	b.mu.Unlock()

	message := map[string]any{"id": id, "method": method}
	if params != nil {
		message["params"] = params
	}
	if s.id != "" {
		message["sessionId"] = s.id
	}
	require.NoError(t, websocket.JSON.Send(b.conn, message), "sending %s", method)

	select {
	case <-ctx.Done():
		t.Fatalf("%s did not answer before the deadline: %v", method, ctx.Err())
	case raw, ok := <-waiter:
		if !ok {
			t.Fatalf("the browser closed its devtools connection during %s", method)
		}

		var reply struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
				Data    string `json:"data"`
			} `json:"error"`
		}
		require.NoError(t, json.Unmarshal(raw, &reply))
		if reply.Error != nil {
			t.Fatalf("%s failed: %s %s", method, reply.Error.Message, reply.Error.Data)
		}

		return reply.Result
	}

	return nil
}

// evaluate runs an expression in this session and unmarshals its value into out.
//
// contextID zero means the target's own default world; a non-zero one is an
// isolated world created inside a frame. awaitPromise is on so an expression may
// end in a promise, and an exception is a test failure rather than a zero value
// that reads like an answer.
func (s *session) evaluate(t *testing.T, ctx context.Context, contextID int, expression string, out any) {
	t.Helper()

	params := map[string]any{
		"expression":    expression,
		"returnByValue": true,
		"awaitPromise":  true,
	}
	if contextID != 0 {
		params["contextId"] = contextID
	}

	raw := s.call(t, ctx, "Runtime.evaluate", params)

	var reply struct {
		Result struct {
			Value json.RawMessage `json:"value"`
		} `json:"result"`
		ExceptionDetails *struct {
			Text      string `json:"text"`
			Exception struct {
				Description string `json:"description"`
			} `json:"exception"`
		} `json:"exceptionDetails"`
	}
	require.NoError(t, json.Unmarshal(raw, &reply))

	if reply.ExceptionDetails != nil {
		t.Fatalf("evaluating %q threw: %s %s",
			expression, reply.ExceptionDetails.Text, reply.ExceptionDetails.Exception.Description)
	}

	if out == nil {
		return
	}

	if len(reply.Result.Value) == 0 {
		t.Fatalf("evaluating %q produced no value", expression)
	}

	require.NoError(t, json.Unmarshal(reply.Result.Value, out),
		"reading the value of %q", expression)
}

// isolatedWorld returns an execution context inside this session's own frame.
//
// An isolated world, not the document's own: it shares the DOM and shares
// nothing else, so what a query here answers cannot come from a global the
// document happens to have defined, and nothing the harness evaluates can be
// seen by the document it is inspecting.
func (s *session) isolatedWorld(t *testing.T, ctx context.Context) int {
	t.Helper()

	raw := s.call(t, ctx, "Page.getFrameTree", nil)

	var tree struct {
		FrameTree struct {
			Frame struct {
				ID string `json:"id"`
			} `json:"frame"`
		} `json:"frameTree"`
	}
	require.NoError(t, json.Unmarshal(raw, &tree))
	require.NotEmpty(t, tree.FrameTree.Frame.ID, "the attached target reported no frame of its own")

	world := s.call(t, ctx, "Page.createIsolatedWorld", map[string]any{
		"frameId":             tree.FrameTree.Frame.ID,
		"worldName":           "flowstate-host-double-harness",
		"grantUniveralAccess": false,
	})

	var created struct {
		ExecutionContextID int `json:"executionContextId"`
	}
	require.NoError(t, json.Unmarshal(world, &created))
	require.NotZero(t, created.ExecutionContextID)

	return created.ExecutionContextID
}

// until polls an expression until it answers true, or fails naming what never
// happened.
//
// Polling rather than waiting on an event, because what is waited for is a state
// the card reached on its own: a rendered section, a disabled button, a message
// that arrived. The bound is the context; the interval only decides how often the
// question is asked.
func (s *session) until(t *testing.T, ctx context.Context, contextID int, expression, what string) {
	t.Helper()

	for {
		var answer bool
		s.evaluate(t, ctx, contextID, "!!("+expression+")", &answer)
		if answer {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("%s: %q never became true (%v)", what, expression, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}
