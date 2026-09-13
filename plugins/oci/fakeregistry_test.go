package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	ociv1 "github.com/picatz/flowstate/plugins/oci/gen/oci/v1"
)

// fakeRegistry speaks enough of the distribution specification to exercise the
// protocol this plugin depends on: the token challenge and exchange, manifests
// by tag and by digest, indexes with platforms, the referrers API, and blobs.
//
// It is deliberately a real HTTP server rather than a stubbed client. What this
// plugin does that an http step cannot is the protocol - the 401, the realm,
// the retry, the hash of what came back - and a fake that answered at the
// client boundary would skip exactly the part worth testing.
type fakeRegistry struct {
	server *httptest.Server
	host   string

	// requireAuth makes every read challenge first, which is what a private
	// repository does and what the token dance exists for.
	requireAuth bool

	// username and password are what the token endpoint accepts when
	// requireAuth is set.
	username string
	password string

	// manifests maps "repository:tag" and "repository@digest" to bytes.
	manifests map[string]fakeContent

	// blobs maps "repository@digest" to bytes.
	blobs map[string]fakeContent

	// referrers maps "repository@digest" to the index answered for it.
	referrers map[string]fakeContent

	// tokenRequests counts exchanges, so a test can prove a token was reused
	// rather than re-minted per request.
	tokenRequests int

	// basicSeen records whether a credential ever crossed on the registry API
	// itself rather than at the token endpoint.
	basicSeen bool
}

// fakeContent is one served document.
type fakeContent struct {
	mediaType string
	body      []byte

	// corrupt serves bytes that do not match the digest they are addressed by,
	// which is the case content addressing exists to catch.
	corrupt bool
}

func newFakeRegistry(t *testing.T) *fakeRegistry {
	t.Helper()

	registry := &fakeRegistry{
		manifests: map[string]fakeContent{},
		blobs:     map[string]fakeContent{},
		referrers: map[string]fakeContent{},
	}
	registry.server = httptest.NewServer(http.HandlerFunc(registry.serve))
	t.Cleanup(registry.server.Close)

	parsed, err := url.Parse(registry.server.URL)
	if err != nil {
		t.Fatalf("parsing the fake registry's URL: %v", err)
	}
	registry.host = parsed.Host

	// One image, one tag, one index, one attestation, so most tests need no
	// setup of their own.
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"sha256:` + strings.Repeat("c", 64) + `"}}`)
	registry.addManifest("app", "1.0", "application/vnd.oci.image.manifest.v1+json", manifest)

	return registry
}

// digestOfBytes is the fake's own hashing, kept separate from the plugin's so a
// test never proves a function against itself.
func digestOfBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// addManifest serves a manifest under a tag and under its digest.
func (f *fakeRegistry) addManifest(repository, tag, mediaType string, body []byte) string {
	digest := digestOfBytes(body)
	content := fakeContent{mediaType: mediaType, body: body}
	if tag != "" {
		f.manifests[repository+":"+tag] = content
	}
	f.manifests[repository+"@"+digest] = content
	return digest
}

// addBlob serves a blob under its own digest.
func (f *fakeRegistry) addBlob(repository, mediaType string, body []byte) string {
	digest := digestOfBytes(body)
	f.blobs[repository+"@"+digest] = fakeContent{mediaType: mediaType, body: body}
	return digest
}

// addCorruptBlob serves bytes under a digest they do not hash to.
func (f *fakeRegistry) addCorruptBlob(repository, digest string, body []byte) {
	f.blobs[repository+"@"+digest] = fakeContent{mediaType: "application/json", body: body, corrupt: true}
}

// addReferrers serves a referrers index for a digest.
func (f *fakeRegistry) addReferrers(repository, digest string, index []byte) {
	f.referrers[repository+"@"+digest] = fakeContent{mediaType: "application/vnd.oci.image.index.v1+json", body: index}
}

// reference renders a tagged reference pointing at this server.
func (f *fakeRegistry) reference(repository, tag string) string {
	return f.host + "/" + repository + ":" + tag
}

// pinned renders a digest-pinned reference pointing at this server.
func (f *fakeRegistry) pinned(repository, digest string) string {
	return f.host + "/" + repository + "@" + digest
}

// client is the plugin's own client wired to this server, over a policy that
// permits loopback the way an operator's policy permits a registry.
func (f *fakeRegistry) client(t *testing.T, creds credentials) *registryClient {
	t.Helper()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxResponseBytes(16<<20),
		netpolicy.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("building the test egress policy: %v", err)
	}

	return &registryClient{http: policy.Client(), creds: creds, scheme: "http", tokens: map[string]string{}}
}

// serve is the registry API.
func (f *fakeRegistry) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/token" {
		f.serveToken(w, r)
		return
	}

	if r.Header.Get("Authorization") != "" && strings.HasPrefix(r.Header.Get("Authorization"), "Basic ") {
		f.basicSeen = true
	}

	if f.requireAuth && r.Header.Get("Authorization") != "Bearer minted-for-tests" {
		w.Header().Set("WWW-Authenticate",
			`Bearer realm="`+f.server.URL+`/token",service="fake",scope="repository:app:pull"`)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}`))
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/v2/")
	switch {
	case strings.Contains(path, "/manifests/"):
		repository, target, _ := strings.Cut(path, "/manifests/")
		f.serveContent(w, f.manifests, key(repository, target))
	case strings.Contains(path, "/blobs/"):
		repository, target, _ := strings.Cut(path, "/blobs/")
		f.serveContent(w, f.blobs, repository+"@"+target)
	case strings.Contains(path, "/referrers/"):
		repository, target, _ := strings.Cut(path, "/referrers/")
		f.serveContent(w, f.referrers, repository+"@"+target)
	default:
		http.NotFound(w, r)
	}
}

// serveToken is the realm the challenge names.
func (f *fakeRegistry) serveToken(w http.ResponseWriter, r *http.Request) {
	f.tokenRequests++

	if f.requireAuth {
		username, password, ok := r.BasicAuth()
		if !ok || username != f.username || password != f.password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"token":"minted-for-tests"}`))
}

// serveContent answers one stored document.
func (f *fakeRegistry) serveContent(w http.ResponseWriter, store map[string]fakeContent, lookup string) {
	content, ok := store[lookup]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errors":[{"code":"MANIFEST_UNKNOWN","message":"not found"}]}`))
		return
	}

	w.Header().Set("Content-Type", content.mediaType)
	w.Header().Set("Docker-Content-Digest", digestOfBytes(content.body))
	_, _ = w.Write(content.body)
}

// key maps a manifest request onto the fake's own storage key: a digest target
// is stored under @, a tag under :.
func key(repository, target string) string {
	if strings.HasPrefix(target, "sha256:") {
		return repository + "@" + target
	}
	return repository + ":" + target
}

// indexWith renders a multi-platform index over the given platforms, plus the
// unknown/unknown entry real indexes carry for attestations.
func indexWith(platforms ...string) []byte {
	type entry struct {
		MediaType string            `json:"mediaType"`
		Digest    string            `json:"digest"`
		Size      int64             `json:"size"`
		Platform  map[string]string `json:"platform"`
	}

	manifests := make([]entry, 0, len(platforms)+1)
	for i, p := range platforms {
		parts := strings.Split(p, "/")
		platform := map[string]string{"os": parts[0], "architecture": parts[1]}
		if len(parts) == 3 {
			platform["variant"] = parts[2]
		}
		manifests = append(manifests, entry{
			MediaType: "application/vnd.oci.image.manifest.v1+json",
			Digest:    "sha256:" + strings.Repeat(fmt.Sprintf("%x", i+1), 64),
			Size:      int64(100 + i),
			Platform:  platform,
		})
	}
	manifests = append(manifests, entry{
		MediaType: "application/vnd.oci.image.manifest.v1+json",
		Digest:    "sha256:" + strings.Repeat("f", 64),
		Size:      99,
		Platform:  map[string]string{"os": "unknown", "architecture": "unknown"},
	})

	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     "application/vnd.oci.image.index.v1+json",
		"manifests":     manifests,
	})
	if err != nil {
		panic(err)
	}
	return body
}

// resolveInputs builds the task's inputs the way the host delivers them.
func resolveInputs(reference, platform, username string) *ociv1.ResolveInputs {
	return &ociv1.ResolveInputs{Reference: reference, Platform: platform, Username: username}
}

// inputsFor renders a task's input message as the named values a task function
// receives, which is what the host does before it calls one.
func inputsFor(t *testing.T, message proto.Message) map[string]*flowstatev1.Value {
	t.Helper()

	outputs, err := sdk.EncodeOutputs(message)
	if err != nil {
		t.Fatalf("encoding task inputs: %v", err)
	}
	return outputs.GetNamedValues()
}

// isPermissionDenied reports whether an error carries the SDK's
// permission-denied classification, which is how a refusal is distinguished
// from a failure without matching on message text.
func isPermissionDenied(err error) bool { return sdk.IsPermissionDenied(err) }

// valueMap reads one referrer out of the task's output.
func valueMap(t *testing.T, value *expr.Value) map[string]*expr.Value {
	t.Helper()

	entries := value.GetMapValue().GetEntries()
	if len(entries) == 0 {
		t.Fatalf("expected a descriptor map, got %v", value)
	}

	out := make(map[string]*expr.Value, len(entries))
	for _, entry := range entries {
		out[entry.GetKey().GetStringValue()] = entry.GetValue()
	}
	return out
}

// errIsNotFound reports the not-found classification, for the same reason
// [isPermissionDenied] exists.
func errIsNotFound(err error) bool { return sdk.IsNotFound(err) }
