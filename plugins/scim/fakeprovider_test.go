package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// fakeProvider is enough of RFC 7644 to exercise what this plugin depends on:
// bearer auth, a user by id, a filtered query, a ListResponse envelope with
// paging, PATCH with and without a body, and If-Match.
//
// It records the filter it was sent, which is how the escaping test proves a
// user name never becomes filter syntax.
type fakeProvider struct {
	server *httptest.Server
	t      *testing.T

	// token is the credential it accepts.
	token string

	// users are the resources it serves, keyed by id.
	users map[string]map[string]any

	// lastFilter is the filter parameter of the most recent query.
	lastFilter string

	// filterMatches, when set, is what a query returns regardless of the
	// filter - so a test can drive the "two users for one name" case a real
	// provider should never produce.
	filterMatches []string

	// patchStatus is what PATCH answers with; 200 returns the updated
	// resource, 204 returns nothing, and anything else is a refusal.
	patchStatus int

	// version is the ETag served and required.
	version string

	// patches counts writes, so a test can prove a no-op review made none.
	patches int
}

func newFakeProvider(t *testing.T) *fakeProvider {
	t.Helper()

	provider := &fakeProvider{
		t:           t,
		token:       "not-a-real-directory-token",
		users:       map[string]map[string]any{},
		patchStatus: http.StatusOK,
		version:     `W/"1"`,
	}
	provider.addUser("2819c223", "alice@example.com", true)
	provider.server = httptest.NewServer(http.HandlerFunc(provider.serve))
	t.Cleanup(provider.server.Close)

	return provider
}

// addUser stores one user resource.
func (f *fakeProvider) addUser(id, userName string, active bool) {
	f.users[id] = map[string]any{
		"schemas":    []string{"urn:ietf:params:scim:schemas:core:2.0:User"},
		"id":         id,
		"externalId": "hr-" + id,
		"userName":   userName,
		"name":       map[string]any{"formatted": "Alice Example"},
		"active":     active,
		"emails": []map[string]any{
			{"value": "alt@example.com", "type": "work"},
			{"value": userName, "primary": true, "type": "work"},
		},
		"groups": []map[string]any{
			{"value": "g-1", "display": "engineering"},
			{"value": "g-2", "display": "oncall"},
		},
		"meta": map[string]any{"resourceType": "User", "version": f.version},
	}
}

// baseURL is what an input names.
func (f *fakeProvider) baseURL() string { return f.server.URL + "/scim/v2" }

// client is the plugin's own client wired to this server, over a policy that
// permits loopback the way an operator's policy permits a provider.
func (f *fakeProvider) client(t *testing.T) *client {
	t.Helper()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxResponseBytes(8<<20),
		netpolicy.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("building the test egress policy: %v", err)
	}

	parsed, err := url.Parse(f.baseURL())
	if err != nil {
		t.Fatalf("parsing the fake provider's URL: %v", err)
	}
	return &client{http: policy.Client(), base: parsed, token: f.token}
}

func (f *fakeProvider) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"schemas":["urn:ietf:params:scim:api:messages:2.0:Error"],"status":"401","detail":"invalid token"}`))
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/scim/v2")
	switch {
	case r.Method == http.MethodGet && path == "/Users":
		f.serveQuery(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/Users/"):
		f.serveUser(w, strings.TrimPrefix(path, "/Users/"))
	case r.Method == http.MethodPatch && strings.HasPrefix(path, "/Users/"):
		f.servePatch(w, r, strings.TrimPrefix(path, "/Users/"))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeProvider) serveUser(w http.ResponseWriter, rawID string) {
	id, err := url.PathUnescape(rawID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	user, ok := f.users[id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"schemas":["urn:ietf:params:scim:api:messages:2.0:Error"],"status":"404","detail":"Resource not found"}`))
		return
	}

	w.Header().Set("Content-Type", scimContentType)
	w.Header().Set("ETag", f.version)
	_ = json.NewEncoder(w).Encode(user)
}

func (f *fakeProvider) serveQuery(w http.ResponseWriter, r *http.Request) {
	f.lastFilter = r.URL.Query().Get("filter")

	count, _ := strconv.Atoi(r.URL.Query().Get("count"))
	startIndex, _ := strconv.Atoi(r.URL.Query().Get("startIndex"))
	if startIndex < 1 {
		startIndex = 1
	}

	// Which users match: the ids a test pinned, or an honest evaluation of the
	// one filter shape this fake understands.
	matched := f.filterMatches
	if matched == nil {
		matched = f.evaluate(f.lastFilter)
	}

	resources := make([]map[string]any, 0, len(matched))
	for i, id := range matched {
		if i+1 < startIndex {
			continue
		}
		if count > 0 && len(resources) == count {
			break
		}
		if user, ok := f.users[id]; ok {
			resources = append(resources, user)
		}
	}

	w.Header().Set("Content-Type", scimContentType)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"schemas":      []string{"urn:ietf:params:scim:api:messages:2.0:ListResponse"},
		"totalResults": len(matched),
		"itemsPerPage": len(resources),
		"startIndex":   startIndex,
		"Resources":    resources,
	})
}

// evaluate understands `userName eq "value"` and an empty filter, which is
// every shape this plugin builds itself.
func (f *fakeProvider) evaluate(filter string) []string {
	ids := make([]string, 0, len(f.users))
	if filter == "" {
		for id := range f.users {
			ids = append(ids, id)
		}
		return sortedStrings(ids)
	}

	const prefix = `userName eq "`
	if !strings.HasPrefix(filter, prefix) || !strings.HasSuffix(filter, `"`) {
		return nil
	}
	literal := strings.TrimSuffix(strings.TrimPrefix(filter, prefix), `"`)
	wanted := strings.NewReplacer(`\"`, `"`, `\\`, `\`).Replace(literal)

	for id, user := range f.users {
		if user["userName"] == wanted {
			ids = append(ids, id)
		}
	}
	return sortedStrings(ids)
}

func (f *fakeProvider) servePatch(w http.ResponseWriter, r *http.Request, rawID string) {
	f.patches++

	id, err := url.PathUnescape(rawID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	user, ok := f.users[id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if match := r.Header.Get("If-Match"); match != "" && match != f.version {
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = w.Write([]byte(`{"schemas":["urn:ietf:params:scim:api:messages:2.0:Error"],"status":"412","detail":"resource has changed"}`))
		return
	}

	var request struct {
		Schemas    []string `json:"schemas"`
		Operations []struct {
			Op    string `json:"op"`
			Path  string `json:"path"`
			Value any    `json:"value"`
		} `json:"Operations"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(request.Schemas) != 1 || request.Schemas[0] != patchOpSchema {
		f.t.Errorf("PATCH body carries schemas %v, want the PatchOp schema RFC 7644 requires", request.Schemas)
	}

	for _, operation := range request.Operations {
		if operation.Op == "replace" && operation.Path == "active" {
			user["active"] = operation.Value
		}
	}

	if f.patchStatus != http.StatusOK {
		w.WriteHeader(f.patchStatus)
		return
	}
	w.Header().Set("Content-Type", scimContentType)
	w.Header().Set("ETag", f.version)
	_ = json.NewEncoder(w).Encode(user)
}

// sortedStrings keeps the fake's answers stable across map iteration.
func sortedStrings(values []string) []string {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
	return values
}

// isNotFound and friends read a classification without matching message text.
func isNotFound(err error) bool { return sdk.IsNotFound(err) }
