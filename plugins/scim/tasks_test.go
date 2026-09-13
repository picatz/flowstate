package main

import (
	"net/http"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// TestUserGetByIdReadsTheAttributesAReviewActsOn covers the ordinary read,
// including the two the RFC leaves room to get wrong: the primary email among
// several, and the whole resource surviving for attributes this plugin does not
// name.
func TestUserGetByIdReadsTheAttributesAReviewActsOn(t *testing.T) {
	provider := newFakeProvider(t)

	out, err := getByID(t.Context(), provider.client(t), "2819c223")
	if err != nil {
		t.Fatalf("getByID: %v", err)
	}

	if out.GetId() != "2819c223" || out.GetUserName() != "alice@example.com" {
		t.Errorf("id = %q user_name = %q", out.GetId(), out.GetUserName())
	}
	if !out.GetActive() {
		t.Error("active = false for a user the provider says is active")
	}
	if out.GetPrimaryEmail() != "alice@example.com" {
		t.Errorf("primary_email = %q, want the address marked primary rather than the first listed", out.GetPrimaryEmail())
	}
	if out.GetExternalId() != "hr-2819c223" {
		t.Errorf("external_id = %q, which is what joins this user to a system of record", out.GetExternalId())
	}
	if got := out.GetGroups(); len(got) != 2 || got[0] != "engineering" {
		t.Errorf("groups = %v", got)
	}
	if out.GetVersion() == "" {
		t.Error("version is empty, so a later write cannot be conditional on this read")
	}
	if out.GetResource().GetMapValue() == nil {
		t.Error("the whole resource did not survive, so an organization's own attributes are unreachable")
	}
}

// TestAMissingActiveAttributeReadsAsActive is the tri-state the RFC leaves
// optional. Reading an omitted `active` as false would tell a review that every
// user at a provider that omits it is already deactivated.
func TestAMissingActiveAttributeReadsAsActive(t *testing.T) {
	provider := newFakeProvider(t)
	delete(provider.users["2819c223"], "active")

	out, err := getByID(t.Context(), provider.client(t), "2819c223")
	if err != nil {
		t.Fatalf("getByID: %v", err)
	}
	if !out.GetActive() {
		t.Error("a user whose provider omits `active` was read as deactivated")
	}
}

// TestAUserNameIsEscapedIntoTheFilterRatherThanInterpolated is this plugin's
// injection boundary, and the reason user_name is not a filter input: a name
// holding a quote must not be able to end the literal and change which users
// the expression selects.
func TestAUserNameIsEscapedIntoTheFilterRatherThanInterpolated(t *testing.T) {
	provider := newFakeProvider(t)
	provider.addUser("evil", `a" or userName pr or "x`, true)

	// The lookup finds exactly the user whose name is that whole string,
	// because the quote was escaped rather than closing the literal.
	out, err := getByUserName(t.Context(), provider.client(t), `a" or userName pr or "x`)
	if err != nil {
		t.Fatalf("getByUserName: %v", err)
	}
	if out.GetId() != "evil" {
		t.Errorf("id = %q, want the user whose name is the whole string", out.GetId())
	}
	if !strings.Contains(provider.lastFilter, `\"`) {
		t.Errorf("the filter sent was %q, which does not escape the quote in the name", provider.lastFilter)
	}
}

// TestAControlCharacterInAUserNameIsRefused: a SCIM filter literal is a JSON
// string, which cannot carry one, and a user name holding one is not a user
// name.
func TestAControlCharacterInAUserNameIsRefused(t *testing.T) {
	if _, err := userNameFilter("alice\nBearer: other"); err == nil {
		t.Error("a user name holding a newline was accepted into a filter")
	} else if !sdk.IsInvalidInput(err) {
		t.Errorf("error is %v, want invalid input", err)
	}
}

// TestTwoUsersForOneNameIsRefusedRatherThanPicked. RFC 7643 makes userName
// unique; a provider returning two is describing a directory this plugin cannot
// choose from, and choosing anyway is how a review deactivates the wrong
// account.
func TestTwoUsersForOneNameIsRefusedRatherThanPicked(t *testing.T) {
	provider := newFakeProvider(t)
	provider.addUser("second", "alice@example.com", true)
	provider.filterMatches = []string{"2819c223", "second"}

	_, err := getByUserName(t.Context(), provider.client(t), "alice@example.com")
	if err == nil {
		t.Fatal("an ambiguous name resolved to one of the matches")
	}
	if !strings.Contains(err.Error(), "will not choose between them") {
		t.Errorf("error = %v", err)
	}
}

// TestAMissingUserIsNotFound keeps a provider's 404 distinguishable, because a
// review gating on "this account still exists" depends on the difference.
func TestAMissingUserIsNotFound(t *testing.T) {
	provider := newFakeProvider(t)

	if _, err := getByID(t.Context(), provider.client(t), "nobody"); !isNotFound(err) {
		t.Errorf("error = %v, want not-found", err)
	}
	if _, err := getByUserName(t.Context(), provider.client(t), "nobody@example.com"); !isNotFound(err) {
		t.Errorf("error = %v, want not-found", err)
	}
}

// TestListPagesThroughTheDirectory covers the cursor contract a workflow loops
// on: next_start_index is the next page's one-based start, and zero exactly
// when there is nothing after this page.
func TestListPagesThroughTheDirectory(t *testing.T) {
	provider := newFakeProvider(t)
	provider.addUser("b", "bob@example.com", true)
	provider.addUser("c", "carol@example.com", false)

	first, err := listUsers(t.Context(), provider.client(t), "", 2, 1)
	if err != nil {
		t.Fatalf("listUsers: %v", err)
	}
	if len(first.GetUsers()) != 2 || first.GetTotalResults() != 3 {
		t.Fatalf("first page has %d users of %d total", len(first.GetUsers()), first.GetTotalResults())
	}
	if first.GetNextStartIndex() != 3 {
		t.Errorf("next_start_index = %d, want 3", first.GetNextStartIndex())
	}

	second, err := listUsers(t.Context(), provider.client(t), "", 2, int(first.GetNextStartIndex()))
	if err != nil {
		t.Fatalf("listUsers: %v", err)
	}
	if len(second.GetUsers()) != 1 {
		t.Fatalf("second page has %d users, want the remaining one", len(second.GetUsers()))
	}
	if second.GetNextStartIndex() != 0 {
		t.Errorf("next_start_index = %d after the last page, want 0 so a loop terminates", second.GetNextStartIndex())
	}
}

// TestAProviderThatIgnoresCountCannotWriteMoreThanWasAskedFor: the count is a
// bound on what enters durable history, not a hint to a cooperative provider.
func TestAProviderThatIgnoresCountCannotWriteMoreThanWasAskedFor(t *testing.T) {
	provider := newFakeProvider(t)
	provider.addUser("b", "bob@example.com", true)
	provider.addUser("c", "carol@example.com", false)
	// The fake honours count; this drives the case where it does not by asking
	// for one and pinning three matches with no count applied server-side.
	provider.filterMatches = []string{"2819c223", "b", "c"}

	page, err := listUsers(t.Context(), provider.client(t), "", 1, 1)
	if err != nil {
		t.Fatalf("listUsers: %v", err)
	}
	if len(page.GetUsers()) != 1 {
		t.Errorf("the page carries %d users for a count of 1", len(page.GetUsers()))
	}
}

// TestListCountIsRefusedRatherThanLowered, for the reason every other ceiling
// in this tree is.
func TestListCountIsRefusedRatherThanLowered(t *testing.T) {
	if _, err := boundedCount(maxListCount + 1); err == nil {
		t.Error("a count over the ceiling was accepted")
	}
	if got, err := boundedCount(0); err != nil || got != defaultListCount {
		t.Errorf("boundedCount(0) = %d, %v", got, err)
	}
	if got, err := boundedStartIndex(0); err != nil || got != 1 {
		t.Errorf("boundedStartIndex(0) = %d, want SCIM's one-based beginning", got)
	}
}

// TestDeactivateTurnsTheAccountOff is the write, end to end.
func TestDeactivateTurnsTheAccountOff(t *testing.T) {
	provider := newFakeProvider(t)

	out, err := deactivate(t.Context(), provider.client(t), "2819c223", "")
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if out.GetActive() || out.GetAlreadyInactive() {
		t.Errorf("active = %v already_inactive = %v", out.GetActive(), out.GetAlreadyInactive())
	}
	if provider.users["2819c223"]["active"] != false {
		t.Error("the provider's user is still active")
	}
}

// TestDeactivatingAnInactiveAccountWritesNothing keeps a review that runs twice
// from producing two writes an auditor has to reconcile - and reports which of
// the two happened rather than making the auditor infer it.
func TestDeactivatingAnInactiveAccountWritesNothing(t *testing.T) {
	provider := newFakeProvider(t)
	provider.users["2819c223"]["active"] = false

	out, err := deactivate(t.Context(), provider.client(t), "2819c223", "")
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if !out.GetAlreadyInactive() {
		t.Error("already_inactive = false for an account that was already off")
	}
	if provider.patches != 0 {
		t.Errorf("%d writes were made against an account that was already off", provider.patches)
	}
}

// TestADeactivationConditionedOnAStaleReadIsAConflict is why expected_version
// exists: a user modified since the reviewer looked must not be overwritten on
// the strength of what the reviewer saw.
func TestADeactivationConditionedOnAStaleReadIsAConflict(t *testing.T) {
	provider := newFakeProvider(t)

	_, err := deactivate(t.Context(), provider.client(t), "2819c223", `W/"stale"`)
	if err == nil {
		t.Fatal("a write conditioned on a stale version was applied")
	}
	if !sdk.IsConflict(err) {
		t.Errorf("error is %v, want the conflict classification a Flowfile can dispatch on", err)
	}
}

// TestADeactivationAnsweredWithNoContentIsReadBack: RFC 7644 lets a provider
// answer PATCH with 204 and no body, which says the write applied and says
// nothing about the resource - so the resource is read rather than asserted.
func TestADeactivationAnsweredWithNoContentIsReadBack(t *testing.T) {
	provider := newFakeProvider(t)
	provider.patchStatus = http.StatusNoContent

	out, err := deactivate(t.Context(), provider.client(t), "2819c223", "")
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if out.GetActive() {
		t.Error("active = true after a deactivation the provider accepted")
	}
}

// TestAProviderThatAcceptsTheWriteAndLeavesTheUserActiveFails is the honest
// direction: an audit record saying an account was turned off, when it is on,
// is worse than a failure.
func TestAProviderThatAcceptsTheWriteAndLeavesTheUserActiveFails(t *testing.T) {
	provider := newFakeProvider(t)
	provider.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			w.Header().Set("Content-Type", scimContentType)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"2819c223","userName":"alice@example.com","active":true}`))
			return
		}
		provider.serve(w, r)
	})

	_, err := deactivate(t.Context(), provider.client(t), "2819c223", "")
	if err == nil {
		t.Fatal("a provider that left the account active reported success")
	}
	if !strings.Contains(err.Error(), "was not turned off") {
		t.Errorf("error = %v", err)
	}
}

// TestARefusedCredentialIsPermanent: retrying a rejected token re-sends the
// same token, so it must not be classified as a backend that could not be
// reached.
func TestARefusedCredentialIsPermanent(t *testing.T) {
	provider := newFakeProvider(t)
	client := provider.client(t)
	client.token = "wrong"

	_, err := getByID(t.Context(), client, "2819c223")
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied", err)
	}
}
