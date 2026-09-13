package main

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	scimv1 "github.com/picatz/flowstate/plugins/scim/gen/scim/v1"
)

const (
	// defaultListCount is what a page with no count asked for returns.
	defaultListCount = 50

	// maxListCount is the ceiling a workflow cannot raise past: each user
	// becomes a map in durable history, so this bounds what one step writes
	// there as much as what it reads.
	maxListCount = 500

	// patchOpSchema is the schema URI RFC 7644 section 3.5.2 requires on a
	// PATCH request. Providers reject a body without it.
	patchOpSchema = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
)

// listResponse is RFC 7644's ListResponse, the envelope a query returns.
type listResponse struct {
	TotalResults int64             `json:"totalResults"`
	ItemsPerPage int               `json:"itemsPerPage"`
	StartIndex   int               `json:"startIndex"`
	Resources    []json.RawMessage `json:"Resources"`
}

func scimUserGet(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("scim.user_get has no usable egress policy, so no provider is authorized: %v", egressRefusal)
	}

	var in scimv1.UserGetInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	token, err := tokenFrom(in.GetToken())
	if err != nil {
		return nil, err
	}

	// Exactly one, because they are two ways to name one user and accepting
	// both would leave which one decided up to this plugin rather than to the
	// author.
	switch {
	case in.GetId() != "" && in.GetUserName() != "":
		return nil, sdk.InvalidInput("id and user_name both name a user; give one")
	case in.GetId() == "" && in.GetUserName() == "":
		return nil, sdk.InvalidInput("give id, the provider's own identifier, or user_name to look one up by name")
	}

	client, err := newClient(in.GetBaseUrl(), token)
	if err != nil {
		return nil, err
	}

	var out *scimv1.UserGetOutputs
	if in.GetId() != "" {
		out, err = getByID(ctx, client, in.GetId())
	} else {
		out, err = getByUserName(ctx, client, in.GetUserName())
	}
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// getByID reads /Users/{id}, the addressed form.
func getByID(ctx context.Context, client *client, rawID string) (*scimv1.UserGetOutputs, error) {
	id, err := identifier(rawID, "id")
	if err != nil {
		return nil, err
	}

	response, body, err := client.do(ctx, http.MethodGet, "/Users/"+pathEscape(id), nil, nil, maxResourceBytes, nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, classifyStatus(client.base.Host, "reading a user", response, body)
	}

	var user userResource
	if err := json.Unmarshal(body, &user); err != nil {
		return nil, sdk.Failed("%s returned a user resource this plugin cannot read", client.base.Host)
	}
	return outputsFor(user, body, response.Header.Get("ETag"))
}

// getByUserName queries /Users with an exact-match filter this plugin builds,
// and refuses an answer that is not exactly one user.
//
// More than one match is a refusal rather than a first-match: userName is
// unique in RFC 7643, so a provider returning two is describing a directory
// this plugin cannot safely pick from, and picking anyway is how a review
// deactivates the wrong account.
func getByUserName(ctx context.Context, client *client, rawUserName string) (*scimv1.UserGetOutputs, error) {
	userName, err := identifier(rawUserName, "user_name")
	if err != nil {
		return nil, err
	}
	filter, err := userNameFilter(userName)
	if err != nil {
		return nil, err
	}

	query := url.Values{}
	query.Set("filter", filter)
	query.Set("count", "2")

	response, body, err := client.do(ctx, http.MethodGet, "/Users", query, nil, maxResourceBytes, nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, classifyStatus(client.base.Host, "looking a user up by name", response, body)
	}

	var page listResponse
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, sdk.Failed("%s returned a list response this plugin cannot read", client.base.Host)
	}

	switch len(page.Resources) {
	case 0:
		return nil, sdk.NotFound("%s has no user named %s", client.base.Host, truncate(userName, 128))
	case 1:
	default:
		return nil, sdk.Failed(
			"%s returned %d users for the name %s, which RFC 7643 makes unique; this plugin will not choose between them",
			client.base.Host, len(page.Resources), truncate(userName, 128))
	}

	var user userResource
	if err := json.Unmarshal(page.Resources[0], &user); err != nil {
		return nil, sdk.Failed("%s returned a user resource this plugin cannot read", client.base.Host)
	}
	return outputsFor(user, page.Resources[0], "")
}

func scimUserList(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("scim.user_list has no usable egress policy, so no provider is authorized: %v", egressRefusal)
	}

	var in scimv1.UserListInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	token, err := tokenFrom(in.GetToken())
	if err != nil {
		return nil, err
	}
	filter, err := checkFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}
	count, err := boundedCount(in.GetCount())
	if err != nil {
		return nil, err
	}
	startIndex, err := boundedStartIndex(in.GetStartIndex())
	if err != nil {
		return nil, err
	}

	client, err := newClient(in.GetBaseUrl(), token)
	if err != nil {
		return nil, err
	}

	out, err := listUsers(ctx, client, filter, count, startIndex)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// listUsers reads one page.
func listUsers(ctx context.Context, client *client, filter string, count, startIndex int) (*scimv1.UserListOutputs, error) {
	query := url.Values{}
	query.Set("count", strconv.Itoa(count))
	query.Set("startIndex", strconv.Itoa(startIndex))
	if filter != "" {
		query.Set("filter", filter)
	}

	response, body, err := client.do(ctx, http.MethodGet, "/Users", query, nil, maxListBytes, nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, classifyStatus(client.base.Host, "listing users", response, body)
	}

	var page listResponse
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, sdk.Failed("%s returned a list response this plugin cannot read", client.base.Host)
	}
	if len(page.Resources) > count {
		// A provider that ignored `count` would otherwise write more into
		// history than the workflow asked to read.
		page.Resources = page.Resources[:count]
	}

	users := make([]*expr.Value, 0, len(page.Resources))
	for _, raw := range page.Resources {
		var user userResource
		if err := json.Unmarshal(raw, &user); err != nil {
			return nil, sdk.Failed("%s returned a user resource this plugin cannot read", client.base.Host)
		}
		users = append(users, summaryValue(user))
	}

	// SCIM's cursor is one-based and its pages are described by totalResults,
	// so the next page starts after what this one covered - and the answer is
	// zero, not an index past the end, when there is nothing after it. A
	// workflow loops while next_start_index is not zero.
	next := 0
	if consumed := startIndex + len(page.Resources) - 1; page.TotalResults > int64(consumed) && len(page.Resources) > 0 {
		next = consumed + 1
	}

	return &scimv1.UserListOutputs{
		Users:          users,
		TotalResults:   page.TotalResults,
		NextStartIndex: int32(next),
	}, nil
}

func scimUserDeactivate(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied("scim.user_deactivate has no usable egress policy, so no provider is authorized: %v", egressRefusal)
	}

	var in scimv1.UserDeactivateInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	token, err := tokenFrom(in.GetToken())
	if err != nil {
		return nil, err
	}
	id, err := identifier(in.GetId(), "id")
	if err != nil {
		return nil, err
	}

	client, err := newClient(in.GetBaseUrl(), token)
	if err != nil {
		return nil, err
	}

	out, err := deactivate(ctx, client, id, in.GetExpectedVersion())
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

// deactivate reads the user, then turns the account off if it is not already.
//
// The read first is what makes already_inactive honest, and it is also what
// keeps this task from writing at all when there is nothing to change: an
// access review that runs twice should produce one change and one no-op, not
// two writes an auditor has to reconcile.
func deactivate(ctx context.Context, client *client, id, expectedVersion string) (*scimv1.UserDeactivateOutputs, error) {
	current, err := getByID(ctx, client, id)
	if err != nil {
		return nil, err
	}
	if !current.GetActive() {
		return &scimv1.UserDeactivateOutputs{
			Id:              current.GetId(),
			Active:          false,
			AlreadyInactive: true,
			Version:         current.GetVersion(),
		}, nil
	}

	body := map[string]any{
		"schemas": []string{patchOpSchema},
		"Operations": []map[string]any{{
			"op":    "replace",
			"path":  "active",
			"value": false,
		}},
	}

	response, raw, err := client.do(ctx, http.MethodPatch, "/Users/"+pathEscape(id), nil, body, maxResourceBytes,
		fmtHeaders("If-Match", expectedVersion))
	if err != nil {
		return nil, err
	}

	switch response.StatusCode {
	case http.StatusOK:
		var user userResource
		if err := json.Unmarshal(raw, &user); err != nil {
			return nil, sdk.Failed("%s returned a user resource this plugin cannot read", client.base.Host)
		}
		if user.isActive() {
			return nil, sdk.Failed(
				"%s accepted the deactivation and returned the user still active; the account was not turned off", client.base.Host)
		}
		return &scimv1.UserDeactivateOutputs{
			Id:      cmp.Or(user.ID, id),
			Active:  false,
			Version: cmp.Or(user.Meta.Version, response.Header.Get("ETag")),
		}, nil

	case http.StatusNoContent:
		// RFC 7644 lets a provider answer a PATCH with 204 and no body. The
		// write is known to have applied; what the resource looks like now is
		// not, so it is read back rather than asserted.
		after, err := getByID(ctx, client, id)
		if err != nil {
			return nil, err
		}
		if after.GetActive() {
			return nil, sdk.Failed(
				"%s accepted the deactivation and the user is still active; the account was not turned off", client.base.Host)
		}
		return &scimv1.UserDeactivateOutputs{Id: after.GetId(), Active: false, Version: after.GetVersion()}, nil
	}

	return nil, classifyStatus(client.base.Host, "deactivating a user", response, raw)
}

// boundedCount applies the page ceiling, refusing rather than lowering: a
// silently clamped count would answer "five hundred users" for a directory with
// more, indistinguishably from one with exactly five hundred.
func boundedCount(requested int32) (int, error) {
	switch {
	case requested < 0:
		return 0, sdk.InvalidInput("count is negative")
	case requested == 0:
		return defaultListCount, nil
	case requested > maxListCount:
		return 0, sdk.InvalidInput("count is %d, over this task's ceiling of %d", requested, maxListCount)
	default:
		return int(requested), nil
	}
}

// boundedStartIndex reads SCIM's one-based cursor, where zero means "the
// beginning" because a Flowfile leaving an input unset sends zero.
func boundedStartIndex(requested int32) (int, error) {
	if requested < 0 {
		return 0, sdk.InvalidInput("start_index is negative; SCIM's cursor is one-based")
	}
	if requested == 0 {
		return 1, nil
	}
	return int(requested), nil
}
