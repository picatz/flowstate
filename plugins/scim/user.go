package main

import (
	"cmp"
	"encoding/json"
	"slices"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	scimv1 "github.com/picatz/flowstate/plugins/scim/gen/scim/v1"
)

// userResource is the part of RFC 7643's User this plugin names. Everything
// else in the resource still reaches a workflow, through the `resource` output,
// because an organization's own attributes are exactly what a review joins on
// and this plugin cannot know them.
type userResource struct {
	ID         string `json:"id"`
	ExternalID string `json:"externalId"`
	UserName   string `json:"userName"`
	Name       struct {
		Formatted string `json:"formatted"`
	} `json:"name"`
	DisplayName string `json:"displayName"`

	// Active is a pointer so that a provider omitting it is distinguishable
	// from one sending false. RFC 7643 makes it optional, and a missing value
	// read as "inactive" would tell a review that every user is already off.
	Active *bool `json:"active"`

	Emails []struct {
		Value   string `json:"value"`
		Primary bool   `json:"primary"`
		Type    string `json:"type"`
	} `json:"emails"`

	Groups []struct {
		Value   string `json:"value"`
		Display string `json:"display"`
	} `json:"groups"`

	Meta struct {
		Version string `json:"version"`
	} `json:"meta"`
}

// primaryEmail is the address a notification would go to: the one marked
// primary, or the first one when the provider marks none.
func (u userResource) primaryEmail() string {
	for _, email := range u.Emails {
		if email.Primary && email.Value != "" {
			return email.Value
		}
	}
	for _, email := range u.Emails {
		if email.Value != "" {
			return email.Value
		}
	}
	return ""
}

// groupNames is what the provider says this user belongs to, bounded and
// deduplicated, preferring the display name a person would recognize.
func (u userResource) groupNames() []string {
	names := make([]string, 0, min(len(u.Groups), maxGroups))
	for _, group := range u.Groups {
		if len(names) == maxGroups {
			break
		}
		name := group.Display
		if name == "" {
			name = group.Value
		}
		if name == "" || slices.Contains(names, name) {
			continue
		}
		names = append(names, truncate(name, 256))
	}
	return names
}

// displayName falls back the way a directory does: the display name, then the
// formatted name, then the user name, so a workflow rendering a review item
// always has something to show.
func (u userResource) display() string {
	return cmp.Or(u.DisplayName, u.Name.Formatted, u.UserName)
}

// isActive reads the tri-state honestly: a provider that omits `active` is
// telling this plugin nothing, and RFC 7643's default for a user that exists is
// that it is usable, so an omitted value reads as active rather than as off.
func (u userResource) isActive() bool {
	return u.Active == nil || *u.Active
}

// outputsFor renders one user as the task's declared outputs.
func outputsFor(user userResource, raw json.RawMessage, etag string) (*scimv1.UserGetOutputs, error) {
	resource, err := literalOf(raw)
	if err != nil {
		return nil, err
	}

	return &scimv1.UserGetOutputs{
		Id:           user.ID,
		UserName:     user.UserName,
		DisplayName:  user.display(),
		Active:       user.isActive(),
		PrimaryEmail: user.primaryEmail(),
		ExternalId:   user.ExternalID,
		Version:      cmp.Or(user.Meta.Version, etag),
		Groups:       user.groupNames(),
		Resource:     resource,
	}, nil
}

// summaryValue renders one user as a map for the list task, carrying the same
// attributes user_get names so a workflow reads one shape either way.
func summaryValue(user userResource) *expr.Value {
	return sdk.Literal(map[string]any{
		"id":            user.ID,
		"user_name":     user.UserName,
		"display_name":  user.display(),
		"active":        user.isActive(),
		"primary_email": user.primaryEmail(),
		"external_id":   user.ExternalID,
		"version":       user.Meta.Version,
		"groups":        user.groupNames(),
	})
}

// literalOf turns a provider's own JSON into a value of unconstrained shape, so
// the attributes this plugin does not name still reach the workflow.
func literalOf(raw json.RawMessage) (*expr.Value, error) {
	if len(raw) == 0 {
		return sdk.Literal(nil), nil
	}

	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, sdk.Failed("the provider returned a user this plugin cannot read as JSON")
	}
	return sdk.Literal(document), nil
}
