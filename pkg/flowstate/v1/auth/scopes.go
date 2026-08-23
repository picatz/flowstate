package auth

import (
	"fmt"
	"slices"
	"strings"
)

// Action is a policy decision shared by every serving surface. OAuth scopes
// are a wire representation of these actions, not a second permission model.
type Action string

const (
	ActionWorkflowsRead   Action = "workflows.read"
	ActionWorkflowsWrite  Action = "workflows.write"
	ActionWorkflowsSignal Action = "workflows.signal"
	ActionSchedulesRead   Action = "schedules.read"
	ActionSchedulesWrite  Action = "schedules.write"
	ActionMCPToolsCall    Action = "mcp.tools.call"
)

// AuthorizationActions is the stable, ordered action vocabulary. New actions
// may be appended; published spellings must never be repurposed.
var AuthorizationActions = []Action{
	ActionWorkflowsRead, ActionWorkflowsWrite, ActionWorkflowsSignal,
	ActionSchedulesRead, ActionSchedulesWrite, ActionMCPToolsCall,
}

// ScopeForAction derives the OAuth spelling from the shared action. Keeping
// the prefix here prevents policy and discovery documents drifting apart.
func ScopeForAction(action Action) (string, error) {
	if !slices.Contains(AuthorizationActions, action) {
		return "", fmt.Errorf("unknown authorization action %q", action)
	}
	return "flowstate:" + string(action), nil
}

// SupportedScopes returns a fresh, deterministic list suitable for OAuth
// metadata. Callers may mutate it safely.
func SupportedScopes() []string {
	result := make([]string, 0, len(AuthorizationActions))
	for _, action := range AuthorizationActions {
		scope, _ := ScopeForAction(action)
		result = append(result, scope)
	}
	return result
}

// OAuthChallenge renders an RFC 6750/9449 challenge. requiredScope must only
// be supplied after authorization has established that disclosing it cannot
// reveal a resource the caller is not allowed to know exists.
func OAuthChallenge(dpop bool, errorCode, metadataURL, requiredScope string) string {
	scheme := "Bearer"
	if dpop {
		scheme = "DPoP"
	}
	parts := make([]string, 0, 3)
	if errorCode != "" {
		parts = append(parts, `error="`+escapeChallenge(errorCode)+`"`)
	}
	if metadataURL != "" {
		parts = append(parts, `resource_metadata="`+escapeChallenge(metadataURL)+`"`)
	}
	if requiredScope != "" {
		parts = append(parts, `scope="`+escapeChallenge(requiredScope)+`"`)
	}
	if len(parts) == 0 {
		return scheme
	}
	return scheme + " " + strings.Join(parts, ", ")
}

func escapeChallenge(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\"", "\\\"").Replace(s)
}
