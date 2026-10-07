package server_test

import (
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// everyAction is the list a trusted issuer entry carries when the test is about
// something other than authority. A verified caller with no list holds no
// action, so a test that only needs the call admitted names them all.
var everyAction = auth.ActionScopes(v1.AuthorizationActionScopes())

// ordinaryActions is every action an entry holds without being told to
// disclose anything: the RPC actions, and not the ones that gate what is shown
// (workload.reveal_sensitive, payload.decode, payload.encode).
func ordinaryActions() auth.ActionScopes {
	return slices.DeleteFunc(slices.Clone(everyAction), func(scope string) bool {
		return slices.Contains([]string{"workload.reveal_sensitive", "payload.decode", "payload.encode"}, scope)
	})
}
