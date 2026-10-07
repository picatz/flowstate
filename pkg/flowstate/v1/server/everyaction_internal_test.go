package server

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// everyAction is the list a trusted issuer entry carries when the test is about
// something other than authority. A verified caller with no list holds no
// action, so a test that only needs the call to be admitted names them all.
var everyAction = auth.ActionScopes(v1.AuthorizationActionScopes())
