package oauthclient

import (
	"fmt"
	"net/http"
)

type transport struct {
	client  *Client
	profile Profile
	request Request
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("oauthclient: nil request")
	}
	cred, err := t.client.credential(req.Context(), t.profile, t.request)
	if err != nil {
		return nil, err
	}
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	clone.Header.Set("Authorization", cred.tokenType+" "+cred.accessToken)
	if t.profile.DPoP {
		if err := t.client.signer.SignRequest(req.Context(), clone, t.request.ProofKey); err != nil {
			return nil, fmt.Errorf("oauthclient: sign proof: %w", err)
		}
	}
	return t.client.base.RoundTrip(clone)
}
