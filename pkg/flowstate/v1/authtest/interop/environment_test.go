package interop_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest/interop"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentScriptsAndRedacts(t *testing.T) {
	env, err := interop.New(time.Unix(1_700_000_000, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Close()) })
	env.Set(interop.TokenExchange, interop.Script{Responses: []interop.Response{
		interop.JSONResponse(http.StatusBadRequest, map[string]string{"error": "invalid_grant"}),
		interop.JSONResponse(http.StatusOK, map[string]string{"access_token": "result"}),
	}})
	form := make(url.Values)
	form["subject_token"] = []string{"secret"}
	form["scope"] = []string{"read"}
	for _, status := range []int{http.StatusBadRequest, http.StatusOK, http.StatusServiceUnavailable} {
		response, requestErr := env.Do(t.Context(), interop.TokenExchange, http.MethodPost, &form)
		require.NoError(t, requestErr)
		require.Equal(t, status, response.StatusCode)
		response.Body.Close()
	}
	requests := env.Requests()
	require.Len(t, requests, 3)
	require.Empty(t, requests[0].Form.Get("subject_token"))
	require.Equal(t, "read", requests[0].Form.Get("scope"))
}

func TestWorkloadCertificateRotation(t *testing.T) {
	env, err := interop.New(time.Unix(1_700_000_000, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Close()) })
	fetch := func() map[string]string {
		socket := strings.TrimPrefix(env.WorkloadAPIAddr(), "unix://")
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}
		client := &http.Client{Transport: transport}
		response, getErr := client.Get("http://workload/spiffe.workload.SpiffeWorkloadAPI/FetchX509SVID")
		require.NoError(t, getErr)
		defer response.Body.Close()
		var got map[string]string
		require.NoError(t, json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&got))
		return got
	}
	before := fetch()
	require.NoError(t, env.RotateCertificate())
	after := fetch()
	require.NotEqual(t, before["serial"], after["serial"])
	require.Equal(t, "spiffe://flowstate.test/workload", after["spiffe_id"])
}
