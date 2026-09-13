package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

const (
	// maxDaemonResponseBytes bounds an ordinary JSON response from the daemon -
	// a create, a wait, an inspect. Log streams are bounded separately, by the
	// run grant.
	maxDaemonResponseBytes = 1 << 20

	// maxErrorBytes bounds what the daemon's own error text may contribute to a
	// failure this plugin reports.
	maxErrorBytes = 512
)

// daemon is the container runtime this plugin talks to.
//
// It is deliberately a small HTTP client over the Engine API rather than the
// Docker SDK: the requests this plugin makes are five, every field it sets is
// one the contract in doc.go names, and a dependency that could set the others
// is a dependency whose next version might.
type daemon struct {
	http    *http.Client
	base    string
	version string
}

// newDaemon builds the client for one grant.
func newDaemon(grant daemonGrant) (*daemon, error) {
	version := grant.apiVersion()

	if grant.Socket != "" {
		// A socket is a file, and the operator named it. Nothing about this
		// path is HTTP, so the host in the URL is a placeholder the transport
		// never resolves.
		transport := &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", grant.Socket)
			},
			DisableCompression: true,
		}
		return &daemon{
			http:    &http.Client{Transport: transport},
			base:    "http://docker/" + version,
			version: version,
		}, nil
	}

	tlsConfig, err := daemonTLS(grant)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
	}
	return &daemon{
		http:    &http.Client{Transport: transport},
		base:    "https://" + grant.Address + "/" + version,
		version: version,
	}, nil
}

// daemonTLS builds the client authentication for a remote daemon.
//
// Every field is required by the grant's own check, and the minimum version is
// stated here rather than left to the default so that an old Go release and a
// new one agree about what this connection is.
func daemonTLS(grant daemonGrant) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(grant.TLSCertFile, grant.TLSKeyFile)
	if err != nil {
		return nil, sdk.Failed("the daemon grant's client certificate could not be loaded: %v", err)
	}

	authority, err := os.ReadFile(grant.TLSCAFile)
	if err != nil {
		return nil, sdk.Failed("the daemon grant's tls_ca_file could not be read: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(authority) {
		return nil, sdk.Failed("the daemon grant's tls_ca_file holds no certificates")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// containerConfig is the create request, holding only the fields this plugin
// sets. What is absent is as deliberate as what is present: no Privileged, no
// CapAdd, no Devices, no PidMode, no IpcMode, no host networking.
type containerConfig struct {
	Image       string            `json:"Image"`
	Cmd         []string          `json:"Cmd,omitempty"`
	Env         []string          `json:"Env,omitempty"`
	User        string            `json:"User"`
	WorkingDir  string            `json:"WorkingDir,omitempty"`
	Tty         bool              `json:"Tty"`
	AttachStdin bool              `json:"AttachStdin"`
	OpenStdin   bool              `json:"OpenStdin"`
	Labels      map[string]string `json:"Labels,omitempty"`
	StopTimeout int               `json:"StopTimeout,omitempty"`
	HostConfig  hostConfig        `json:"HostConfig"`
}

// hostConfig is the half of a create request that decides what the container
// may reach.
type hostConfig struct {
	NetworkMode    string        `json:"NetworkMode"`
	ReadonlyRootfs bool          `json:"ReadonlyRootfs"`
	AutoRemove     bool          `json:"AutoRemove"`
	Privileged     bool          `json:"Privileged"`
	CapDrop        []string      `json:"CapDrop"`
	SecurityOpt    []string      `json:"SecurityOpt"`
	Memory         int64         `json:"Memory"`
	NanoCpus       int64         `json:"NanoCpus"`
	PidsLimit      *int64        `json:"PidsLimit,omitempty"`
	Mounts         []mountConfig `json:"Mounts,omitempty"`
}

// mountConfig is one bind mount, resolved from a mount grant.
type mountConfig struct {
	Type     string `json:"Type"`
	Source   string `json:"Source"`
	Target   string `json:"Target"`
	ReadOnly bool   `json:"ReadOnly"`
}

// createResponse is what the daemon answers a create with.
type createResponse struct {
	ID       string   `json:"Id"`
	Warnings []string `json:"Warnings"`
}

// waitResponse is what the daemon answers a wait with.
type waitResponse struct {
	StatusCode int64 `json:"StatusCode"`
	Error      *struct {
		Message string `json:"Message"`
	} `json:"Error"`
}

// errorResponse is the daemon's own error shape.
type errorResponse struct {
	Message string `json:"message"`
}

// create asks the daemon for a container and returns its identifier.
func (d *daemon) create(ctx context.Context, config containerConfig) (string, error) {
	body, err := json.Marshal(config)
	if err != nil {
		return "", sdk.Failed("encoding the create request: %v", err)
	}

	response, raw, err := d.do(ctx, http.MethodPost, "/containers/create", nil, body, maxDaemonResponseBytes)
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusCreated {
		return "", classifyDaemonStatus(response, raw, "creating a container")
	}

	var created createResponse
	if err := json.Unmarshal(raw, &created); err != nil || created.ID == "" {
		return "", sdk.Failed("the daemon created a container and returned no identifier")
	}
	return created.ID, nil
}

// start runs a created container.
func (d *daemon) start(ctx context.Context, id string) error {
	response, raw, err := d.do(ctx, http.MethodPost, "/containers/"+id+"/start", nil, nil, maxDaemonResponseBytes)
	if err != nil {
		return err
	}
	switch response.StatusCode {
	case http.StatusNoContent, http.StatusNotModified:
		return nil
	default:
		return classifyDaemonStatus(response, raw, "starting a container")
	}
}

// wait blocks until the container is no longer running and returns its status.
func (d *daemon) wait(ctx context.Context, id string) (int32, error) {
	query := url.Values{"condition": []string{"not-running"}}

	response, raw, err := d.do(ctx, http.MethodPost, "/containers/"+id+"/wait", query, nil, maxDaemonResponseBytes)
	if err != nil {
		return 0, err
	}
	if response.StatusCode != http.StatusOK {
		return 0, classifyDaemonStatus(response, raw, "waiting for a container")
	}

	var waited waitResponse
	if err := json.Unmarshal(raw, &waited); err != nil {
		return 0, sdk.Failed("the daemon returned a wait response this plugin cannot read")
	}
	if waited.Error != nil && waited.Error.Message != "" {
		return 0, sdk.OutcomeUnknown(
			"the daemon reported an error while waiting for the container (%s); it may have run, so it is not retried automatically",
			truncate(waited.Error.Message, maxErrorBytes))
	}
	return int32(waited.StatusCode), nil
}

// logs reads the container's output, demultiplexed and bounded.
func (d *daemon) logs(ctx context.Context, id string, limit int64) (stdout, stderr string, truncated bool, err error) {
	query := url.Values{
		"stdout": []string{"1"},
		"stderr": []string{"1"},
	}

	request, err := d.request(ctx, http.MethodGet, "/containers/"+id+"/logs", query, nil)
	if err != nil {
		return "", "", false, err
	}

	response, err := d.http.Do(request)
	if err != nil {
		return "", "", false, sdk.Unavailable("reading the container's output: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, maxDaemonResponseBytes))
		return "", "", false, classifyDaemonStatus(response, raw, "reading a container's output")
	}

	return demultiplex(response.Body, limit)
}

// remove stops and deletes a container, with its anonymous volumes.
//
// It takes its own context so that cleanup happens even when the call's context
// is already cancelled - a container left running because the workflow was
// cancelled is the failure mode #1348 names.
func (d *daemon) remove(id string, grace time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	query := url.Values{
		"force": []string{"1"},
		"v":     []string{"1"},
	}

	response, raw, err := d.do(ctx, http.MethodDelete, "/containers/"+id, query, nil, maxDaemonResponseBytes)
	if err != nil {
		return err
	}
	switch response.StatusCode {
	case http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return classifyDaemonStatus(response, raw, "removing a container")
	}
}

// do performs one request and reads a bounded response.
func (d *daemon) do(ctx context.Context, method, path string, query url.Values, body []byte, limit int64) (*http.Response, []byte, error) {
	request, err := d.request(ctx, method, path, query, body)
	if err != nil {
		return nil, nil, err
	}

	response, err := d.http.Do(request)
	if err != nil {
		return nil, nil, classifyTransport(method, path, err)
	}
	defer response.Body.Close()

	raw, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if readErr != nil {
		return nil, nil, sdk.Unavailable("reading the daemon's response: %v", readErr)
	}
	if int64(len(raw)) > limit {
		return nil, nil, sdk.Failed("the daemon returned more than the %d-byte limit this plugin reads", limit)
	}
	return response, raw, nil
}

// request builds one Engine API request.
func (d *daemon) request(ctx context.Context, method, path string, query url.Values, body []byte) (*http.Request, error) {
	endpoint := d.base + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint, payload)
	if err != nil {
		return nil, sdk.Failed("building a daemon request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

// classifyTransport turns a failure reaching the daemon into a classification.
//
// Whether this is safe to retry depends on what was being done, and the caller
// knows: reaching the daemon at all fails before anything ran, while a request
// that was already in flight may have created or started a container. The
// method and path are enough to tell those apart, and a create that may have
// happened is reconciled by the caller's own cleanup rather than by a retry.
func classifyTransport(method, path string, err error) error {
	if method == http.MethodPost && (path == "/containers/create" || len(path) > len("/containers/")) {
		return sdk.OutcomeUnknown(
			"the connection to the container runtime failed during %s %s; a container may exist, so it is not retried automatically",
			method, truncate(path, 128))
	}
	return sdk.Unavailable("the container runtime could not be reached: %v", err)
}

// classifyDaemonStatus turns the daemon's own refusal into a classification.
func classifyDaemonStatus(response *http.Response, raw []byte, doing string) error {
	message := ""
	var parsed errorResponse
	if err := json.Unmarshal(raw, &parsed); err == nil && parsed.Message != "" {
		message = " (" + truncate(parsed.Message, maxErrorBytes) + ")"
	}

	switch response.StatusCode {
	case http.StatusNotFound:
		return sdk.NotFound("the container runtime has no such object while %s%s", doing, message)
	case http.StatusConflict:
		return sdk.Conflict("the container runtime reports a conflict while %s%s", doing, message)
	case http.StatusBadRequest:
		return sdk.InvalidInput("the container runtime refused the request while %s%s", doing, message)
	case http.StatusForbidden, http.StatusUnauthorized:
		return sdk.PermissionDenied("the container runtime refused this worker while %s%s", doing, message)
	}
	if response.StatusCode >= 500 {
		return sdk.Unavailable("the container runtime returned HTTP %d while %s%s", response.StatusCode, doing, message)
	}
	return sdk.Failed("the container runtime returned HTTP %d while %s%s", response.StatusCode, doing, message)
}

// containerLabels are what this plugin stamps on every container it creates, so
// an operator looking at a host can tell which containers are a workflow's and
// which run they belong to.
func containerLabels(run string) map[string]string {
	return map[string]string{
		"io.flowstate.plugin": "docker",
		"io.flowstate.run":    truncate(run, 64),
	}
}
