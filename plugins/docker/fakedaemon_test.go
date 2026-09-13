package main

import (
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeDaemon is an Engine API server on a Unix socket, in this process.
//
// It is a real HTTP server on a real socket rather than a stub at the client's
// boundary, because what this plugin has to get right is the request it sends:
// the create body is where every claim in doc.go is either true or not, and a
// fake that accepted a struct would not be reading the JSON a daemon reads.
type fakeDaemon struct {
	socket string
	t      *testing.T

	mu sync.Mutex

	// created is the create request the plugin sent, decoded.
	created containerConfig

	// containers records the identifiers created, started and removed, so a
	// test can prove cleanup happened.
	created_ids []string
	started     []string
	removed     []string

	// exitCode is what wait answers with, and stdout/stderr what the log stream
	// carries.
	exitCode int64
	stdout   string
	stderr   string

	// waitBlocks makes wait hang, for the timeout case.
	waitBlocks chan struct{}

	// createStatus and startStatus override the successful answers.
	createStatus int
	startStatus  int

	// errorBody is the daemon's own error message for an overridden status.
	errorBody string
}

// newFakeDaemon starts one on a socket in a temporary directory.
func newFakeDaemon(t *testing.T) *fakeDaemon {
	t.Helper()

	// A short path: a Unix socket has about a hundred bytes to work with, and
	// t.TempDir()'s own name is most of that on some platforms.
	dir := t.TempDir()
	socket := filepath.Join(dir, "d.sock")

	daemon := &fakeDaemon{
		socket:       socket,
		t:            t,
		stdout:       "ok\n",
		createStatus: http.StatusCreated,
		startStatus:  http.StatusNoContent,
	}

	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening on a unix socket: %v", err)
	}

	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: http.HandlerFunc(daemon.serve)},
	}
	server.Start()
	t.Cleanup(server.Close)

	return daemon
}

// grant is the daemon grant pointing at this fake.
func (f *fakeDaemon) grant() daemonGrant {
	return daemonGrant{Socket: f.socket}
}

// createRequest is what the plugin asked for.
func (f *fakeDaemon) createRequest() containerConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created
}

// removals is how many containers were removed.
func (f *fakeDaemon) removals() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.removed)
}

// starts is how many containers were started.
func (f *fakeDaemon) starts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.started)
}

func (f *fakeDaemon) serve(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/containers/create"):
		f.serveCreate(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/start"):
		f.serveStart(w, path)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/wait"):
		f.serveWait(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/logs"):
		f.serveLogs(w)
	case r.Method == http.MethodDelete:
		f.serveRemove(w, path)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeDaemon) serveCreate(w http.ResponseWriter, r *http.Request) {
	var config containerConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.created = config
	f.created_ids = append(f.created_ids, "container-1")
	status, message := f.createStatus, f.errorBody
	f.mu.Unlock()

	if status != http.StatusCreated {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"message":"` + message + `"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"Id":"container-1","Warnings":[]}`))
}

func (f *fakeDaemon) serveStart(w http.ResponseWriter, path string) {
	f.mu.Lock()
	f.started = append(f.started, path)
	status, message := f.startStatus, f.errorBody
	f.mu.Unlock()

	if status != http.StatusNoContent {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"message":"` + message + `"}`))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeDaemon) serveWait(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	blocks, code := f.waitBlocks, f.exitCode
	f.mu.Unlock()

	if blocks != nil {
		select {
		case <-blocks:
		case <-r.Context().Done():
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(waitResponse{StatusCode: code})
}

// serveLogs writes the daemon's own multiplexed framing, which is the format
// this plugin has to demultiplex.
func (f *fakeDaemon) serveLogs(w http.ResponseWriter) {
	f.mu.Lock()
	stdout, stderr := f.stdout, f.stderr
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/vnd.docker.raw-stream")
	writeFrame(w, streamStdout, stdout)
	writeFrame(w, streamStderr, stderr)
}

func (f *fakeDaemon) serveRemove(w http.ResponseWriter, path string) {
	f.mu.Lock()
	f.removed = append(f.removed, path)
	f.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

// writeFrame writes one multiplexed frame.
func writeFrame(w http.ResponseWriter, stream byte, payload string) {
	if payload == "" {
		return
	}

	header := make([]byte, frameHeaderBytes)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	_, _ = w.Write(header)
	_, _ = w.Write([]byte(payload))
}
