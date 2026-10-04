package gates

import (
	"bytes"
	"errors"
	"io"
	"net/http"
)

// maxAPIResponseBytes bounds one API answer the page reads back. The API
// already bounds what it returns; this is the page's own limit on a handler it
// does not control, so a misbehaving one cannot grow this process's memory.
const maxAPIResponseBytes = 8 << 20

var errAPIResponseTooLarge = errors.New("gates: the API answered with more than the page will read")

// outerRequest carries the browser's request to the loopback transport, so the
// API handler sees the peer address and TLS state of the connection the person
// made rather than of nothing. A verifier that binds an identity to a client
// certificate, and a failure log that names the peer, both read those.
type outerRequest struct{}

// loopback is an [http.RoundTripper] that serves a request by calling an
// [http.Handler] in this process. It lets the page be a Connect client of the
// handler it is mounted beside without a socket, a second listener, or a
// second copy of the handler's authentication.
type loopback struct{ handler http.Handler }

// RoundTrip implements [http.RoundTripper].
func (l loopback) RoundTrip(req *http.Request) (*http.Response, error) {
	served := req.Clone(req.Context())
	if outer, ok := req.Context().Value(outerRequest{}).(*http.Request); ok {
		served.RemoteAddr = outer.RemoteAddr
		served.TLS = outer.TLS
	}
	if served.Body == nil {
		served.Body = http.NoBody
	}

	rec := &recorder{header: http.Header{}, status: http.StatusOK}
	l.handler.ServeHTTP(rec, served)
	if rec.overflow {
		return nil, errAPIResponseTooLarge
	}

	return &http.Response{
		Status:        http.StatusText(rec.status),
		StatusCode:    rec.status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        rec.header,
		Body:          io.NopCloser(bytes.NewReader(rec.body.Bytes())),
		ContentLength: int64(rec.body.Len()),
		Request:       req,
	}, nil
}

// recorder is the [http.ResponseWriter] a loopback request is answered into. It
// buffers rather than streams because the page only calls unary methods.
type recorder struct {
	header      http.Header
	body        bytes.Buffer
	status      int
	wroteHeader bool
	overflow    bool
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = status
}

func (r *recorder) Write(p []byte) (int, error) {
	r.WriteHeader(http.StatusOK)
	if r.body.Len()+len(p) > maxAPIResponseBytes {
		r.overflow = true
		return 0, errAPIResponseTooLarge
	}

	return r.body.Write(p)
}
