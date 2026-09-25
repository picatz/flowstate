package main

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

func TestOpenDBRefusesAnUnrecognizedEngine(t *testing.T) {
	_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_UNSPECIFIED, "irrelevant", nil)
	if err == nil {
		t.Fatal("openDB(ENGINE_UNSPECIFIED): got no error, want a refusal")
	}
	if !strings.Contains(err.Error(), "sqlite") || !strings.Contains(err.Error(), "postgres") {
		t.Errorf("error does not list this build's supported engines: %v", err)
	}
}

func TestOpenDBRefusesAMalformedPostgresDSN(t *testing.T) {
	oldPolicy := egressPolicy
	egressPolicy, _ = netpolicy.New(netpolicy.WithSchemes("postgres"))
	t.Cleanup(func() { egressPolicy = oldPolicy })

	_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, "not a valid postgres connection string \x00", nil)
	if err == nil {
		t.Fatal("openDB(ENGINE_POSTGRES) with a malformed DSN: got no error, want a refusal")
	}
}

// fakeConn is a minimal net.Conn backed by an in-memory buffer, standing in
// for a real TCP connection so wireBoundConn's own Read logic - the
// enforcement mechanism behind maxWireBytes - can be tested without a
// network, a server, or postgres itself.
type fakeConn struct {
	net.Conn
	r *bytes.Reader
}

func (c *fakeConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *fakeConn) Close() error                { return nil }
func (c *fakeConn) SetDeadline(time.Time) error { return nil }

// TestWireBoundConnRefusesPastItsByteBudget is the postgres-specific
// enforcement of maxWireBytes, proven directly: a connection with more
// bytes available than its budget allows must stop delivering them once
// the budget is spent, the same "refuse rather than silently keep serving
// past the bound" shape every other bound in this plugin has.
func TestWireBoundConnRefusesPastItsByteBudget(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 100)
	conn := &wireBoundConn{
		Conn:      &fakeConn{r: bytes.NewReader(payload)},
		remaining: 40,
	}

	buf := make([]byte, 100)
	total := 0
	var lastErr error
	for {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			lastErr = err
			break
		}
	}

	if total > 40 {
		t.Fatalf("wireBoundConn delivered %d bytes against a 40 byte budget", total)
	}
	if lastErr == nil {
		t.Fatal("wireBoundConn read past its budget without ever refusing")
	}
	if !strings.Contains(lastErr.Error(), "ceiling") {
		t.Errorf("error does not name the bound: %v", lastErr)
	}
}

// TestWireBoundConnAllowsExactlyTheBudget proves the boundary case: a
// connection reading exactly its budget's worth of bytes and no more must
// not spuriously refuse - the same "exactly the bound succeeds" pairing
// bounds_test.go and query_test.go apply to max_rows.
func TestWireBoundConnAllowsExactlyTheBudget(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 40)
	conn := &wireBoundConn{
		Conn:      &fakeConn{r: bytes.NewReader(payload)},
		remaining: 40,
	}

	buf := make([]byte, 40)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("wireBoundConn.Read within budget: unexpected error: %v", err)
	}
	if n != 40 {
		t.Fatalf("wireBoundConn.Read within budget: read %d bytes, want 40", n)
	}
}
