package main

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

func withPostgresPolicy(t *testing.T, policy *netpolicy.Policy, lookup func(context.Context, string) ([]string, error)) {
	t.Helper()
	oldPolicy, oldLookup := egressPolicy, lookupPostgresHost
	egressPolicy, lookupPostgresHost = policy, lookup
	t.Cleanup(func() {
		egressPolicy, lookupPostgresHost = oldPolicy, oldLookup
	})
}

func postgresDSN(host string, port int) string {
	return "postgres://app:containment-secret@" + net.JoinHostPort(host, strconv.Itoa(port)) + "/app?sslmode=verify-full"
}

// The listener is the mutation oracle: with the policy check removed or moved
// after Dial, this test observes an accepted connection. The fixed path refuses
// during governed resolution, before a socket can be opened.
func TestPostgresDeniedBeforeDial(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
			accepted <- struct{}{}
		}
	}()

	policy, err := netpolicy.New(netpolicy.WithSchemes("postgres"))
	if err != nil {
		t.Fatal(err)
	}
	withPostgresPolicy(t, policy, func(context.Context, string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	})

	port := listener.Addr().(*net.TCPAddr).Port
	_, err = openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, postgresDSN("database.example", port), secrets.NewScrubber())
	if err == nil || !strings.Contains(err.Error(), "denied by deployment egress policy") {
		t.Fatalf("openDB error = %v, want policy denial", err)
	}
	select {
	case <-accepted:
		t.Fatal("unauthorized postgres listener accepted a connection; policy ran after dial or was bypassed")
	case <-time.After(100 * time.Millisecond):
	}
}

// This is the positive half using the same real DialFunc as database/sql. It
// also serves as the mutation control for TestPostgresDeniedBeforeDial: allowing
// loopback makes the listener observe the connection.
func TestPostgresAllowedTargetUsesGovernedDial(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
			accepted <- struct{}{}
		}
	}()

	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	policy, err := netpolicy.New(
		netpolicy.WithSchemes("postgres"),
		netpolicy.WithAllowLoopback(),
		netpolicy.WithAllowPorts(port),
	)
	if err != nil {
		t.Fatal(err)
	}
	withPostgresPolicy(t, policy, func(context.Context, string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	})

	db, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, postgresDSN("database.example", int(port)), secrets.NewScrubber())
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	_ = db.PingContext(t.Context()) // the fixture is a listener, not a PostgreSQL server
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("allowed postgres target was not dialed")
	}
}

func TestPostgresDNSAnswersFailClosedAsASet(t *testing.T) {
	policy, err := netpolicy.New(netpolicy.WithSchemes("postgres"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		answers []string
	}{
		{"mixed public and loopback", []string{"8.8.8.8", "127.0.0.1"}},
		{"IPv6 loopback", []string{"::1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withPostgresPolicy(t, policy, func(context.Context, string) ([]string, error) {
				return tc.answers, nil
			})
			_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, postgresDSN("database.example", 5432), secrets.NewScrubber())
			if err == nil || !strings.Contains(err.Error(), "denied by deployment egress policy") {
				t.Fatalf("openDB error = %v, want policy denial", err)
			}
		})
	}
}

func TestPostgresResolutionIsBoundedPinnedAndCancellable(t *testing.T) {
	policy, err := netpolicy.New(netpolicy.WithSchemes("postgres"))
	if err != nil {
		t.Fatal(err)
	}

	calls := 0
	withPostgresPolicy(t, policy, func(context.Context, string) ([]string, error) {
		calls++
		return []string{"8.8.8.8"}, nil
	})
	dsn := "postgres://app:secret@database.example:5432,database.example:5433/app?sslmode=verify-full"
	db, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, dsn, secrets.NewScrubber())
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	db.Close()
	if calls != 1 {
		t.Fatalf("DNS resolver called %d times for one host, want one checked and pinned resolution", calls)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	withPostgresPolicy(t, policy, func(ctx context.Context, _ string) ([]string, error) {
		return nil, ctx.Err()
	})
	_, err = openDB(ctx, sqlv1.Engine_ENGINE_POSTGRES, postgresDSN("database.example", 5432), secrets.NewScrubber())
	if err == nil || !strings.Contains(err.Error(), "could not be resolved") {
		t.Fatalf("cancelled resolution error = %v, want closed generic resolution failure", err)
	}
}

func TestPostgresDSNFilesystemOptionsAndParseErrorsAreRefusedWithoutLeakage(t *testing.T) {
	policy, err := netpolicy.New(netpolicy.WithSchemes("postgres"))
	if err != nil {
		t.Fatal(err)
	}
	withPostgresPolicy(t, policy, func(context.Context, string) ([]string, error) {
		return []string{"8.8.8.8"}, nil
	})

	for _, dsn := range []string{
		"postgres://app:containment-secret@database.example/app?sslmode=verify-full&passfile=/tmp/pass",
		"host=database.example sslmode=verify-full servicefile=/tmp/service",
		"postgres://app:containment-secret@database.example/app?sslmode=verify-full&sslcert=/tmp/cert",
		"postgres://%zz",
	} {
		_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, dsn, secrets.NewScrubber())
		if err == nil {
			t.Fatal("openDB succeeded, want unsafe or invalid DSN refusal")
		}
		if strings.Contains(err.Error(), dsn) || strings.Contains(err.Error(), "containment-secret") {
			t.Fatalf("openDB returned unsafe error %q for refused DSN", err)
		}
	}
}

func TestPostgresMultiHostAndPortPolicyFailClosed(t *testing.T) {
	policy, err := netpolicy.New(
		netpolicy.WithSchemes("postgres"),
		netpolicy.WithAllowPorts(5432),
	)
	if err != nil {
		t.Fatal(err)
	}
	withPostgresPolicy(t, policy, func(_ context.Context, host string) ([]string, error) {
		if host == "denied.example" {
			return []string{"127.0.0.1"}, nil
		}
		return []string{"8.8.8.8"}, nil
	})

	for _, dsn := range []string{
		"postgres://app:secret@allowed.example:5432,denied.example:5432/app?sslmode=verify-full",
		postgresDSN("allowed.example", 5433),
	} {
		if _, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, dsn, secrets.NewScrubber()); err == nil {
			t.Fatal("openDB succeeded, want policy denial")
		}
	}
}

func TestPostgresRefusesMissingPolicyUnixSocketAndUnverifiedTLS(t *testing.T) {
	oldPolicy := egressPolicy
	egressPolicy = nil
	_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, postgresDSN("database.example", 5432), secrets.NewScrubber())
	egressPolicy = oldPolicy
	if err == nil || !strings.Contains(err.Error(), "requires an operator egress policy") {
		t.Fatalf("missing-policy error = %v", err)
	}

	policy, _ := netpolicy.New(netpolicy.WithSchemes("postgres"), netpolicy.WithAllowLoopback())
	withPostgresPolicy(t, policy, func(context.Context, string) ([]string, error) { return []string{"127.0.0.1"}, nil })
	for _, tc := range []struct{ name, dsn, want string }{
		{"unix", "host=/tmp dbname=app sslmode=verify-full", "Unix sockets are not permitted"},
		{"plaintext", "postgres://app:secret@database.example/app?sslmode=disable", "requires verified TLS"},
		{"unverified", "postgres://app:secret@database.example/app?sslmode=require", "requires verified TLS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_POSTGRES, tc.dsn, secrets.NewScrubber())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("openDB error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSQLiteIsNotReachableFromDistributedTask(t *testing.T) {
	old := allowSQLiteFilesForTests
	allowSQLiteFilesForTests = false
	t.Cleanup(func() { allowSQLiteFilesForTests = old })

	for _, dsn := range []string{":memory:", "/tmp/database.sqlite", "../database.sqlite", "file:/tmp/database.sqlite?mode=rwc", "file:shared?mode=memory&cache=shared"} {
		_, err := openDB(t.Context(), sqlv1.Engine_ENGINE_SQLITE, dsn, secrets.NewScrubber())
		if err == nil || !strings.Contains(err.Error(), "sqlite is disabled") {
			t.Fatalf("openDB error = %v, want sqlite filesystem-authority refusal", err)
		}
	}
}
