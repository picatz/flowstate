package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver; pure Go, no cgo.

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// openDB opens one connection for one call, for exactly the lifetime of
// that call - see doc.go, "Transactions end where the activity ends," for
// why this plugin never pools or reuses a connection across invocations:
// there is no connection-scoped state a retried or resumed activity could
// safely find still there.
func openDB(ctx context.Context, engine sqlv1.Engine, dsn string, scrubber *secrets.Scrubber) (*sql.DB, error) {
	switch engine {
	case sqlv1.Engine_ENGINE_SQLITE:
		if !allowSQLiteFilesForTests {
			return nil, sdk.PermissionDenied(
				"sqlite is disabled in the distributed plugin because an embedded database grants worker-filesystem authority; " +
					"use postgres with an operator egress policy")
		}
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			return nil, sdk.InvalidInput("dsn is not a sqlite connection string this task can open: %v", err)
		}
		// A hard cap on concurrent connections, since sql.exec's own
		// single-transaction contract (doc.go) means every call needs
		// exactly one connection, never a pool serving several at once
		// under one *sql.DB.
		db.SetMaxOpenConns(1)
		return db, nil

	case sqlv1.Engine_ENGINE_POSTGRES:
		if egressPolicy == nil {
			return nil, sdk.PermissionDenied(
				"postgres requires an operator egress policy passed with --egress-policy; the SQL plugin denies network access when it is absent")
		}
		cfg, err := pgx.ParseConfigWithOptions(dsn, pgx.ParseConfigOptions{
			ConnStringAllowedKeys: []string{
				"host", "port", "dbname", "database", "user", "password",
				"sslmode", "connect_timeout", "target_session_attrs",
				"application_name", "options", "require_auth", "channel_binding",
				"sslnegotiation",
			},
		})
		if err != nil {
			return nil, sdk.InvalidInput("dsn is not a postgres connection string this task can open")
		}
		if err := governPostgresConfig(ctx, cfg, scrubber); err != nil {
			return nil, err
		}
		// See "The wire-level bound" in doc.go: AfterNetConnect is called
		// once TLS (if any) is already established, so this wraps the exact
		// net.Conn the protocol reads from regardless of whether the DSN
		// asked for TLS - the RoundTripper lesson from CLAUDE.md applied to
		// a database wire protocol instead of HTTP: the bound lives below
		// the client library, on the actual socket, so no path the library
		// treats specially (an error response, a COPY stream, a result set
		// larger than any row-count bound anticipated) can miss it.
		cfg.Config.AfterNetConnect = func(_ context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
			return &wireBoundConn{Conn: conn, remaining: maxWireBytes}, nil
		}
		db := stdlib.OpenDB(*cfg)
		db.SetMaxOpenConns(1)
		return db, nil

	default:
		return nil, sdk.InvalidInput(
			"engine %q is not one this build supports; this build was compiled with: sqlite, postgres",
			engine.String())
	}
}

// allowSQLiteFilesForTests is false in every built plugin. Package tests set it
// from helper_test.go so existing hermetic fixtures can continue exercising SQL
// semantics without making worker filesystem authority reachable from a task.
var allowSQLiteFilesForTests bool

const maxPostgresHosts = 16

var lookupPostgresHost = net.DefaultResolver.LookupHost

// governPostgresConfig resolves and authorizes the complete pgx host set before
// pgx can attempt any connection, then pins that checked answer set into pgx's
// LookupFunc. The actual DialFunc checks the selected IP again immediately
// before net.Dialer opens its socket. This closes both directions: one denied
// answer makes a mixed DNS result fail closed, and DNS cannot rebind between a
// policy check and the dial because pgx receives only the checked IP literals.
func governPostgresConfig(ctx context.Context, cfg *pgx.ConnConfig, scrubber *secrets.Scrubber) error {
	type endpoint struct {
		host string
		port uint16
	}
	endpoints := []endpoint{{host: cfg.Config.Host, port: cfg.Config.Port}}
	for _, fallback := range cfg.Config.Fallbacks {
		endpoints = append(endpoints, endpoint{host: fallback.Host, port: fallback.Port})
	}
	if len(endpoints) > maxPostgresHosts {
		return sdk.InvalidInput("dsn names more than the %d postgres host attempts this task permits", maxPostgresHosts)
	}

	policyCtx := postgresPolicyContext(ctx)
	resolved := make(map[string][]string, len(endpoints))
	for _, ep := range endpoints {
		if ep.host == "" || ep.host[0] == '/' || (len(ep.host) >= 3 && ep.host[1] == ':' && (ep.host[2] == '\\' || ep.host[2] == '/')) {
			return sdk.PermissionDenied("postgres Unix sockets are not permitted; SQL destinations must be TCP endpoints authorized by egress policy")
		}
		if scrubber != nil {
			scrubber.AddValue(ep.host)
		}
		// Refuse a host/port that request-scoped policy denies before DNS.
		// Resolution is itself an outbound effect, and an attacker-controlled
		// denied name must not become a DNS exfiltration channel merely because
		// the eventual TCP connection would be refused.
		target := &url.URL{Scheme: "postgres", Host: net.JoinHostPort(ep.host, strconv.Itoa(int(ep.port)))}
		if err := egressPolicy.CheckURL(policyCtx, http.MethodConnect, target); err != nil {
			return sdk.PermissionDenied("postgres destination is denied by deployment egress policy")
		}
		if _, ok := resolved[ep.host]; ok {
			continue
		}

		answers, err := lookupPostgresHost(policyCtx, ep.host)
		if err != nil || len(answers) == 0 {
			return sdk.Unavailable("postgres destination could not be resolved")
		}
		if len(answers) > maxPostgresHosts {
			return sdk.PermissionDenied("postgres destination resolved to more addresses than this task will authorize")
		}

		for _, answer := range answers {
			ip, err := netip.ParseAddr(answer)
			if err != nil {
				return sdk.PermissionDenied("postgres destination resolved to an invalid address")
			}
			if scrubber != nil {
				scrubber.AddValue(answer)
			}
			// A hostname may appear with different ports in a multi-host DSN.
			// Check every port attached to it before admitting any answer.
			for _, candidate := range endpoints {
				if candidate.host != ep.host {
					continue
				}
				if err := egressPolicy.CheckConnection(policyCtx, "postgres", ep.host, netip.AddrPortFrom(ip, candidate.port)); err != nil {
					return sdk.PermissionDenied("postgres destination is denied by deployment egress policy")
				}
			}
		}
		resolved[ep.host] = slices.Clone(answers)
	}

	// A DSN can ask pgx to try plaintext after TLS (sslmode=prefer/allow) or
	// disable verification. Neither is an acceptable path for a host-resolved
	// credential. Require every primary/fallback attempt to use verified TLS,
	// and apply the same TLS floor the operator configured for egress.
	tlsConfigs := []*tls.Config{cfg.Config.TLSConfig}
	for _, fallback := range cfg.Config.Fallbacks {
		tlsConfigs = append(tlsConfigs, fallback.TLSConfig)
	}
	for _, tlsConfig := range tlsConfigs {
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify {
			return sdk.PermissionDenied("postgres requires verified TLS on every host attempt; use sslmode=verify-full")
		}
		if tlsConfig.MinVersion < egressPolicy.MinTLSVersion() {
			tlsConfig.MinVersion = egressPolicy.MinTLSVersion()
		}
	}

	cfg.Config.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		answers, ok := resolved[host]
		if !ok {
			return nil, fmt.Errorf("postgres host was not authorized before resolution")
		}
		return slices.Clone(answers), nil
	}
	dialer := &net.Dialer{Timeout: cfg.Config.ConnectTimeout}
	cfg.Config.DialFunc = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, sdk.PermissionDenied("postgres may connect only over TCP")
		}
		host, portText, err := net.SplitHostPort(address)
		if err != nil {
			return nil, sdk.PermissionDenied("postgres dial target is not an IP address and port")
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return nil, sdk.PermissionDenied("postgres dial target was not resolved before dialing")
		}
		port, err := strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return nil, sdk.PermissionDenied("postgres dial target has an invalid port")
		}
		// The hostname and connection-scoped CEL rule were evaluated against
		// this exact IP in the pinned resolution pass above. CheckAddr here is
		// the non-bypassable last gate on the actual socket target; calling
		// CheckConnection with host=IP would incorrectly change a rule written
		// against the original hostname.
		if err := egressPolicy.CheckAddr(netip.AddrPortFrom(ip, uint16(port))); err != nil {
			return nil, sdk.PermissionDenied("postgres destination is denied by deployment egress policy")
		}
		return dialer.DialContext(dialCtx, network, address)
	}

	if scrubber != nil {
		scrubber.AddValue(cfg.Config.Password)
	}
	return nil
}

func postgresPolicyContext(ctx context.Context) context.Context {
	caller, _ := sdk.CallerFromContext(ctx)
	identity := caller.Identity
	ctx = netpolicy.ContextWithIdentity(ctx, netpolicy.Identity{
		Subject:   identity.GetSubject(),
		Issuer:    identity.GetIssuer(),
		Namespace: identity.GetNamespace(),
		Claims:    identity.GetClaims(),
	})
	return netpolicy.ContextWithCredentials(ctx, true)
}

// wireBoundConn wraps a net.Conn and refuses to read past a fixed byte
// budget - the postgres-specific enforcement of maxWireBytes (bounds.go).
type wireBoundConn struct {
	net.Conn
	remaining int64
}

func (c *wireBoundConn) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		return 0, fmt.Errorf("sql: postgres connection exceeded the %d byte wire-read ceiling this task enforces", maxWireBytes)
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.Conn.Read(p)
	c.remaining -= int64(n)
	return n, err
}
