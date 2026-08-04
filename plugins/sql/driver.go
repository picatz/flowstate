package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver; pure Go, no cgo.

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	sqlv1 "github.com/picatz/flowstate/plugins/sql/gen/sql/v1"
)

// openDB opens one connection for one call, for exactly the lifetime of
// that call - see doc.go, "Transactions end where the activity ends," for
// why this plugin never pools or reuses a connection across invocations:
// there is no connection-scoped state a retried or resumed activity could
// safely find still there.
func openDB(engine sqlv1.Engine, dsn string) (*sql.DB, error) {
	switch engine {
	case sqlv1.Engine_ENGINE_SQLITE:
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
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return nil, sdk.InvalidInput("dsn is not a postgres connection string this task can open: %v", err)
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
