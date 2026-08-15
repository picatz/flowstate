//go:build integration

package observability_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
)

const clickHouseImage = "clickhouse/clickhouse-server:25.8.3.66"

func TestClickHouseTenantAndTraceContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()

	docker, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Skipf("Docker-compatible daemon unavailable: %v", err)
	}
	defer docker.Close()
	if _, err := docker.Ping(ctx, client.PingOptions{}); err != nil {
		t.Skipf("Docker-compatible daemon unavailable: %v", err)
	}

	pull, err := docker.ImagePull(ctx, clickHouseImage, client.ImagePullOptions{})
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, pull)
	require.NoError(t, err)
	require.NoError(t, pull.Close())

	port := network.MustParsePort("8123/tcp")
	created, err := docker.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:        clickHouseImage,
			Env:          []string{"CLICKHOUSE_DB=otel"},
			ExposedPorts: network.PortSet{port: struct{}{}},
			Labels:       map[string]string{"dev.flowstate.test": "clickhouse-observability"},
		},
		HostConfig: &container.HostConfig{
			PortBindings: network.PortMap{port: []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: "0"}}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, _ = docker.ContainerRemove(cleanup, created.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	})
	_, err = docker.ContainerStart(ctx, created.ID, client.ContainerStartOptions{})
	require.NoError(t, err)

	inspect, err := docker.ContainerInspect(ctx, created.ID, client.ContainerInspectOptions{})
	require.NoError(t, err)
	binding := inspect.Container.NetworkSettings.Ports[port]
	require.NotEmpty(t, binding)
	endpoint := "http://127.0.0.1:" + binding[0].HostPort
	waitForClickHouse(t, ctx, endpoint)

	query(t, ctx, endpoint, `CREATE TABLE otel.flowstate_events (
tenant_id LowCardinality(String), service_name LowCardinality(String),
trace_id FixedString(32), span_id FixedString(16), run_id String,
observed_at DateTime64(9), body String
) ENGINE = MergeTree PARTITION BY toDate(observed_at)
ORDER BY (tenant_id, service_name, observed_at, trace_id)
TTL observed_at + INTERVAL 24 HOUR`)
	query(t, ctx, endpoint, `INSERT INTO otel.flowstate_events FORMAT Values
('tenant-a','flowstate-worker','0123456789abcdef0123456789abcdef','0123456789abcdef','run-a',now64(9),'step complete'),
('tenant-b','flowstate-server','fedcba9876543210fedcba9876543210','fedcba9876543210','run-b',now64(9),'run accepted')`)

	require.Equal(t, "1", query(t, ctx, endpoint,
		"SELECT count() FROM otel.flowstate_events WHERE tenant_id = 'tenant-a' AND trace_id = '0123456789abcdef0123456789abcdef'"))
	require.Equal(t, "2", query(t, ctx, endpoint,
		"SELECT count() FROM otel.flowstate_events"), "a tenant predicate is a query convention, not an isolation boundary")
}

func waitForClickHouse(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/?query=SELECT%201", nil)
		if err == nil {
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("ClickHouse did not become ready before the test deadline")
		case <-ticker.C:
		}
	}
}

func query(t *testing.T, ctx context.Context, endpoint, sql string) string {
	t.Helper()
	values := url.Values{"query": []string{sql}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/", strings.NewReader(values.Encode()))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, "ClickHouse query failed: %s", body)
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text())
	}
	require.NoError(t, scanner.Err())
	return fmt.Sprint(strings.TrimSpace(string(body)))
}
