package server_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestARefusedDeliveryLogsOneLine: a sender chooses the request path, so a
// line break in it must not become a second, forged log entry (CWE-117).
func TestARefusedDeliveryLogsOneLine(t *testing.T) {
	t.Parallel()

	var logged bytes.Buffer

	receiver := newReceiver(t, server.WithWebhookLogger(slog.New(slog.NewTextHandler(&logged, nil))))

	req := httptest.NewRequest(http.MethodPost, "/hooks/%0Alevel=ERROR%20msg=forged", strings.NewReader("{}"))
	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusNotFound, recorder.Code)
	assert.Equal(t, 1, strings.Count(logged.String(), "\n"), "one entry, one line: %q", logged.String())
	assert.Contains(t, logged.String(), `refused a delivery: no such webhook`)
	assert.Contains(t, logged.String(), `\\n`)
}
