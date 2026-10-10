#!/usr/bin/env bash
# An http step aimed at loopback, which the default egress policy denies.
cat > workflow.yaml <<'EOT'
edition: v2026.4
name: probe
steps:
  - id: ping
    http:
      url: http://localhost:8080/health
      expect: ${response.status_code == 200}
  - id: done
    log:
      message: probe answered
EOT
