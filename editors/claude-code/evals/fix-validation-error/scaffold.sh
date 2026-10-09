#!/usr/bin/env bash
# Two real `flow validate` errors: an input the http task does not have (`uri`
# for `url`) and a reference to a step that does not exist (`pong`).
cat > workflow.yaml <<'EOF'
edition: v2026.4
name: health
steps:
  - id: ping
    http:
      uri: https://httpbin.org/status/200
      expect: ${response.status_code == 200}
  - id: done
    log:
      message: ${"ping answered %d".format([steps.pong.status_code])}
EOF
