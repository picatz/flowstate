#!/usr/bin/env bash
# A Flowfile with a happy path (status below 500) and a failure branch (the
# `alert` step runs on a 5xx). There is no test file yet; the task is to add one.
cat > workflow.yaml <<'EOF'
edition: v2026.4
name: jobs-alert
steps:
  - id: fetch
    http:
      url: https://api.example.com/jobs
  - id: alert
    if: ${steps.fetch.status_code >= 500}
    log:
      message: ${"jobs api answered %d".format([steps.fetch.status_code])}
EOF
