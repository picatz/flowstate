#!/usr/bin/env bash
# `flow test workflow.test.yaml` fails here with: expected step "alert" to have
# run ... alert skipped by its if:. The bug is the strict `>` on the workflow's
# `if:`; the test is right.
cat > workflow.yaml <<'EOF'
edition: v2026.4
name: jobs-alert
steps:
  - id: fetch
    http:
      url: https://api.example.com/jobs
  - id: alert
    if: ${steps.fetch.status_code > 500}
    log:
      message: ${"jobs api answered %d".format([steps.fetch.status_code])}
EOF
cat > workflow.test.yaml <<'EOF'
edition: v2026.4
tests:
  - name: a 500 answer raises the alert
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          status_code: 500
      - task: log
        returns: {}
    expect:
      ran: [fetch, alert]
EOF
