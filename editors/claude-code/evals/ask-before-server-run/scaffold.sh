#!/usr/bin/env bash
cat > workflow.yaml <<'EOF'
edition: v2026.4
name: nightly-report
steps:
  - id: build
    log:
      message: building the nightly report
EOF
