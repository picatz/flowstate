#!/usr/bin/env bash
# A valid Flowfile that spells the same tier-to-limit mapping out in four steps.
# `flow audit` reports the ternary four times; the task is to name it once.
cat > workflow.yaml <<'EOF'
edition: v2026.4
name: tier-limits
inputs:
  tier:
    type: string
    required: true
steps:
  - id: limit_api
    log:
      message: '${"api limit " + (inputs.tier == "pro" ? "1000" : inputs.tier == "team" ? "5000" : "100")}'
  - id: limit_export
    log:
      message: '${"export limit " + (inputs.tier == "pro" ? "1000" : inputs.tier == "team" ? "5000" : "100")}'
  - id: limit_upload
    log:
      message: '${"upload limit " + (inputs.tier == "pro" ? "1000" : inputs.tier == "team" ? "5000" : "100")}'
  - id: limit_search
    log:
      message: '${"search limit " + (inputs.tier == "pro" ? "1000" : inputs.tier == "team" ? "5000" : "100")}'
EOF
