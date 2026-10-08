import { test, expect } from 'claude-code/testing'

import { isFlowfile, parseReports, summarize, toFileReport } from '../hooks/flowfile'

test('recognises Flowfiles and not test files', () => {
  expect(isFlowfile('examples/hello-world/workflow.yaml')).toBe(true)
  expect(isFlowfile('a/billing.flow.yaml')).toBe(true)
  expect(isFlowfile('workflow.yaml')).toBe(true)
  expect(isFlowfile('examples/hello-world/workflow.test.yaml')).toBe(false)
  expect(isFlowfile('docker-compose.yaml')).toBe(false)
  expect(isFlowfile('notworkflow.yaml')).toBe(false)
  expect(isFlowfile('a/orders.flow.test.yaml')).toBe(false)
  expect(isFlowfile('testdefaults.yaml')).toBe(false)
})

test('recognises every name docs/EDITORS.md lists, on either separator', () => {
  expect(isFlowfile('Flowfile')).toBe(true)
  expect(isFlowfile('svc/Flowfile.yaml')).toBe(true)
  expect(isFlowfile('workflow.yml')).toBe(true)
  expect(isFlowfile('workflows/nightly/etl.yaml')).toBe(true)
  expect(isFlowfile('workflows/etl.test.yaml')).toBe(false)
  expect(isFlowfile('C:\\repo\\workflows\\etl.yaml')).toBe(true)
  expect(isFlowfile('C:\\repo\\orders.flow.yaml')).toBe(true)
  expect(isFlowfile('k8s/deployment.yaml')).toBe(false)
})

test('parses jsonl and skips the summary lines', () => {
  const out = [
    '{"file":"a.flow.yaml","diagnostics":[]}',
    '{"file":"b.flow.yaml","diagnostics":[{"line":4,"column":5,"message":"bad step"}]}',
    'ERROR',
    'validation failed',
  ].join('\n')
  const reports = parseReports(out)
  expect(reports.length).toBe(2)
  expect(reports[1].diagnostics[0].line).toBe(4)
  expect(summarize(toFileReport(reports[1]))).toContain('line 4: bad step')
  expect(summarize(toFileReport(reports[0]))).toBe('a.flow.yaml: valid')
})

test('ignores malformed lines', () => {
  expect(parseReports('{not json\n{"x":1}')).toEqual([])
})
