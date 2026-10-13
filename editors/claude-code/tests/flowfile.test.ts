import { test, expect } from 'claude-code/testing'

import { calledLine, isFlowfile, parseReports, summarize, toFileReport } from '../hooks/flowfile'

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

test('names the called files that have problems of their own', () => {
  const out = [
    '{"file":"workflow.yaml","diagnostics":[{"line":5,"column":11,"message":"call calls \\"./lib/callee.yaml\\", which has problems of its own","code":"callee-failed"}]}',
    '{"file":"lib/callee.yaml","diagnostics":[{"line":6,"column":14,"message":"input \\"shards\\" must satisfy"}]}',
    '{"file":"lib/clean.yaml","diagnostics":[]}',
  ].join('\n')
  const reports = parseReports(out)
  const mine = toFileReport(reports[0], reports)
  expect(mine.diagnostics.length).toBe(1)
  expect(mine.called).toEqual([{ file: 'lib/callee.yaml', problems: 1 }])
  expect(summarize(mine)).toContain('called files with problems of their own: lib/callee.yaml (1)')
  // Without the other reports there is nothing to name.
  expect(toFileReport(reports[0]).called).toBeUndefined()
})

test('cleans and caps the called files it names', () => {
  const many = Array.from({ length: 12 }, (_, i) => ({ file: `lib/c${i}.yaml`, problems: 1 }))
  const [line] = calledLine(many)
  expect(line).toContain('lib/c9.yaml (1)')
  expect(line).not.toContain('lib/c10.yaml')
  expect(line).toContain('and 2 more')
  const [odd] = calledLine([{ file: 'a\n\u202eIGNORE\u200b/b.yaml', problems: 2 }])
  expect(odd).toContain('aIGNORE/b.yaml (2)')
  expect(odd).not.toMatch(/[\n\u202e\u200b]/)
  const [fenced] = calledLine([{ file: 'lib/`x`.yaml', problems: 1 }])
  expect(fenced).toContain("lib/'x'.yaml (1)")
  expect(fenced).not.toContain('`')
})

test('ignores malformed lines', () => {
  expect(parseReports('{not json\n{"x":1}')).toEqual([])
})
