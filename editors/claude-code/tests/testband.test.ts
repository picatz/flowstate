import { MAX_CASES, MAX_FAILING, MAX_STDOUT, bandFor, bandText, filesOf } from '../hooks/testband'
import { expect, test } from 'claude-code/testing'

const kase = (name: string, passed: boolean, over: Record<string, unknown> = {}) => ({
  name,
  passed,
  failures: passed ? [] : [{ line: 18, column: 7, message: 'output "x": expected bool false, got bool true', field: 'expect.outputs' }],
  error: '',
  duration: '0.01s',
  warnings: [],
  ...over,
})
const file = (cases: unknown[], over: Record<string, unknown> = {}) => ({ file: 'w.test.yaml', cases, refused: '', coverage: [], skipped: [], ...over })
const json = (...files: unknown[]) => JSON.stringify({ files }, null, 2)
const text = (stdout: string, ok = true) => bandText(bandFor({ stdout, ok })).join('\n')

test('every case passing is a pass with counts, uncovered steps and skips', () => {
  const out = json(file([kase('a', true), kase('b', true)], { skipped: [{ name: 'c', reason: 'later' }], coverage: [{ unreached: ['s1', 's2'] }] }))
  expect(text(out)).toBe('test ✓ passed · ✗ 0 failed · ✓ 2 passed · – 1 skipped · – 2 uncovered')
})

test('failing cases are named with where and the first reason, up to a few', () => {
  const cases = [kase('a', true), ...['f1', 'f2', 'f3', 'f4', 'f5'].map(n => kase(n, false))]
  const b = bandFor({ stdout: json(file(cases)), ok: false })
  expect(b.outcome).toBe('failed')
  expect(b.failing.length).toBe(MAX_FAILING)
  expect(b.more).toBe(2)
  const lines = bandText(b)
  expect(lines[0]).toBe('test ✗ failed · ✗ 5 failed · ✓ 1 passed')
  expect(lines[1]).toBe('  ✗ failed f1 (w.test.yaml:18): output "x": expected bool false, got bool true')
  expect(lines.at(-1)).toBe('  and 2 more')
})

test('a case that could not run shows its error, and a refused file is a failure', () => {
  const broken = kase('b', false, { failures: [], error: 'stub names a task the registry lacks' })
  expect(text(json(file([broken])), false)).toContain('✗ failed b (w.test.yaml): stub names a task the registry lacks')
  const refused = text(json(file([], { refused: 'w.test.yaml:1:4: sequence end token' })), false)
  expect(refused).toContain('test ✗ failed')
  expect(refused).toContain('file refused (w.test.yaml): w.test.yaml:1:4: sequence end token')
})

test('jsonl, one file per line, reads the same as json', () => {
  const lines = [file([kase('a', true)]), file([kase('b', false)], { file: 'x.test.yaml' })].map(f => JSON.stringify(f)).join('\n')
  const b = bandFor({ stdout: lines, ok: false })
  expect([b.passed, b.failed, b.failing[0].file]).toEqual([1, 1, 'x.test.yaml'])
  expect(filesOf(`${lines}\nnot json`)).toBeUndefined()
})

test('nothing ran is not a pass, nor is a zero exit that hid a failure', () => {
  expect(bandFor({ stdout: json(file([])), ok: true }).outcome).toBe('unknown')
  expect(bandFor({ stdout: json(file([], { skipped: [{ name: 's', reason: 'r' }] })), ok: true }).note).toBe('no case ran')
  expect(bandFor({ stdout: '{"files":[]}', ok: true }).outcome).toBe('unknown')
  // Exit 1 with every case green (a coverage or warning gate) is still failed.
  const gate = bandFor({ stdout: json(file([kase('a', true)])), ok: false })
  expect([gate.outcome, gate.note]).toEqual(['failed', 'exit status not 0 though no case failed'])
  // A case that says neither is not counted as passing.
  expect(bandFor({ stdout: json(file([kase('a', true), { name: 'odd' }])), ok: true }).outcome).toBe('unknown')
})

test('output that cannot be read is not known, whatever the exit status', () => {
  for (const stdout of ['{"files": [', '{"unrelated": true}', '[]', '{"files":[1,2]}']) {
    expect(bandFor({ stdout, ok: true }).outcome).toBe('unknown')
  }
  expect(bandFor({ stdout: json(file([kase('a', true)])), ok: true, partial: true }).outcome).toBe('unknown')
  expect(bandFor({ stdout: json(file([kase('a', true)])), ok: true, unfinished: true }).note).toBe('the run did not finish')
  expect(bandFor({ stdout: json(file([kase('x'.repeat(MAX_STDOUT), true)])), ok: true }).outcome).toBe('unknown')
})

test('the text report earns the exit status and nothing more', () => {
  const plain = 'PASS  w.test.yaml: a\n\n1 file · 1 cases · 1 passed · 0.0s\n'
  expect(text(plain)).toBe('test ✓ passed · exit 0, no case detail; add -o json')
  expect(text('FAIL  w.test.yaml: a\n', false)).toBe('test ✗ failed · exit status not 0, no case detail; add -o json')
})

test('hostile names and reasons are cleaned and bounded', () => {
  const evil = `x\u001b[2J‮​ ${'n'.repeat(500)}`
  const out = text(json(file([kase(evil, false, { failures: [{ line: 3, message: evil }] })], { file: evil })), false)
  expect(out).not.toMatch(new RegExp('[\\u0000-\\u0009\\u000b-\\u001f\\u007f-\\u009f\\u200b-\\u200f\\u2028\\u2029\\u202a-\\u202e]'))
  expect(out.length).toBeLessThan(400)
})

test('a huge suite is cut at a bound and never reads as passed', () => {
  const many = Array.from({ length: MAX_CASES + 50 }, (_, i) => kase(`c${i}`, true))
  const passing = bandFor({ stdout: json(file(many)), ok: true })
  expect([passing.outcome, passing.cut, passing.passed]).toEqual(['unknown', true, MAX_CASES])
  expect(bandText(passing)[0]).toContain('✓ 99+ passed')
  const failing = bandFor({ stdout: json(file([kase('bad', false), ...many])), ok: false })
  expect(failing.outcome).toBe('failed')
  expect(failing.failing.length).toBe(1)
  const files = Array.from({ length: 500 }, () => file([kase('a', true)]))
  expect(bandFor({ stdout: json(...files), ok: true }).outcome).toBe('unknown')
})
