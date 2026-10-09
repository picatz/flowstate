import { expect, test } from 'claude-code/testing'

import { COLOR, SPINNER, chip, duration, middleTruncate, progressBar, runRow, statusFor, statusOf, story } from '../hooks/vocab'

test('every status has a symbol, a word, and a colour token, and the symbol alone tells them apart', () => {
  const kinds = ['succeeded', 'failed', 'running', 'waiting', 'cancelled', 'skipped', 'compensated', 'unknown'] as const
  for (const k of kinds) {
    const s = statusFor(k)
    expect(s.symbol).not.toBe('')
    expect(s.word).not.toBe('')
    expect(COLOR[s.tone]).toBeTruthy()
  }
  expect(statusFor('succeeded')).toMatchObject({ symbol: '✓', tone: 'ok' })
  expect(statusFor('failed')).toMatchObject({ symbol: '✗', tone: 'fail' })
  expect(statusFor('running')).toMatchObject({ symbol: '●', tone: 'active' })
  expect(statusFor('waiting')).toMatchObject({ symbol: '◔', tone: 'wait' })
  expect(statusFor('cancelled')).toMatchObject({ symbol: '⊘', tone: 'muted' })
  expect(statusFor('skipped')).toMatchObject({ symbol: '–', tone: 'muted' })
  expect(statusFor('compensated')).toMatchObject({ symbol: '↺', tone: 'undone' })
  // Colour is never the only signal: no two kinds with different meanings share a symbol and word.
  const pairs = kinds.map(k => `${statusFor(k).symbol} ${statusFor(k).word}`)
  expect(new Set(pairs).size).toBe(kinds.length)
  expect(new Set(kinds.map(k => statusFor(k).symbol)).size).toBe(kinds.length)
  expect(chip(statusFor('failed'))).toBe('✗ failed')
})

test('the spinner is opt-in: a running status is static unless a caller animates it', () => {
  expect(statusFor('running').symbol).toBe('●')
  expect(statusFor('running', undefined, 0).symbol).toBe(SPINNER[0])
  expect(statusFor('running', undefined, 13).symbol).toBe(SPINNER[3])
  // Only running ever animates.
  expect(statusFor('failed', undefined, 4).symbol).toBe('✗')
})

test('statusOf reads every status the schema and the timeline name', () => {
  expect(statusOf('STATUS_RUNNING').kind).toBe('running')
  expect(statusOf('STATUS_COMPLETED').kind).toBe('succeeded')
  expect(statusOf('STATUS_FAILED').kind).toBe('failed')
  expect(statusOf('STATUS_CANCELED')).toMatchObject({ kind: 'cancelled', word: 'cancelled' })
  expect(statusOf('STATUS_TERMINATED')).toMatchObject({ kind: 'cancelled', word: 'terminated' })
  expect(statusOf('STATUS_TIMED_OUT')).toMatchObject({ kind: 'failed', word: 'timed out' })
  expect(statusOf('KIND_STEP_COMPLETED').kind).toBe('succeeded')
  expect(statusOf('KIND_STEP_FAILED').kind).toBe('failed')
  expect(statusOf('KIND_TIMER_STARTED').kind).toBe('waiting')
  expect(statusOf('KIND_TIMER_FIRED').kind).toBe('succeeded')
  expect(statusOf('FAILED').kind).toBe('failed')
  expect(statusOf('compensated').kind).toBe('compensated')
})

test('an unknown, missing, or hostile status is shown as unknown, never guessed or crashed on', () => {
  for (const raw of ['STATUS_UNSPECIFIED', 'STATUS_NEW_THING', '', undefined, null, 7, {}, 'constructor', '__proto__', 'toString']) {
    expect(statusOf(raw)).toMatchObject({ kind: 'unknown', symbol: '?', word: 'unknown' })
  }
  expect(statusOf('\u001b[31mSTATUS_FAILED').kind).toBe('unknown')
})

test('progressBar fills in proportion, always has the same width, and survives bad numbers', () => {
  expect(progressBar(2, 3, 6)).toBe('████░░')
  expect(progressBar(0, 3, 6)).toBe('░░░░░░')
  expect(progressBar(3, 3, 6)).toBe('██████')
  expect(progressBar(9, 3, 6)).toBe('██████')
  expect(progressBar(-1, 3, 6)).toBe('░░░░░░')
  expect(progressBar(1, 0, 6)).toBe('░░░░░░')
  expect(progressBar(NaN, NaN, 6)).toBe('░░░░░░')
  expect(progressBar(1, 2, 0).length).toBe(1)
  expect(progressBar(1, 2, 10_000).length).toBe(80)
  expect(progressBar(1, 2).length).toBe(12)
})

test('duration is short, honest, and empty when it is not a duration', () => {
  expect(duration(0)).toBe('0ms')
  expect(duration(450)).toBe('450ms')
  expect(duration(1500)).toBe('1.5s')
  expect(duration(12_000)).toBe('12s')
  expect(duration(65_000)).toBe('1m 5s')
  expect(duration(3 * 3600_000 + 4 * 60_000)).toBe('3h 4m')
  expect(duration(51 * 3600_000)).toBe('2d 3h')
  for (const bad of [-1, NaN, Infinity, undefined]) expect(duration(bad)).toBe('')
})

test('middleTruncate keeps the head and tail of an id and never grows it', () => {
  expect(middleTruncate('short', 24)).toBe('short')
  const id = 'flowstate-workflow-3f7c2e91-aaaa-bbbb-cccc-0123456789ab'
  const out = middleTruncate(id, 24)
  expect(out.length).toBe(24)
  expect(out.startsWith('flowstate-wo')).toBe(true)
  expect(out.endsWith('56789ab')).toBe(true)
  expect(out).toContain('…')
  expect(middleTruncate(id, 1).length).toBeLessThanOrEqual(3)
  expect(middleTruncate(undefined)).toBe('')
  expect(middleTruncate('a\u001b[31mb' + 'x'.repeat(5000), 10)).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/)
})

test('story reads as one sentence for each state of a run', () => {
  const base = { name: 'Deploy', workflowId: 'wf-1' }
  expect(story({ ...base, status: 'STATUS_RUNNING', done: 2, total: 3, waitingOn: 'approval' })).toBe(
    'Deploy: 2 of 3 steps done, waiting for approval',
  )
  expect(story({ ...base, status: 'STATUS_RUNNING', done: 1, total: 3 })).toBe('Deploy: 1 of 3 steps done, running')
  expect(story({ ...base, status: 'STATUS_COMPLETED', done: 3, total: 3, elapsedMs: 65_000 })).toBe(
    'Deploy: succeeded, 3 steps done (1m 5s)',
  )
  expect(story({ ...base, status: 'STATUS_FAILED', done: 1, total: 2, failedStep: 'build', failure: 'tests failed' })).toBe(
    'Deploy: failed in build, tests failed (1 of 2 steps done)',
  )
  expect(story({ ...base, status: 'STATUS_TIMED_OUT', done: 0, total: 1 })).toContain('timed out')
  expect(story({ ...base, status: 'STATUS_CANCELED', done: 1, total: 2 })).toBe('Deploy: cancelled after 1 of 2 steps done')
  expect(story({ ...base, status: 'STATUS_NEW', done: 0, total: 0 })).toBe('Deploy: status unknown, no steps yet')
})

test('story names an unnamed run by its truncated id and cleans every string it was given', () => {
  const long = 'flowstate-workflow-3f7c2e91-aaaa-bbbb-cccc-0123456789ab'
  expect(story({ workflowId: long, status: 'STATUS_RUNNING', done: 0, total: 0 })).toContain('…')
  const out = story({
    name: 'De\u001b[31mploy\u009b',
    workflowId: 'wf',
    status: 'STATUS_FAILED',
    done: 0,
    total: 1,
    failedStep: 'b\u0007uild',
    failure: 'x\u001b]0;pwn\u0007' + 'y'.repeat(1000),
  })
  expect(out).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/)
  expect(out.length).toBeLessThan(300)
  // Counts that are not counts do not print as NaN.
  expect(story({ workflowId: 'w', status: 'STATUS_RUNNING', done: NaN, total: -4 })).not.toContain('NaN')
})

test('a run row carries the status word and the id, so it reads without colour', () => {
  const row = runRow({ workflowId: 'nightly-1', runId: 'r', status: 'STATUS_FAILED', name: 'nightly-etl' } as never)
  expect(row.text).toBe('failed nightly-etl (nightly-1)')
  expect(row.status.symbol).toBe('✗')
})
