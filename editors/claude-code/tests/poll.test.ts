import { expect, mock, test } from 'claude-code/testing'
import { DRAW_GRACE_MS, MAX_IDLE_READS, Poller, READ_FAST_MS, READ_SLOW_MS, TICK_MS, isLive, readDelay } from '../hooks/poll'
import { factsFor, fingerprint, parseTimeline, settleWaits, stepElapsed } from '../hooks/detail'

/** A poller on a hand-driven clock: `fire` runs the one pending timer. */
const rig = () => {
  let t = 1_000_000
  const pending: { fn: () => void; ms: number; live: boolean }[] = []
  const redraws: boolean[] = []
  let inFlight = 0
  let maxInFlight = 0
  const poller = new Poller(() => t)
  const deps = {
    after: (ms: number, fn: () => void) => {
      const one = { fn, ms, live: true }
      pending.push(one)
      return () => {
        one.live = false
      }
    },
    redraw: async (read: boolean) => {
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      redraws.push(read)
      await Promise.resolve()
      poller.drawBegan()
      poller.drawEnded()
      inFlight--
    },
  }
  const fire = async () => {
    const one = pending.shift()
    if (one === undefined || !one.live) return false
    t += one.ms
    one.fn()
    // let the tick (an async function) finish
    for (let i = 0; i < 5; i++) await Promise.resolve()
    return true
  }
  return { poller, deps, fire, redraws, pending, maxInFlight: () => maxInFlight, advance: (ms: number) => (t += ms) }
}

test('reads back off from 2s to 10s, and a change takes the poll back to fast', () => {
  expect([0, 1, 2, 3, 5, 6, 9, 12, 30].map(readDelay)).toEqual([2000, 2000, 2000, 4000, 4000, 8000, 10_000, 10_000, 10_000])
  expect(readDelay(-5)).toBe(READ_FAST_MS)
  expect(readDelay(Number.NaN)).toBe(READ_FAST_MS)
  expect(readDelay(1e9)).toBe(READ_SLOW_MS)
})

test('only a running or waiting run is live', () => {
  expect(['running', 'waiting'].map(isLive)).toEqual([true, true])
  expect(['succeeded', 'failed', 'cancelled', 'unknown', ''].some(isLive)).toBe(false)
})

test('a poller ticks every second, asks for a read on the backoff schedule and never overlaps itself', async () => {
  const r = rig()
  r.poller.start(r.deps)
  r.poller.start(r.deps) // idempotent: still one timer
  expect(r.pending.length).toBe(1)
  for (let i = 0; i < 6; i++) await r.fire()
  // a read at 2s, 4s, 6s (fast), then the local ticks between
  expect(r.redraws.slice(0, 6)).toEqual([false, true, false, true, false, true])
  expect(r.maxInFlight()).toBe(1)
  expect(r.pending.length).toBe(1)
  r.poller.stop()
  expect(await r.fire()).toBe(false)
})

test('a run that is over stops the poll and leaves no timer running', async () => {
  const r = rig()
  r.poller.start(r.deps)
  await r.fire()
  r.poller.seen('done', false)
  expect(r.poller.active).toBe(false)
  expect(await r.fire()).toBe(false)
  expect(r.pending.length).toBe(0)
  // and a stale timer firing late does nothing
  const late = rig()
  late.poller.start(late.deps)
  const stale = late.pending[0]
  late.poller.stop()
  late.poller.start(late.deps)
  stale.fn()
  for (let i = 0; i < 5; i++) await Promise.resolve()
  expect(late.redraws.length).toBe(0)
})

test('a pane that stopped drawing stops the poll after the grace period', async () => {
  const r = rig()
  r.poller.start(r.deps)
  r.advance(DRAW_GRACE_MS + 1)
  await r.fire()
  expect(r.poller.active).toBe(false)
  expect(r.redraws.length).toBe(0)
})

test('with no change the poll gives up after a bounded number of reads', async () => {
  const r = rig()
  r.poller.start(r.deps)
  let ticks = 0
  while (r.poller.active && ticks < 5000) {
    ticks++
    r.poller.seen('same', true)
    await r.fire()
  }
  expect(r.poller.active).toBe(false)
  expect(r.poller.reads).toBeLessThanOrEqual(MAX_IDLE_READS)
  // 3 reads at 2s, 3 at 4s, 3 at 8s, then 10s each: bounded well under an hour of ticks.
  expect(ticks).toBeLessThan(MAX_IDLE_READS * (READ_SLOW_MS / TICK_MS) + 10)
})

test('a changing run keeps the poll alive past the idle bound', async () => {
  const r = rig()
  r.poller.start(r.deps)
  for (let i = 0; i < 400; i++) {
    r.poller.seen(`state ${Math.floor(i / 2)}`, true)
    await r.fire()
  }
  expect(r.poller.active).toBe(true)
  r.poller.stop()
})

test('a redraw that throws stops the poll rather than looping', async () => {
  const r = rig()
  r.poller.start({ ...r.deps, redraw: async () => { throw new Error('closed') } })
  await r.fire()
  expect(r.poller.active).toBe(false)
  expect(r.pending.length).toBe(0)
})

const entries = (...rows: [string, string, number][]) =>
  JSON.stringify({ entries: rows.map(([kind, step, at], i) => ({ eventId: String(i + 1), time: new Date(Date.UTC(2026, 9, 9, 10, 0, at)).toISOString(), kind: `KIND_${kind}`, step, attempt: 1, failure: '' })) })

test('a waiting step counts up from its start between reads', () => {
  const p = parseTimeline(entries(['STEP_SCHEDULED', 'a', 0], ['STEP_COMPLETED', 'a', 5], ['TIMER_STARTED', 'settle · sleep', 10]))
  if (!('detail' in p)) throw new Error('no detail')
  const [a, settle] = p.detail.steps
  const base = Date.UTC(2026, 9, 9, 10, 0, 10)
  expect(stepElapsed(a, base + 999_999)).toBe(5000)
  expect(stepElapsed(settle, base + 173_000)).toBe(173_000)
  expect(stepElapsed(settle, base + 174_000)).toBe(174_000)
  // a clock behind the row (skew) shows nothing rather than a negative
  expect(stepElapsed(settle, base - 1)).toBeUndefined()
})

test('a finished run settles its waits and the bar fills; a live run is left alone', () => {
  const p = parseTimeline(entries(['STEP_SCHEDULED', 'a', 0], ['STEP_COMPLETED', 'a', 5], ['TIMER_STARTED', 'settle · sleep', 10], ['STEP_SCHEDULED', 'b', 11]))
  if (!('detail' in p)) throw new Error('no detail')
  const close = Date.UTC(2026, 9, 9, 10, 3, 0)
  const done = settleWaits(p.detail, 'succeeded', close)
  expect(done.steps.map(s => s.status.kind)).toEqual(['succeeded', 'succeeded', 'succeeded'])
  expect(done.steps[1].durationMs).toBe(close - Date.UTC(2026, 9, 9, 10, 0, 10))
  const facts = factsFor({ workflowId: 'w', status: 'STATUS_COMPLETED', closeTime: new Date(close).toISOString() }, done)
  expect(facts.done).toBe(facts.total)
  expect(facts.waitingOn).toBeUndefined()
  // failed or cancelled: closed, never presented as success
  expect(settleWaits(p.detail, 'failed', close).steps[1].status.word).toBe('closed')
  expect(settleWaits(p.detail, 'cancelled', close).steps[1].status.kind).toBe('cancelled')
  // not over, or not known: unchanged
  expect(settleWaits(p.detail, 'running', close)).toBe(p.detail)
  expect(settleWaits(p.detail, 'unknown', close)).toBe(p.detail)
  // a close time before the step began does not invent a negative duration
  expect(settleWaits(p.detail, 'succeeded', 1).steps[1].durationMs).toBeUndefined()
})

test('the fingerprint changes with status and steps but not with time', () => {
  const a = parseTimeline(entries(['STEP_SCHEDULED', 'a', 0]))
  const b = parseTimeline(entries(['STEP_SCHEDULED', 'a', 50]))
  const c = parseTimeline(entries(['STEP_SCHEDULED', 'a', 0], ['STEP_COMPLETED', 'a', 1]))
  if (!('detail' in a) || !('detail' in b) || !('detail' in c)) throw new Error('no detail')
  expect(fingerprint('STATUS_RUNNING', a.detail)).toBe(fingerprint('STATUS_RUNNING', b.detail))
  expect(fingerprint('STATUS_RUNNING', a.detail)).not.toBe(fingerprint('STATUS_RUNNING', c.detail))
  expect(fingerprint('STATUS_RUNNING', a.detail)).not.toBe(fingerprint('STATUS_COMPLETED', a.detail))
})

// The pane, end to end: a run open and running is read again on a clock; once the server says COMPLETED the card settles and no more reads happen.
const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')

test('an open running run is re-read and settles when it completes, then stops reading', async ($, on) => {
  const clock = mock.clock(on, { now: Date.UTC(2026, 9, 9, 10, 3, 0) })
  let status = 'STATUS_RUNNING'
  let complete = false
  const timelineCalls: number[] = []
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    if (e.argv[1] === 'timeline') {
      timelineCalls.push(1)
      const rows = [
        ['STEP_SCHEDULED', 'a', 0],
        ['STEP_COMPLETED', 'a', 5],
        ['TIMER_STARTED', 'settle · sleep', 10],
        ...(complete ? [['TIMER_FIRED', 'settle · sleep', 70]] : []),
      ] as [string, string, number][]
      return { value: { exitCode: 0, stdout: entries(...rows), stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
    }
    const list = { runs: [{ workflowId: 'wf-1', runId: 'r', status, name: 'Deploy', startTime: '2026-10-09T10:00:00Z', closeTime: status === 'STATUS_RUNNING' ? null : '2026-10-09T10:01:10Z' }] }
    return { value: { exitCode: e.argv[1] === 'list' ? 0 : 1, stdout: JSON.stringify(list), stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  expect(await texts(ui)).toMatch(/settle · sleep.*waiting/)
  const afterOpen = timelineCalls.length

  // The server finishes the run; within the backoff the card reads it again and settles.
  status = 'STATUS_COMPLETED'
  complete = true
  await clock.advance(3000)
  expect(timelineCalls.length).toBeGreaterThan(afterOpen)
  const all = await texts(ui)
  expect(all).toMatch(/succeeded/)
  expect(all).not.toMatch(/settle · sleep.*waiting/)
  expect(all).toMatch(/2\/2 steps/)

  // Over: no further timeline reads however long the clock runs.
  const settled = timelineCalls.length
  await clock.advance(120_000)
  expect(timelineCalls.length).toBe(settled)
  await ui.unmount()
})

test('a run that is already over starts no poll', async ($, on) => {
  const clock = mock.clock(on, { now: 0 })
  const calls: string[] = []
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    calls.push(e.argv[1])
    const list = { runs: [{ workflowId: 'wf-1', runId: 'r', status: 'STATUS_COMPLETED', name: 'Deploy', startTime: '2026-10-09T10:00:00Z', closeTime: '2026-10-09T10:01:10Z' }] }
    return { value: { exitCode: 0, stdout: e.argv[1] === 'timeline' ? entries(['STEP_SCHEDULED', 'a', 0], ['STEP_COMPLETED', 'a', 5]) : JSON.stringify(list), stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  const before = calls.length
  await clock.advance(60_000)
  expect(calls.length).toBe(before)
  await ui.unmount()
})
