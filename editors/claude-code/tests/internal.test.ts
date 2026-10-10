import { expect, test } from 'claude-code/testing'
import { factsFor, hiddenNote, leaseLine, parseTimeline, plainLabel, settleWaits } from '../hooks/detail'

type Row = [id: number, kind: string, step: string, at?: number]
const doc = (rows: Row[]) =>
  JSON.stringify({ entries: rows.map(([id, kind, step, at = 0]) => ({ eventId: String(id), time: new Date(Date.UTC(2026, 9, 9, 10, 0, at)).toISOString(), kind: `KIND_${kind}`, step, attempt: 1, failure: '' })) })
const parsed = (rows: Row[]) => {
  const p = parseTimeline(doc(rows))
  if (!('detail' in p)) throw new Error(p.error)
  return p.detail
}

// A finished run as Kent saw it: engine rows, a debug lease timer, a step with backticks, and a sleep.
const KENT: Row[] = [
  [1, 'STEP_SCHEDULED', 'task capability admission', 0],
  [2, 'STEP_COMPLETED', 'task capability admission', 0],
  [3, 'STEP_SCHEDULED', 'flowstate_debug', 1],
  [4, 'STEP_COMPLETED', 'flowstate_debug', 1],
  [5, 'TIMER_STARTED', 'debug lease 6f1c2a9e-3b0d-4c55-9a1e-0123456789ab held by htt', 2],
  [6, 'STEP_SCHEDULED', '`validate`', 3],
  [7, 'STEP_COMPLETED', '`validate`', 4],
  [8, 'STEP_SCHEDULED', '`orders` > `charge`', 5],
  [9, 'STEP_COMPLETED', '`orders` > `charge`', 9],
  [10, 'TIMER_STARTED', '`settle` · sleep', 10],
  [11, 'TIMER_FIRED', '`settle` · sleep', 20],
  [12, 'STEP_SCHEDULED', '`ship`', 21],
  [13, 'STEP_COMPLETED', '`ship`', 22],
]

test('engine-internal rows are hidden and counted, and the debug lease is the debugger, not a waiting step', () => {
  const d = parsed(KENT)
  expect(d.steps.map(s => s.name)).toEqual(['validate', 'orders > charge', 'settle · sleep', 'ship'])
  expect(d.hidden).toBe(2)
  expect(hiddenNote(d.hidden)).toBe('2 internal steps hidden')
  expect(hiddenNote(1)).toBe('1 internal step hidden')
  expect(hiddenNote(0)).toBe('')
  expect(d.leases).toHaveLength(1)
  expect(d.leases[0].holder).toBe('htt')
  expect(d.leases[0].open).toBe(true)
  // The lease timer is never a waiting step, so a completed run has nothing to settle and nothing waiting.
  expect(d.steps.some(s => s.status.kind === 'waiting')).toBe(false)
})

test('the step count is the author steps only: done and total never include engine rows or the lease', () => {
  const d = parsed(KENT)
  const facts = factsFor({ workflowId: 'w', status: 'STATUS_COMPLETED' }, d)
  expect([facts.done, facts.total]).toEqual([4, 4])
  expect(facts.waitingOn).toBeUndefined()
  // Mid-run: the lease is open and a user step runs; the lease is not "waiting for".
  const live = factsFor({ workflowId: 'w', status: 'STATUS_RUNNING' }, parsed(KENT.slice(0, 8)))
  expect([live.done, live.total]).toEqual([1, 2])
  expect(live.waitingOn).toBeUndefined()
})

test('the debugger reads attached while the run is live and detached once it is over', () => {
  const [lease] = parsed(KENT).leases
  expect(leaseLine(lease, true)).toBe('◉ debugger htt attached')
  expect(leaseLine(lease, false)).toBe('◉ debugger htt detached')
  const ended = parsed([...KENT, [14, 'TIMER_FIRED', 'debug lease 6f1c2a9e-3b0d-4c55-9a1e-0123456789ab held by htt', 30]]).leases[0]
  expect(ended.open).toBe(false)
  expect(leaseLine(ended, true)).toBe('◉ debugger htt detached')
})

test('settling a completed run leaves the debugger out of the steps: only a gate timer is released, a sleep stays as the timeline said', () => {
  const rows = KENT.filter(r => r[0] !== 11)
  const d = settleWaits(parsed(rows), 'STATUS_COMPLETED')!
  expect(d.steps.map(s => s.status.kind)).toEqual(['succeeded', 'succeeded', 'waiting', 'succeeded'])
  const gated = settleWaits(parsed(rows.map((r): Row => (r[0] === 10 ? [10, 'TIMER_STARTED', '`approve` · wait timeout', 10] : r))), 'STATUS_COMPLETED')!
  expect(gated.steps.map(s => s.status.kind)).toEqual(['succeeded', 'succeeded', 'succeeded', 'succeeded'])
  expect(gated.leases).toHaveLength(1)
})

test('backticks are dropped from every label shown, and only from the display', () => {
  expect(plainLabel('`orders` > `charge`')).toBe('orders > charge')
  expect(plainLabel('  `a`   `b` ')).toBe('a b')
  const d = parsed(KENT)
  // The graph overlay still matches on the engine's own label.
  expect(d.executions.some(e => e.label === '`orders` > `charge`')).toBe(true)
  // Executions stay whole, internal rows included, so the overlay's "not in this graph" count is honest.
  expect(d.executions.some(e => e.label === 'flowstate_debug')).toBe(true)
})

test('only exact internal labels are hidden: a user step that merely mentions one is a step', () => {
  const d = parsed([
    [1, 'STEP_SCHEDULED', '`flowstate_debug_notes`', 0],
    [2, 'STEP_COMPLETED', '`flowstate_debug_notes`', 1],
    [3, 'STEP_SCHEDULED', 'my task capability admission', 2],
    [4, 'STEP_COMPLETED', 'my task capability admission', 3],
    [5, 'STEP_SCHEDULED', 'debug lease notes', 4],
    [6, 'STEP_COMPLETED', 'debug lease notes', 5],
  ])
  expect(d.steps.map(s => s.name)).toEqual(['flowstate_debug_notes', 'my task capability admission', 'debug lease notes'])
  expect(d.hidden).toBe(0)
  expect(d.leases).toEqual([])
})

test('steps keep the order they began even when rows arrive out of order', () => {
  const d = parsed([
    [4, 'STEP_COMPLETED', '`b`', 4],
    [3, 'STEP_SCHEDULED', '`b`', 3],
    [2, 'STEP_COMPLETED', '`a`', 2],
    [1, 'STEP_SCHEDULED', '`a`', 1],
    [5, 'STEP_SCHEDULED', '`c`', 5],
  ])
  expect(d.steps.map(s => s.name)).toEqual(['a', 'b', 'c'])
  expect(d.steps.map(s => s.status.kind)).toEqual(['succeeded', 'succeeded', 'running'])
})

test('a hostile lease label is cleaned and bounded, and leases are capped', () => {
  const evil = `debug lease x held by ${'z'.repeat(500)}\u001b[31m‮`
  const d = parsed([[1, 'TIMER_STARTED', evil, 0]])
  expect(d.leases[0].holder.length).toBeLessThanOrEqual(40)
  expect(d.leases[0].holder).not.toMatch(/[\u001b‮]/)
  const many = parsed(Array.from({ length: 20 }, (_, i): Row => [i + 1, 'TIMER_STARTED', `debug lease ${i} held by u${i}`, i]))
  expect(many.leases.length).toBe(5)
  expect(many.steps).toEqual([])
})

test('a label cut at the bound keeps its 60-character card name without backticks', () => {
  const id = 'a'.repeat(70)
  const d = parsed([[1, 'STEP_SCHEDULED', `\`${id}\``, 0]])
  expect(d.steps[0].name).toHaveLength(60)
})

test('the pane lists the author steps in order, notes the hidden ones, and shows the debugger once', async ($, on) => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    const list = { runs: [{ workflowId: 'wf-1', runId: 'r', status: 'STATUS_COMPLETED', name: 'Orders', startTime: '2026-10-09T10:00:00Z', closeTime: '2026-10-09T10:01:00Z' }] }
    const stdout = e.argv[1] === 'timeline' ? doc(KENT) : JSON.stringify(list)
    return { value: { exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } })
  await ui.press({ key: 'run:wf-1' })
  const all = (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')
  expect(all).toMatch(/4\/4 steps/)
  expect(all).toMatch(/2 internal steps hidden/)
  expect(all).toMatch(/◉ debugger htt detached/)
  expect(all).not.toMatch(/flowstate_debug|task capability admission|`/)
  expect(all).not.toMatch(/debug lease/)
  expect(all.indexOf('validate')).toBeLessThan(all.indexOf('orders > charge'))
  expect(all.indexOf('orders > charge')).toBeLessThan(all.indexOf('ship'))
  await ui.unmount()
})
