import { expect, test } from 'claude-code/testing'
import { MAX_NOTES, MAX_SNAPSHOT, debugArgv, storyOf } from '../hooks/debug'

const snap = (over: Record<string, unknown> = {}) =>
  JSON.stringify({
    session: { sessionId: 's1' },
    revision: '4',
    state: 'DEBUG_RUN_STATE_HELD',
    reason: 'DEBUG_STOP_REASON_BREAKPOINT',
    occurrence: { address: 'orders[1]/charge' },
    breakpointIds: ['bp-1'],
    observations: [],
    ...over,
  })

test('the argv is one flow debug get with the address bound and the id after --', () => {
  expect(debugArgv('flow', '', 'wf-1')).toEqual(['flow', 'debug', 'get', '-o', 'json', '--', 'wf-1'])
  expect(debugArgv('flow', 'host:9233', 'wf-1')).toEqual(['flow', 'debug', 'get', '-o', 'json', '--address=host:9233', '--', 'wf-1'])
  // An id that is not a plain one is refused, never sent.
  expect(debugArgv('flow', '', '--help')).toBeUndefined()
  expect(debugArgv('flow', '', 'a b')).toBeUndefined()
})

test('a run held at a breakpoint reads as one paused line with where and why', () => {
  expect(storyOf(snap())?.lines).toEqual(['◉ paused orders[1]/charge · breakpoint · 1 breakpoint hit'])
  expect(storyOf(snap({ reason: 'DEBUG_STOP_REASON_STEP', breakpointIds: [] }))?.lines).toEqual(['◉ paused orders[1]/charge · step'])
})

test('a session that is running, ending or ended is said as it is, without a hit it cannot know', () => {
  expect(storyOf(snap({ state: 'DEBUG_RUN_STATE_RUNNING', reason: '', breakpointIds: [] }))?.lines).toEqual(['◉ debug session running orders[1]/charge'])
  expect(storyOf(snap({ state: 'DEBUG_RUN_STATE_DETACHED', breakpointIds: [] }))?.lines).toEqual(['◉ debug session detached · last at orders[1]/charge'])
  expect(storyOf(snap({ state: 'DEBUG_RUN_STATE_EXPIRED', breakpointIds: [] }))?.lines[0]).toMatch(/expired/)
  // A breakpoint count is claimed only while the run is held there.
  expect(storyOf(snap({ state: 'DEBUG_RUN_STATE_COMPLETED' }))?.lines[0]).not.toMatch(/hit/)
})

test('observations add at most three dim notes, the newest, and bookkeeping kinds are left to the timeline', () => {
  const obs = [
    { kind: 'DEBUG_OBSERVATION_KIND_FINISHED', text: 'finished validate' },
    { kind: 'DEBUG_OBSERVATION_KIND_LOG', text: 'one' },
    { kind: 'DEBUG_OBSERVATION_KIND_LOG', text: 'two' },
    { kind: 'DEBUG_OBSERVATION_KIND_NOTICE', text: 'three' },
    { kind: 'DEBUG_OBSERVATION_KIND_NOTICE', text: 'four\nwith newline' },
  ]
  const lines = storyOf(snap({ observations: obs }))!.lines
  expect(lines.slice(1)).toEqual(['  · two', '  · three', '  · four with newline'])
  expect(lines.length - 1).toBeLessThanOrEqual(MAX_NOTES)
  expect(lines.join('\n')).not.toMatch(/finished validate/)
})

test('a failure stop shows its redacted sentence, cleaned and bounded', () => {
  const lines = storyOf(snap({ reason: 'DEBUG_STOP_REASON_FAILURE', failure: `boom\u001b[31m${'x'.repeat(500)}` }))!.lines
  expect(lines[1]).toMatch(/^ {2}✗ boom/)
  expect(lines[1].length).toBeLessThan(130)
  expect(lines[1]).not.toMatch(/\u001b/)
})

test('hostile or unreadable snapshots give no story rather than a guessed one', () => {
  expect(storyOf('not json')).toBeUndefined()
  expect(storyOf('[]')).toBeUndefined()
  expect(storyOf('null')).toBeUndefined()
  expect(storyOf('{}')).toBeUndefined()
  expect(storyOf(snap({ state: 'DEBUG_RUN_STATE_UNSPECIFIED' }))).toBeUndefined()
  expect(storyOf(snap(), true)).toBeUndefined()
  expect(storyOf(' '.repeat(MAX_SNAPSHOT + 1) + snap())).toBeUndefined()
  // observations that are not objects, a non-array list and a huge address are bounded, not thrown on
  expect(storyOf(snap({ observations: [1, null, 'x', { kind: 7, text: 9 }] }))?.lines).toHaveLength(1)
  expect(storyOf(snap({ observations: 'no' }))?.lines).toHaveLength(1)
  expect(storyOf(snap({ occurrence: { address: 'a/'.repeat(5000) } }))!.lines[0].length).toBeLessThan(200)
  expect(storyOf(snap({ occurrence: { address: '‮evil\u001b' } }))!.lines[0]).not.toMatch(/[‮\u001b]/)
})

// The pane: only a run whose timeline shows a debug lease asks, and a failed answer shows nothing.
const doc = (rows: [string, string][]) =>
  JSON.stringify({ entries: rows.map(([kind, step], i) => ({ eventId: String(i + 1), time: '2026-10-09T10:00:00Z', kind: `KIND_${kind}`, step, attempt: 1, failure: '' })) })

const world = (on: any, rows: [string, string][], debug: { exitCode: number; stdout: string }, seen: string[][]) => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const list = { runs: [{ workflowId: 'wf-1', runId: 'r', status: 'STATUS_RUNNING', name: 'Orders', startTime: '2026-10-09T10:00:00Z', closeTime: null }] }
    const reply = e.argv[1] === 'debug' ? { ...debug, stderr: debug.exitCode ? 'no session' : '' } : { exitCode: 0, stdout: e.argv[1] === 'timeline' ? doc(rows) : JSON.stringify(list), stderr: '' }
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
}
const open = async ($: any) => {
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } })
  await ui.press({ key: 'run:wf-1' })
  return { ui, all: (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n') }
}
const LEASE: [string, string][] = [['STEP_SCHEDULED', '`a`'], ['TIMER_STARTED', 'debug lease 6f1c2a9e-3b0d held by htt']]

test('a run with a debug lease shows the debugger and the pause; the debug read is one argv', async ($, on) => {
  const seen: string[][] = []
  world(on, LEASE, { exitCode: 0, stdout: snap() }, seen)
  const { ui, all } = await open($)
  expect(all).toMatch(/◉ debugger htt attached/)
  expect(all).toMatch(/◉ paused orders\[1\]\/charge · breakpoint/)
  expect(seen.filter(a => a[1] === 'debug')).toEqual([['flow', 'debug', 'get', '-o', 'json', '--', 'wf-1']])
  await ui.unmount()
})

test('a run without a debug lease never asks for a debug snapshot', async ($, on) => {
  const seen: string[][] = []
  world(on, [['STEP_SCHEDULED', '`a`']], { exitCode: 0, stdout: snap() }, seen)
  const { ui, all } = await open($)
  expect(seen.some(a => a[1] === 'debug')).toBe(false)
  expect(all).not.toMatch(/◉/)
  await ui.unmount()
})

for (const [name, reply] of [['fails', { exitCode: 1, stdout: '' }], ['prints something else', { exitCode: 0, stdout: 'garbage' }]] as const) {
  test(`a debug read that ${name} adds no story and leaves the card whole`, async ($, on) => {
    world(on, LEASE, reply, [])
    const { ui, all } = await open($)
    expect(all).toMatch(/◉ debugger htt attached/)
    expect(all).not.toMatch(/paused|debug session/)
    expect(all).toMatch(/Orders: /)
    await ui.unmount()
  })
}
