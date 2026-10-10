import { MAX_ENTRIES, MAX_STEPS, factsFor, parseTimeline, visibleSteps } from '../hooks/detail'
import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const
const mount = ($: any, surface: 'terminal' | 'desktop' = 'terminal') => $.ui.mount({ plugin: 'flowstate', surface, ...PANE })

type Reply = { exitCode: number; stdout: string; stderr: string }
const ok = (stdout: string): Reply => ({ exitCode: 0, stdout, stderr: '' })
const fail = (stderr: string): Reply => ({ exitCode: 1, stdout: '', stderr })

const runs = (...rs: [id: string, status: string, name?: string][]) =>
  ok(JSON.stringify({ runs: rs.map(([workflowId, status, name]) => ({ workflowId, runId: 'r', status, name: name ?? '', startTime: '2026-10-09T10:00:00Z', closeTime: status === 'STATUS_RUNNING' ? null : '2026-10-09T10:01:05Z' })) }))

let n = 0
const row = (kind: string, step: string, extra: Record<string, unknown> = {}, at = 0) => ({
  eventId: String(++n),
  time: new Date(Date.UTC(2026, 9, 9, 10, 0, at)).toISOString(),
  kind: `KIND_${kind}`,
  step,
  attempt: 1,
  failure: '',
  ...extra,
})
const timeline = (entries: unknown[], extra: Record<string, unknown> = {}) => ok(JSON.stringify({ entries, ...extra }))

// Answer `flow list` and `flow timeline` separately and record every argv.
const stub = (on: any, answers: { list: Reply; timeline?: Reply }, seen: string[][] = []) => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const reply = e.argv[1] === 'timeline' ? (answers.timeline ?? fail('no timeline stubbed')) : answers.list
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
}

const deploy = [
  row('STEP_SCHEDULED', 'build', {}, 0),
  row('STEP_COMPLETED', 'build', {}, 12),
  row('STEP_SCHEDULED', 'test', {}, 12),
  row('STEP_COMPLETED', 'test', {}, 75),
  row('TIMER_STARTED', 'approval', {}, 80),
]

test('selecting a run shows its story, progress and one row per step, from flow timeline', async ($, on) => {
  const seen: string[][] = []
  stub(on, { list: runs(['wf-1', 'STATUS_RUNNING', 'Deploy']), timeline: timeline(deploy) }, seen)
  const ui = await mount($)
  expect(await ui.find({ text: /Deploy: / })).toBeUndefined()

  await ui.press({ key: 'run:wf-1' })

  expect(seen.some(a => a.join(' ') === 'flow timeline -o json --max-entries 500 -- wf-1')).toBe(true)
  expect(await ui.find({ type: 'Text', text: /Deploy: 2 of 3 steps done, waiting for approval/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /2\/3 steps/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /build.*succeeded.*12s/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /test.*succeeded.*1m 3s/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /approval.*waiting/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /id wf-1/ })).toBeDefined()
  await ui.unmount()
})

test('a failed step leads with its reason on a dimmed second line, and the symbol carries the status', async ($, on) => {
  const entries = [
    row('STEP_SCHEDULED', 'build', {}, 0),
    row('STEP_FAILED', 'build', { attempt: 3, failure: 'tests failed: 2 of 40' }, 5),
  ]
  stub(on, { list: runs(['wf-2', 'STATUS_FAILED']), timeline: timeline(entries) })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-2' })

  const step = await ui.find({ type: 'Text', text: /✗ build failed/ })
  expect(step?.text).toMatch(/attempt 3/)
  expect(await ui.find({ type: 'Text', text: /^\s*tests failed: 2 of 40$/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /failed in build, tests failed: 2 of 40 \(0 of 1 step done\)/ })).toBeDefined()
  await ui.unmount()
})

test('the terminal and desktop forms carry the same facts', async ($, on) => {
  stub(on, { list: runs(['wf-1', 'STATUS_RUNNING', 'Deploy']), timeline: timeline(deploy) })
  const texts: string[] = []
  for (const surface of ['terminal', 'desktop'] as const) {
    const ui = await mount($, surface)
    await ui.press({ key: 'run:wf-1' })
    const all = await ui.findAll({ type: 'Text' })
    texts.push(all.map(t => t.text).join('\n'))
    await ui.unmount()
  }
  expect(texts[0]).toBe(texts[1])
  expect(texts[0]).toMatch(/waiting for approval/)
})

test('a timeline that fails says so and leaves the list and the rest of the pane alone', async ($, on) => {
  stub(on, { list: runs(['wf-1', 'STATUS_RUNNING', 'Deploy']), timeline: fail('ERROR\nno Flowstate server answered at x\n') })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })

  expect(await ui.find({ type: 'Text', text: /Timeline unavailable \(no Flowstate server answered at x\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Button', text: /running Deploy \(wf-1\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /No Flowfile edited yet/ })).toBeDefined()
  await ui.unmount()
})

for (const out of ['not json at all', '[1,2,3]', 'null', '"text"', '']) {
  test(`output that is not a timeline is said to be so: ${JSON.stringify(out)}`, async ($, on) => {
    stub(on, { list: runs(['wf-1', 'STATUS_RUNNING']), timeline: ok(out) })
    const ui = await mount($)
    await ui.press({ key: 'run:wf-1' })
    expect(await ui.find({ type: 'Text', text: /Timeline unavailable \(flow printed something that is not a timeline\)/ })).toBeDefined()
    await ui.unmount()
  })
}

test('an empty account says there are no steps yet instead of a blank card', async ($, on) => {
  stub(on, { list: runs(['wf-1', 'STATUS_RUNNING']), timeline: ok('{}') })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  expect(await ui.find({ type: 'Text', text: /No steps yet/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /0\/0 steps/ })).toBeDefined()
  await ui.unmount()
})

test('control characters in a step, a failure, or a run name never reach the pane', async ($, on) => {
  const entries = [
    row('STEP_SCHEDULED', 'bu\u001b[31mild\u009b', {}, 0),
    row('STEP_FAILED', 'bu\u001b[31mild\u009b', { failure: 'boom \u001b]0;pwn\u0007' + 'z'.repeat(5000) }, 1),
  ]
  stub(on, { list: runs(['wf-\u001b1', 'STATUS_FAILED', 'na\u009bme']), timeline: timeline(entries) })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })

  expect(await ui.find({ type: 'Text', text: /[\u0000-\u0008\u000b-\u001f\u007f-\u009f]/ })).toBeUndefined()
  const reason = await ui.find({ type: 'Text', text: /^\s*boom / })
  expect(reason?.text.length).toBeLessThan(200)
  await ui.unmount()
})

test('a thousand steps stay bounded: thirty rows, the failures first, and the rest counted', async ($, on) => {
  const entries = Array.from({ length: 1000 }, (_, i) => row(i === 700 ? 'STEP_FAILED' : 'STEP_COMPLETED', `s${i}`, i === 700 ? { failure: 'late failure' } : {}))
  stub(on, { list: runs(['wf-1', 'STATUS_FAILED']), timeline: timeline(entries) })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })

  // Only the first MAX_ENTRIES rows are read, so the failure at 700 was never seen:
  expect(await ui.find({ type: 'Text', text: /late failure/ })).toBeUndefined()
  const shown = await ui.findAll({ type: 'Text', text: /^\s*✓ s\d+ succeeded/ })
  expect(shown.length).toBe(MAX_STEPS)
  expect(await ui.find({ type: 'Text', text: new RegExp(`and ${MAX_ENTRIES - MAX_STEPS} more`) })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /server clipped this account/ })).toBeDefined()
  await ui.unmount()
})

test('visibleSteps keeps the steps that need a reader when it must cut', () => {
  const parsed = parseTimeline(
    JSON.stringify({
      entries: [
        ...Array.from({ length: 40 }, (_, i) => row('STEP_COMPLETED', `ok${i}`)),
        row('STEP_FAILED', 'bad', { failure: 'x' }),
        row('TIMER_STARTED', 'gate'),
      ],
    }),
  )
  if (!('detail' in parsed)) throw new Error('expected a detail')
  const { shown, more } = visibleSteps(parsed.detail.steps)
  expect(shown.length).toBe(MAX_STEPS)
  expect(more).toBe(12)
  expect(shown.map(s => s.name)).toContain('bad')
  expect(shown.map(s => s.name)).toContain('gate')
  expect(shown[0].name).toBe('ok0')
})

test('an unknown status or kind is shown as unknown or skipped, never as success', async ($, on) => {
  const entries = [row('SOMETHING_NEW', 'mystery'), row('STEP_SCHEDULED', 'real'), 'junk', null, { kind: 7 }]
  stub(on, { list: runs(['wf-1', 'STATUS_FUTURE', 'Odd']), timeline: timeline(entries) })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })

  expect(await ui.find({ type: 'Button', text: /\? unknown Odd/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /Odd: status unknown, 0 of 1 steps done|Odd: status unknown/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /mystery/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /● real running/ })).toBeDefined()
  await ui.unmount()
})

test('Close hides the card, and a run no longer listed is still readable by its id', async ($, on) => {
  const seen: string[][] = []
  stub(on, { list: runs(['wf-1', 'STATUS_COMPLETED', 'Deploy']), timeline: timeline(deploy.slice(0, 2)) }, seen)
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  expect(await ui.find({ type: 'Text', text: /id wf-1/ })).toBeDefined()
  await ui.press({ key: 'close-run' })
  expect(await ui.find({ type: 'Text', text: /id wf-1/ })).toBeUndefined()
  await ui.unmount()
})

test('the filter box passes the text to flow list unchanged, as one flag value', async ($, on) => {
  const seen: string[][] = []
  stub(on, { list: runs(['wf-1', 'STATUS_FAILED']) }, seen)
  const ui = await mount($)
  const expr = `status == "FAILED" && labels.?team.orValue("") == "payments"`
  await ui.input({ key: 'filter', text: expr })

  expect(seen.at(-1)).toEqual(['flow', 'list', '-o', 'json', `--filter=${expr}`])
  expect(await ui.find({ type: 'Text', text: /Filter: status == "FAILED"/ })).toBeDefined()
  await ui.press({ key: 'clear-filter' })
  expect(seen.at(-1)).toEqual(['flow', 'list', '-o', 'json'])
  await ui.unmount()
})

test('a filter that begins with a dash is a value, not a flag', async ($, on) => {
  const seen: string[][] = []
  stub(on, { list: runs() }, seen)
  const ui = await mount($)
  await ui.input({ key: 'filter', text: '--address evil:1' })
  expect(seen.at(-1)).toEqual(['flow', 'list', '-o', 'json', '--filter=--address evil:1'])
  expect(await ui.find({ type: 'Text', text: /No runs match the filter/ })).toBeDefined()
  await ui.unmount()
})

test('a filter the CLI rejects shows the CLI message, whole, and nothing is invented', async ($, on) => {
  const message = 'ERROR\nfilter: ERROR: <input>:1:9: Syntax error: mismatched input\n | status==\n | ........^\n\nNEXT\n  flow server dev\n'
  stub(on, { list: fail(message) })
  const ui = await mount($)
  await ui.input({ key: 'filter', text: 'status==' })

  expect(await ui.find({ type: 'Text', text: /Runs unavailable \(filter: ERROR: <input>:1:9: Syntax error: mismatched input \| status== \| \.+\^\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /flow server dev/ })).toBeUndefined()
  await ui.unmount()
})

test('a filter too long to pass whole is refused, not cut, and flow is not run', async ($, on) => {
  const seen: string[][] = []
  stub(on, { list: runs(['wf-1', 'STATUS_FAILED']) }, seen)
  const ui = await mount($)
  const before = seen.length
  await ui.input({ key: 'filter', text: 'a'.repeat(2001) })

  expect(seen.slice(before).some(a => a.some(x => x.startsWith('--filter=')))).toBe(false)
  expect(await ui.find({ type: 'Text', text: /the filter is longer than 2000 characters/ })).toBeDefined()
  await ui.unmount()
})

test('parseTimeline reads protojson and keeps timing, retries and the run-level failure', () => {
  const parsed = parseTimeline(
    JSON.stringify({
      entries: [
        row('STEP_SCHEDULED', 'a', {}, 0),
        row('STEP_FAILED', 'a', { attempt: 1, failure: 'flaky' }, 1),
        row('STEP_COMPLETED', 'a', { attempt: 2 }, 4),
        row('RUN_ENDED', '', { failure: 'run failed' }, 5),
      ],
    }),
  )
  if (!('detail' in parsed)) throw new Error('expected a detail')
  const [a] = parsed.detail.steps
  expect(a).toMatchObject({ name: 'a', attempts: 2, durationMs: 4000, reason: '' })
  expect(a.status.kind).toBe('succeeded')
  expect(parsed.detail.runFailure).toBe('run failed')
  expect(factsFor({ workflowId: 'w', status: 'STATUS_FAILED' }, parsed.detail).failure).toBe('run failed')
})

test('a successful timeline that explains a gap on stderr shows the note, cleaned and bounded', async ($, on) => {
  const note = `step "wait" is in retry backoff\u001b[31m; no failure row yet ${'x'.repeat(400)}`
  stub(on, { list: runs(['wf-1', 'STATUS_RUNNING']), timeline: { exitCode: 0, stdout: timeline(deploy).stdout, stderr: `${note}\n` } })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  const shown = await ui.find({ type: 'Text', text: /in retry backoff/ })
  expect(shown?.text).toMatch(/no failure row yet/)
  expect(shown?.text).not.toMatch(/\u001b/)
  expect(shown!.text.trim().length).toBeLessThanOrEqual(240)
  await ui.unmount()
})

test('the card keeps the pressed run name and status when the listing no longer returns it', async ($, on) => {
  const answers = { list: runs(['wf-1', 'STATUS_FAILED', 'Deploy']), timeline: timeline(deploy) }
  stub(on, answers)
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  await ui.unmount()

  answers.list = runs(['wf-9', 'STATUS_RUNNING', 'Newer'])
  const again = await mount($)
  expect(await again.find({ type: 'Button', text: /wf-1/ })).toBeUndefined()
  expect(await again.find({ type: 'Text', text: /failed Deploy/ })).toBeDefined()
  expect(await again.find({ type: 'Text', text: /unknown/ })).toBeUndefined()
  await again.unmount()
})

test('a pressed run that was still running is not claimed running once the listing drops it', async ($, on) => {
  const answers = { list: runs(['wf-1', 'STATUS_RUNNING', 'Deploy']), timeline: timeline(deploy) }
  stub(on, answers)
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  await ui.unmount()

  answers.list = runs(['wf-9', 'STATUS_RUNNING', 'Newer'])
  const again = await mount($)
  expect(await again.find({ type: 'Text', text: /unknown Deploy/ })).toBeDefined()
  expect(await again.find({ type: 'Text', text: /running Deploy/ })).toBeUndefined()
  await again.unmount()
})

test('filter text is passed unchanged, but all-whitespace means no filter',async ($, on) => {
  const seen: string[][] = []
  stub(on, { list: runs() }, seen)
  const ui = await mount($)
  await ui.input({ key: 'filter', text: '  status == "FAILED" ' })
  expect(seen.at(-1)).toEqual(['flow', 'list', '-o', 'json', '--filter=  status == "FAILED" '])
  await ui.input({ key: 'filter', text: '   ' })
  expect(seen.at(-1)).toEqual(['flow', 'list', '-o', 'json'])
  await ui.unmount()
})

test('the truncation note does not claim a plain rerun reads the rest', async ($, on) => {
  stub(on, { list: runs(['wf-1', 'STATUS_RUNNING']), timeline: timeline(deploy, { truncated: true }) })
  const ui = await mount($)
  await ui.press({ key: 'run:wf-1' })
  const t = await ui.find({ type: 'Text', text: /clipped/ })
  expect(t?.text).toMatch(/flow timeline --help/)
  expect(t?.text).toMatch(/--run-id/)
  expect(t?.text).toMatch(/--after-event-id/)
  expect(t?.text).not.toMatch(/reads the rest/)
  await ui.unmount()
})

// A wait_for_signal opens a "<step> · wait timeout" timer; when the signal wins, the engine emits only the signal row.
const parsed = (...entries: unknown[]) => {
  const p = parseTimeline(JSON.stringify({ entries }))
  if (!('detail' in p)) throw new Error(p.error)
  return p.detail
}
const T = (step: string, at = 0) => row('TIMER_STARTED', `${step} · wait timeout`, {}, at)
const S = (name: string, at = 0) => row('SIGNAL_RECEIVED', name, {}, at)

test('a signal closes the one open wait timer as answered, on the execution and the step', () => {
  const d = parsed(row('STEP_SCHEDULED', 'build', {}, 0), row('STEP_COMPLETED', 'build', {}, 1), T('approve', 2), S('release-approved', 5))
  const timer = d.executions.find(e => e.label === 'approve · wait timeout')!
  expect(timer.status.kind).toBe('succeeded')
  expect(timer.status.word).toBe('answered')
  const step = d.steps.find(s => s.name === 'approve · wait timeout')!
  expect(step.status.kind).toBe('succeeded')
  expect(step.status.word).toBe('answered')
  expect(step.durationMs).toBe(3000)
  expect(d.steps.some(s => s.status.kind === 'waiting')).toBe(false)
  // done/total counts it: build, the timer and the signal row.
  expect(factsFor({ workflowId: 'wf' }, d)).toMatchObject({ done: 3, total: 3, waitingOn: undefined })
})

test('two open wait timers and one signal: ambiguous, both stay waiting', () => {
  const d = parsed(T('approve', 0), T('review', 1), S('release-approved', 2))
  expect(d.executions.filter(e => e.status.kind === 'waiting')).toHaveLength(2)
  expect(d.steps.filter(s => s.status.kind === 'waiting')).toHaveLength(2)
  expect(factsFor({ workflowId: 'wf' }, d).waitingOn).toBe('approve · wait timeout')
})

test('each signal answers the one timer open at its moment', () => {
  const d = parsed(T('a', 0), S('s1', 1), T('b', 2), S('s2', 3))
  expect(d.executions.filter(e => e.label.endsWith('wait timeout')).map(e => e.status.word)).toEqual(['answered', 'answered'])
})

test('a signal with no wait timer changes nothing', () => {
  const d = parsed(row('STEP_SCHEDULED', 'build', {}, 0), S('release-approved', 1))
  expect(d.steps.map(s => [s.name, s.status.kind])).toEqual([['build', 'running'], ['release-approved', 'succeeded']])
})

test('a signal that arrives before the timer opens does not answer it later', () => {
  const d = parsed(S('early', 0), T('approve', 1))
  expect(d.steps.find(s => s.name === 'approve · wait timeout')!.status.kind).toBe('waiting')
})

test('a timer that fired stays as it is (succeeded, not answered), and a later signal has nothing to close', () => {
  const d = parsed(T('approve', 0), row('TIMER_FIRED', 'approve · wait timeout', {}, 4), S('late', 5))
  const timer = d.executions.find(e => e.label === 'approve · wait timeout')!
  expect(timer.status.kind).toBe('succeeded')
  expect(timer.status.word).toBe('succeeded')
})

test('an unanswered wait timer with no signal stays waiting', () => {
  const d = parsed(T('approve', 0))
  expect(d.steps[0].status.kind).toBe('waiting')
  expect(factsFor({ workflowId: 'wf' }, d).waitingOn).toBe('approve · wait timeout')
})

test('a plain timer that is not a wait timeout is never closed by a signal', () => {
  const d = parsed(row('TIMER_STARTED', 'sleep', {}, 0), S('x', 1))
  expect(d.steps.find(s => s.name === 'sleep')!.status.kind).toBe('waiting')
})
