import { expect, test } from 'claude-code/testing'
import { fingerprint, parseTimeline, pauseLines } from '../hooks/detail'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const
const mount = ($: any, surface: 'terminal' | 'desktop' = 'terminal') => $.ui.mount({ plugin: 'flowstate', surface, ...PANE })
const ok = (stdout: string) => ({ exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false })

// Rows in the protojson shape `flow timeline -o json` prints: int64 ids as strings, enum names, camelCase.
let n = 0
const row = (kind: string, step: string, extra: Record<string, unknown> = {}, at = 0) => ({
  eventId: String(++n),
  time: new Date(Date.UTC(2026, 9, 9, 10, 0, at)).toISOString(),
  kind: `KIND_${kind}`,
  step,
  ...extra,
})
const WHO = 'https://issuer.example#sre'
const LABEL = `debug lease c9a7 held by ${WHO} expires`
const paused = (at: number, extra: Record<string, unknown> = {}) => row('DEBUG_PAUSED', LABEL, { sessionId: 'c9a7', actor: WHO, ...extra }, at)
const resumed = (at: number, endReason: string, extra: Record<string, unknown> = {}) => row('DEBUG_RESUMED', LABEL, { sessionId: 'c9a7', actor: WHO, endReason, ...extra }, at)
const parse = (entries: unknown[]) => {
  const p = parseTimeline(JSON.stringify({ entries }))
  if (!('detail' in p)) throw new Error(p.error)
  return p.detail
}
const texts = (d: ReturnType<typeof parse>, live: boolean) => pauseLines(d, live).map(l => l.text)

const world = (on: any, entries: unknown[], status: string) => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => ({
    value:
      e.argv[1] === 'timeline'
        ? ok(JSON.stringify({ entries }))
        : e.argv[1] === 'debug'
          ? { ...ok(''), exitCode: 1 }
          : ok(JSON.stringify({ runs: [{ workflowId: 'wf-1', runId: 'r', status, name: 'Deploy', startTime: '2026-10-09T10:00:00Z', closeTime: status === 'STATUS_RUNNING' ? null : '2026-10-09T10:01:05Z' }] })),
  }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
}
const card = async ($: any, surface: 'terminal' | 'desktop' = 'terminal') => {
  const ui = await mount($, surface)
  await ui.press({ key: 'run:wf-1' })
  const all = (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')
  await ui.unmount()
  return all
}

test('an open pause is a hold, not a step, and reads attached on a live run and detached on a finished one', () => {
  const d = parse([row('STEP_SCHEDULED', 'build', { attempt: 1, occurrence: 1 }), paused(1)])
  expect(d.steps.map(s => s.name)).toEqual(['build'])
  expect(d.executions.map(e => e.label)).toEqual(['build'])
  expect(d.leases).toMatchObject([{ holder: WHO, session: 'c9a7', open: true }])
  expect(texts(d, true)).toEqual([`◉ debugger ${WHO} attached`])
  expect(texts(d, false)).toEqual([`◉ debugger ${WHO} detached`])
})

test('a resume closes the pause: released and lapsed read as one dim line with the time held', () => {
  const released = parse([paused(0), resumed(3, 'released')])
  expect(released.leases[0]).toMatchObject({ open: false, endReason: 'released', endedMs: released.leases[0].startedMs! + 3000 })
  expect(pauseLines(released, true)).toEqual([{ text: `◉ debugger ${WHO} paused 3s · resumed`, dim: true }])
  expect(texts(parse([paused(0), resumed(90, 'lapsed')]), true)).toEqual([`◉ debugger ${WHO} paused 1m 30s · lease lapsed`])
  // A finished run keeps the past line.
  expect(texts(released, false)).toEqual(texts(released, true))
})

test('a renewal-free history is one pause per hold; a second session is its own hold', () => {
  const d = parse([paused(0), paused(1), paused(2, { sessionId: 'b1' }), resumed(5, 'released', { sessionId: 'b1' })])
  expect(d.leases.map(l => [l.session, l.open])).toEqual([['c9a7', true], ['b1', false]])
})

test('odd orderings fail safe: an orphan resume, an unknown reason, no actor', () => {
  expect(parse([resumed(0, 'released')]).leases).toEqual([])
  const d = parse([resumed(0, 'released'), paused(1), resumed(2, 'martian')])
  expect(d.leases).toMatchObject([{ open: false, endReason: 'released' }])
  expect(parse([row('DEBUG_PAUSED', '', { sessionId: 's' })]).leases).toHaveLength(1)
  // The label stands in when the server sent no actor.
  expect(parse([row('DEBUG_PAUSED', LABEL, { sessionId: 'c9a7' })]).leases[0].holder).toBe(WHO)
})

test('a legacy lease timer still works, and reads the same once it ends', () => {
  const d = parse([row('TIMER_STARTED', 'debug lease 6f1c held by htt expires', {}, 0), row('TIMER_FIRED', 'debug lease 6f1c held by htt expires', {}, 4)])
  expect(d.steps).toEqual([])
  expect(texts(d, true)).toEqual(['◉ debugger htt paused 4s · lease lapsed'])
  expect(texts(parse([row('TIMER_STARTED', 'debug lease 6f1c held by htt expires')]), true)).toEqual(['◉ debugger htt attached'])
})

test('hostile strings are cleaned and bounded', () => {
  const evil = `x\u001b[31m‮\`${'z'.repeat(300)}`
  const d = parse([paused(0, { actor: evil, sessionId: evil }), resumed(1, 'released', { actor: evil, sessionId: evil, endReason: evil })])
  const line = texts(d, true).join('\n')
  expect(line).not.toMatch(/[\u001b‮`]/)
  expect(d.leases[0].holder.length).toBeLessThanOrEqual(40)
  expect(line.length).toBeLessThan(120)
})

test('at most three past holds are named, the newest, then how many came earlier', () => {
  const rows: unknown[] = []
  for (let i = 0; i < 6; i++) rows.push(paused(i * 10), resumed(i * 10 + 2 + i, 'released'))
  expect(texts(parse(rows), true)).toEqual([
    'and 3 earlier',
    `◉ debugger ${WHO} paused 5s · resumed`,
    `◉ debugger ${WHO} paused 6s · resumed`,
    `◉ debugger ${WHO} paused 7s · resumed`,
  ])
  // Past the kept bound the oldest are counted, not lost from the line.
  const many = parse(Array.from({ length: 40 }, (_, i) => [paused(i * 10), resumed(i * 10 + 1, 'released')]).flat())
  expect(many.leases).toHaveLength(8)
  expect(texts(many, true)[0]).toBe('and 37 earlier')
  expect(pauseLines(parse([row('STEP_SCHEDULED', 'a')]), true)).toEqual([])
})

test('the scheduling count is the highest occurrence, and unknown future kinds are skipped', () => {
  const d = parse([
    row('STEP_SCHEDULED', 'deploy', { attempt: 1, occurrence: 1 }),
    row('STEP_COMPLETED', 'deploy', { attempt: 1, occurrence: 1 }),
    row('STEP_SCHEDULED', 'deploy', { attempt: 1, occurrence: 2 }),
    row('STEP_COMPLETED', 'deploy', { attempt: 1, occurrence: 2 }),
    row('STEP_SCHEDULED', 'once', { attempt: 1, occurrence: 1 }),
    row('STEP_SCHEDULED', 'old', { attempt: 1 }),
    row('FUTURE_KIND', 'ghost', { occurrence: 9 }),
  ])
  expect(d.steps.map(s => [s.name, s.ran])).toEqual([['deploy', 2], ['once', 1], ['old', 0]])
})

test('the fingerprint changes with a new pause, its end or a new scheduling, not with time', () => {
  const a = fingerprint('STATUS_RUNNING', parse([paused(0)]))
  expect(fingerprint('STATUS_RUNNING', parse([paused(5)]))).toBe(a)
  expect(fingerprint('STATUS_RUNNING', parse([paused(0), resumed(1, 'released')]))).not.toBe(a)
  expect(fingerprint('STATUS_RUNNING', parse([paused(0), resumed(1, 'released'), paused(2)]))).not.toBe(fingerprint('STATUS_RUNNING', parse([paused(0), resumed(1, 'released')])))
  expect(fingerprint('STATUS_RUNNING', parse([row('STEP_SCHEDULED', 'a', { occurrence: 1 })]))).not.toBe(fingerprint('STATUS_RUNNING', parse([row('STEP_SCHEDULED', 'a', { occurrence: 2 })])))
})

test('the card draws the holds and the count after a repeated step only, and both forms agree', async ($, on) => {
  const entries = [
    row('STEP_SCHEDULED', 'deploy', { attempt: 1, occurrence: 1 }, 0),
    row('STEP_COMPLETED', 'deploy', { attempt: 1, occurrence: 1 }, 1),
    paused(2),
    resumed(5, 'released'),
    row('STEP_SCHEDULED', 'deploy', { attempt: 1, occurrence: 2 }, 6),
    row('STEP_SCHEDULED', 'once', { attempt: 1, occurrence: 1 }, 6),
    paused(7),
  ]
  world(on, entries, 'STATUS_RUNNING')
  const forms = [await card($, 'terminal'), await card($, 'desktop')]
  expect(forms[0]).toBe(forms[1])
  expect(forms[0]).toMatch(/deploy ×2/)
  expect(forms[0]).not.toMatch(/once ×/)
  expect(forms[0]).toMatch(/◉ debugger https:\/\/issuer\.example#sre paused 3s · resumed/)
  expect(forms[0]).toMatch(/◉ debugger https:\/\/issuer\.example#sre attached/)
})

test('a finished run shows the open hold detached', async ($, on) => {
  world(on, [paused(0)], 'STATUS_COMPLETED')
  expect(await card($)).toMatch(/◉ debugger https:\/\/issuer\.example#sre detached/)
})

test('a run that never had a debugger shows no extra row', async ($, on) => {
  world(on, [row('STEP_SCHEDULED', 'a', { occurrence: 1 })], 'STATUS_RUNNING')
  expect(await card($)).not.toMatch(/◉|earlier/)
})
