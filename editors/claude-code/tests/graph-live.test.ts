import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 120, rows: 80 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

type Reply = { exitCode: number; stdout: string; stderr: string }
const ok = (stdout: string): Reply => ({ exitCode: 0, stdout, stderr: '' })
const fail = (stderr: string): Reply => ({ exitCode: 1, stdout: '', stderr })
const entry = (name: string) => ({ name, kind: 'file', size: 1, mtimeMs: 1, isLink: false })

const node = (id: string, kind: string, label: string, address: string, detail = '') => ({ id, kind: `GRAPH_NODE_KIND_${kind}`, label, address, detail })
const edge = (from: string, to: string) => ({ from, to, kind: 'GRAPH_EDGE_KIND_CONTAINS', count: 1 })
const GRAPH = ok(
  JSON.stringify({
    nodes: [node('w', 'WORKFLOW', 'deploy', ''), node('b', 'STEP', 'build', 'build'), node('g', 'STEP', 'gate', 'gate', 'switch'), node('s', 'STEP', 'ship', 'gate?0/ship')],
    edges: [edge('w', 'b'), edge('w', 'g'), edge('g', 's')],
    partial: false,
    notes: [],
    overlays: [],
  }),
)
const runs = (name: string) => ok(JSON.stringify({ runs: [{ workflowId: 'wf-1234567890abcdef', runId: 'r', status: 'STATUS_RUNNING', name, startTime: '2026-10-09T10:00:00Z', closeTime: null }] }))
const rows = (...r: [string, string][]) =>
  ok(JSON.stringify({ entries: r.map(([kind, step], i) => ({ eventId: String(i + 1), time: '2026-10-09T10:00:00Z', kind: `KIND_${kind}`, step, attempt: 1, failure: '' })) }))

interface World {
  list: Reply
  timeline: Reply
}
const stub = (on: any, w: World, seen: string[][]) => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const reply = e.argv[1] === 'graph' ? GRAPH : e.argv[1] === 'timeline' ? w.timeline : e.argv[1] === 'list' ? w.list : fail(`no ${e.argv[1]} stubbed`)
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('fs.list', (_$: unknown, e: { path: string }) => ({ value: /workflows$/.test(e.path ?? '') ? [] : [entry('deploy.flow.yaml')] }))
  on('fs.read', () => ({ value: 'name: deploy\nsteps: []\n' }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
}
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')
const open = async ($: any, on: any, w: World) => {
  const seen: string[][] = []
  stub(on, w, seen)
  const ui = await mount($)
  await ui.select({ plugin: 'flowstate', key: 'run-file', value: 'deploy.flow.yaml' })
  return { ui, seen }
}

test('with no run open the graph keeps the not-run mark and says no run is shown', async ($, on) => {
  const { ui } = await open($, on, { list: runs('deploy'), timeline: rows(['STEP_COMPLETED', '`build`']) })
  const all = await texts(ui)
  expect(all).toMatch(/○ build/)
  expect(all).toMatch(/no run is shown/)
  expect(all).not.toMatch(/status from run/)
  await ui.unmount()
})

test('with a run of the same workflow open, matched top-level steps carry symbol and word, labelled with the run id', async ($, on) => {
  const { ui, seen } = await open($, on, { list: runs('deploy'), timeline: rows(['STEP_COMPLETED', '`build`'], ['STEP_COMPLETED', '`deploy` > `ship`'], ['TIMER_STARTED', '`gate` · sleep']) })
  await ui.press({ key: 'run:wf-1234567890abcdef' })
  const all = await texts(ui)
  expect(all).toMatch(/status from run wf-12345…0abcdef/)
  expect(all).toMatch(/✓ build · succeeded/)
  expect(all).toMatch(/○ gate — switch/)
  expect(all).toMatch(/○ ship/)
  expect(all).toMatch(/2 timeline steps not in this graph's top-level steps/)
  expect(all).toMatch(/1 nested step keep ○/)
  // Nothing extra was started for the overlay: one graph read, and the timeline the card already needed.
  expect(seen.filter(a => a[1] === 'graph')).toHaveLength(1)
  await ui.unmount()
})

test('a run of a different workflow gets no overlay, and the section says so', async ($, on) => {
  const { ui } = await open($, on, { list: runs('other-flow'), timeline: rows(['STEP_COMPLETED', '`build`']) })
  await ui.press({ key: 'run:wf-1234567890abcdef' })
  const all = await texts(ui)
  expect(all).toMatch(/○ build/)
  expect(all).not.toMatch(/✓ build/)
  expect(all).toMatch(/is of other-flow, not deploy: no status is shown/)
  await ui.unmount()
})

test('a run whose timeline cannot be read leaves the graph as declared', async ($, on) => {
  const { ui } = await open($, on, { list: runs('deploy'), timeline: fail('no such run') })
  await ui.press({ key: 'run:wf-1234567890abcdef' })
  const all = await texts(ui)
  expect(all).toMatch(/○ build/)
  expect(all).not.toMatch(/status from run/)
  await ui.unmount()
})

test('a clipped timeline is said in the graph notes too', async ($, on) => {
  const clipped = ok(JSON.stringify({ truncated: true, entries: [{ eventId: '1', time: '2026-10-09T10:00:00Z', kind: 'KIND_STEP_COMPLETED', step: '`build`', attempt: 1, failure: '' }] }))
  const { ui } = await open($, on, { list: runs('deploy'), timeline: clipped })
  await ui.press({ key: 'run:wf-1234567890abcdef' })
  expect(await texts(ui)).toMatch(/note: the server clipped the timeline, so a step without a status may not have been read/)
  await ui.unmount()
})
