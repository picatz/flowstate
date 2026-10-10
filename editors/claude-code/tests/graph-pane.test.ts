import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 80 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

type Reply = { exitCode: number; stdout: string; stderr: string }
const ok = (stdout: string): Reply => ({ exitCode: 0, stdout, stderr: '' })
const fail = (stderr: string, exitCode = 1): Reply => ({ exitCode, stdout: '', stderr })
const entry = (name: string, kind = 'file', mtimeMs = 1) => ({ name, kind, size: 1, mtimeMs, isLink: false })

const node = (id: string, kind: string, label: string, detail = '') => ({ id, kind: `GRAPH_NODE_KIND_${kind}`, label, address: label, detail })
const edge = (from: string, to: string) => ({ from, to, kind: 'GRAPH_EDGE_KIND_CONTAINS', count: 1 })
const GRAPH = (extra: Record<string, unknown> = {}) =>
  ok(
    JSON.stringify({
      nodes: [node('workflow:deploy', 'WORKFLOW', 'deploy'), node('step:deploy/build', 'STEP', 'build', 'task "log"'), node('step:deploy/gate', 'STEP', 'gate', 'switch'), node('step:deploy/gate?0/ship', 'STEP', 'ship', 'task "log"')],
      edges: [edge('workflow:deploy', 'step:deploy/build'), edge('workflow:deploy', 'step:deploy/gate'), edge('step:deploy/gate', 'step:deploy/gate?0/ship')],
      partial: false,
      notes: [],
      overlays: [],
      ...extra,
    }),
  )

interface World {
  graph?: Reply | 'deny'
  /** The Flowfile's text; 'deny' makes the read fail. */
  source?: string | 'deny'
  mtimeMs?: number
}
const world: World = {}
const limits: unknown[] = []

const stub = (on: any, w: World, seen: string[][]) => {
  Object.assign(world, { graph: GRAPH(), source: 'name: deploy\nsteps: []\n', mtimeMs: 1, ...w })
  on('process.run', (_$: unknown, e: { argv: string[]; init?: { timeoutMs?: number }; timeoutMs?: number }) => {
    seen.push(e.argv)
    if (e.argv[1] === 'graph') {
      limits.push(e.init?.timeoutMs ?? e.timeoutMs)
      if (world.graph === 'deny') return { deny: 'timed out' }
      return { value: { ...world.graph!, isStdoutTruncated: false, isStderrTruncated: false } }
    }
    return { value: { ...fail(`no ${e.argv[1]} stubbed`), isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('fs.list', (_$: unknown, e: { path: string }) => ({ value: /workflows$/.test(e.path ?? '') ? [] : [entry('deploy.flow.yaml', 'file', world.mtimeMs)] }))
  on('fs.read', () => (world.source === 'deny' ? { deny: 'unreadable' } : { value: world.source }))
  on('ui.status', () => ({ value: undefined }))
  on('env.get', () => ({ value: undefined }))
}
const graphs = (seen: string[][]) => seen.filter(a => a[1] === 'graph')
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')

const open = async ($: any, on: any, w: World = {}, pick = 'deploy.flow.yaml') => {
  const seen: string[][] = []
  stub(on, w, seen)
  const ui = await mount($)
  if (pick) await ui.select({ plugin: 'flowstate', key: 'run-file', value: pick })
  return { ui, seen }
}

test('with no Flowfile chosen the Graph section is an empty state naming the one command, and nothing runs', async ($, on) => {
  const { ui, seen } = await open($, on, {}, '')
  const all = await texts(ui)
  expect(all).toMatch(/Graph/)
  expect(all).toMatch(/flow graph -o json --workflow NAME -- FILE/)
  expect(await ui.find({ type: 'Button', text: /Refresh graph/ })).toBeUndefined()
  expect(graphs(seen)).toEqual([])
  await ui.unmount()
})

test('choosing a Flowfile reads its steps with exactly flow graph -o json --workflow NAME -- FILE, 10 s, and draws the rows', async ($, on) => {
  const { ui, seen } = await open($, on)
  expect(graphs(seen)).toEqual([['flow', 'graph', '-o', 'json', '--workflow', 'deploy', '--', 'deploy.flow.yaml']])
  expect(limits.at(-1)).toBe(10000)
  expect(graphs(seen)[0]).not.toContain('--live')
  const all = await texts(ui)
  expect(all).toMatch(/deploy\.flow\.yaml: 3 steps as declared/)
  expect(all).toMatch(/○ build — task "log"/)
  expect(all).toMatch(/○ gate — switch/)
  expect(all).toMatch(/ {4}○ ship — task "log"/)
  await ui.unmount()
})

test('the read is cached by file and modification time: a redraw does not run it again, a changed file does', async ($, on) => {
  const { ui, seen } = await open($, on)
  await ui.input({ plugin: 'flowstate', key: 'filter', text: 'status == "FAILED"', kind: 'submit' })
  expect(graphs(seen)).toHaveLength(1)
  await ui.unmount()
  world.mtimeMs = 2
  const again = await mount($)
  await again.select({ plugin: 'flowstate', key: 'run-file', value: 'deploy.flow.yaml' })
  expect(graphs(seen)).toHaveLength(2)
  await again.unmount()
})

test('a failed read is cached too, shows one reason line, and Refresh reads again', async ($, on) => {
  const { ui, seen } = await open($, on, { graph: fail('ERROR\nno workflow named "deploy"; the files declare: other\n') })
  expect(await texts(ui)).toMatch(/Graph unavailable \(no workflow named "deploy"; the files declare: other\)\./)
  await ui.input({ plugin: 'flowstate', key: 'filter', text: 'x', kind: 'submit' })
  expect(graphs(seen)).toHaveLength(1)
  world.graph = GRAPH()
  await ui.press({ plugin: 'flowstate', key: 'refresh-graph' })
  expect(graphs(seen)).toHaveLength(2)
  expect(await texts(ui)).toMatch(/○ build/)
  await ui.unmount()
})

test('a timeout reads unreadable, and the rest of the pane (the run form, the Flowfiles list) is unaffected', async ($, on) => {
  const { ui } = await open($, on, { graph: 'deny' })
  const all = await texts(ui)
  expect(all).toMatch(/Graph unavailable \(/)
  expect(all).toMatch(/Run a Flowfile/)
  expect(all).toMatch(/No Flowfile edited yet this session/)
  await ui.unmount()
})

test('a partial graph is labelled partial and its notes are shown', async ($, on) => {
  const { ui } = await open($, on, { graph: GRAPH({ partial: true, notes: ['other.yaml does not compile and was left out'] }) })
  const all = await texts(ui)
  expect(all).toMatch(/; partial/)
  expect(all).toMatch(/note: other\.yaml does not compile and was left out/)
  await ui.unmount()
})

for (const [why, source] of [['no top-level name', 'steps: []\n'], ['a flag-like name', 'name: --live\n'], ['an unreadable file', 'deny']] as const) {
  test(`a file with ${why} runs nothing and says why`, async ($, on) => {
    const { ui, seen } = await open($, on, { source })
    expect(graphs(seen)).toEqual([])
    expect(await texts(ui)).toMatch(/Graph unavailable \(/)
    await ui.unmount()
  })
}

test('malformed output reads unreadable, never a half graph', async ($, on) => {
  const { ui } = await open($, on, { graph: ok('{"nodes": [') })
  const all = await texts(ui)
  expect(all).toMatch(/Graph unavailable \(the output was not a JSON graph\)/)
  expect(all).not.toMatch(/○/)
  await ui.unmount()
})
