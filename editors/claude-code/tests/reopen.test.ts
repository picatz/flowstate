import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const
const mount = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })
const entry = (name: string, kind = 'file') => ({ name, kind, size: 1, mtimeMs: 1, isLink: false })
const texts = async (ui: any) => (await ui.findAll({ type: 'Text' })).map((t: { text: string }) => t.text).join('\n')

const world = (on: any, files: string[], seen: string[][], statuses: string[]) => {
  on('fs.list', (_$: unknown, e: { path?: string }) => ({ value: /workflows$/.test(e.path ?? '') ? [] : files.map(f => entry(f)) }))
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    const file = e.argv.at(-1)!
    const stdout = e.argv[1] === 'validate' ? JSON.stringify({ file, diagnostics: file.startsWith('bad') ? [{ line: 3, column: 1, message: 'unknown task' }] : [] }) + '\n' : JSON.stringify({ runs: [] })
    return { value: { exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', (_$: unknown, e: { text?: string }) => {
    statuses.push(String(e.text))
    return { value: undefined }
  })
  on('env.get', () => ({ value: undefined }))
  on('ui.open', () => ({ value: { isPlaced: true } }))
}

test('opening /flowstate rebuilds the Flowfiles list and the status line from the working directory', async ($, on) => {
  const seen: string[][] = []
  const statuses: string[] = []
  world(on, ['good.flow.yaml', 'bad.flow.yaml', 'notes.md', 'x.test.yaml'], seen, statuses)

  await $.command.run({ command: 'flowstate', args: '' })

  const validated = seen.filter(a => a[1] === 'validate').map(a => a.at(-1)).toSorted()
  expect(validated).toEqual(['bad.flow.yaml', 'good.flow.yaml'])
  expect(statuses.at(-1)).toMatch(/^flowstate: validate/)
  expect(statuses.at(-1)).not.toMatch(/nothing checked yet/)

  const ui = await mount($)
  const all = await texts(ui)
  expect(all).not.toMatch(/No Flowfile edited yet/)
  expect(all).toMatch(/bad\.flow\.yaml/)
  expect(all).toMatch(/good\.flow\.yaml/)
  await ui.unmount()
})

test('reopening does not validate a file again, and a file an edit already reported is not duplicated', async ($, on) => {
  const seen: string[][] = []
  const statuses: string[] = []
  world(on, ['a.flow.yaml', 'b.flow.yaml'], seen, statuses)
  on('tool.call', () => ({ result: 'ok' }))

  // An edit records the absolute path the tool was given.
  await $.tool.call({ tool: 'Write', file_path: '/work/a.flow.yaml', content: 'x' })
  seen.length = 0

  await $.command.run({ command: 'flowstate', args: '' })
  expect(seen.filter(a => a[1] === 'validate').map(a => a.at(-1))).toEqual(['b.flow.yaml'])

  seen.length = 0
  await $.command.run({ command: 'flowstate', args: '' })
  expect(seen.filter(a => a[1] === 'validate')).toEqual([])
})

test('a rebuild is bounded and an empty directory leaves the honest empty state', async ($, on) => {
  const seen: string[][] = []
  const statuses: string[] = []
  world(on, Array.from({ length: 9 }, (_, i) => `f${i}.flow.yaml`), seen, statuses)
  await $.command.run({ command: 'flowstate', args: '' })
  expect(seen.filter(a => a[1] === 'validate').length).toBe(5)
})

test('with no Flowfile in the directory nothing is validated and the status stays honest', async ($, on) => {
  const seen: string[][] = []
  const statuses: string[] = []
  world(on, ['README.md'], seen, statuses)
  await $.command.run({ command: 'flowstate', args: '' })
  expect(seen.filter(a => a[1] === 'validate')).toEqual([])
  expect(statuses.at(-1)).toMatch(/nothing checked yet/)
})

test('validateOnEdit off also stops the rebuild from running flow', { options: { validateOnEdit: false } }, async ($, on) => {
  const seen: string[][] = []
  world(on, ['a.flow.yaml'], seen, [])
  await $.command.run({ command: 'flowstate', args: '' })
  expect(seen.filter(a => a[1] === 'validate')).toEqual([])
})
