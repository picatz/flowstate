import { expect, test } from 'claude-code/testing'

const PASSING = JSON.stringify({ files: [{ file: 'w.test.yaml', cases: [{ name: 'a', passed: true, failures: [] }], refused: '', coverage: [{ unreached: ['s9'] }], skipped: [] }] })
const FAILING = JSON.stringify({
  files: [{ file: 'w.test.yaml', cases: [{ name: 'wrong output', passed: false, failures: [{ line: 18, message: 'expected false, got true' }] }], refused: '', coverage: [], skipped: [] }],
})

// Answer the Bash call with the given result; every process the band could start is recorded.
const stub = (on: any, bash: { stdout: string; isError?: boolean; interrupted?: boolean; persistedOutputPath?: string; timedOutAfterMs?: number }) => {
  const argvs: string[][] = []
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    argvs.push(e.argv)
    return { value: { exitCode: 0, stdout: '{"file":"a.flow.yaml","diagnostics":[]}\n', stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('tool.call', (_$: unknown, e: { tool: string }) =>
    e.tool === 'Bash'
      ? { result: { stdout: bash.stdout, stderr: '', interrupted: bash.interrupted ?? false, persistedOutputPath: bash.persistedOutputPath, timedOutAfterMs: bash.timedOutAfterMs }, ...(bash.isError ? { isError: true } : {}) }
      : { result: 'ok' },
  )
  on('turn.start', () => ({ turnId: 't' }))
  on('fs.list', () => ({ value: [] }))
  on('ui.status', () => ({ value: undefined }))
  // The engine's own band is empty when the plugin hands the draw on.
  on('ui.render', () => ({ type: 'Box', props: {}, children: [] }))
  return { argvs }
}
const band = ($: any) => $.ui.mount({ plugin: 'flowstate', surface: 'terminal', component: 'AbovePrompt', props: { hasSurvey: false }, requestId: 'band', viewport: { columns: 100, rows: 20 } })
const flowTest = ($: any, command = 'flow test -o json .') => $.tool.call({ tool: 'Bash', command })

test('a passing flow test shows the pass, the uncovered steps, and starts nothing', async ($, on) => {
  const { argvs } = stub(on, { stdout: PASSING })
  await flowTest($)
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /✓ passed.*✓ 1 passed.*– 1 uncovered/ })).toBeDefined()
  expect(argvs).toEqual([])
  await ui.unmount()
})

test('a failing flow test names the case, where, and why', async ($, on) => {
  stub(on, { stdout: FAILING, isError: true })
  await flowTest($)
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /✗ failed.*✗ 1 failed/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /✗ failed wrong output \(w\.test\.yaml:18\): expected false, got true/ })).toBeDefined()
  await ui.unmount()
})

test('a run that is not a test run earns no band: --list, --help, a pipe, another verb', async ($, on) => {
  stub(on, { stdout: PASSING })
  for (const c of ['flow test --list .', 'flow test --help', 'flow test -o json . | head', 'flow validate .', 'ls']) await flowTest($, c)
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /test/ })).toBeUndefined()
  await ui.unmount()
})

test('text output is shown as the exit status alone, not as case detail', async ($, on) => {
  stub(on, { stdout: 'PASS  w.test.yaml: a\n' })
  await flowTest($, 'flow test .')
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /✓ passed · exit 0, no case detail/ })).toBeDefined()
  await ui.unmount()
})

test('an interrupted run is unknown, not a pass', async ($, on) => {
  stub(on, { stdout: PASSING, interrupted: true })
  await flowTest($)
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /\? unknown · the run did not finish/ })).toBeDefined()
  await ui.unmount()
})

test('an edit of a Flowfile or a test file clears the band; another file does not', async ($, on) => {
  stub(on, { stdout: PASSING })
  for (const [path, cleared] of [['notes.md', false], ['a.flow.yaml', true], ['w.test.yaml', true]] as const) {
    await flowTest($)
    await $.tool.call({ tool: 'Edit', file_path: path, old_string: 'x', new_string: 'y' })
    const ui = await band($)
    expect((await ui.find({ type: 'Text', text: /test/ })) === undefined).toBe(cleared)
    await ui.unmount()
  }
})

test('the Hide button clears the band', async ($, on) => {
  stub(on, { stdout: PASSING })
  await flowTest($)
  const ui = await band($)
  await ui.press({ key: 'hide' })
  expect(await ui.find({ type: 'Text', text: /test/ })).toBeUndefined()
  await ui.unmount()
})

test('a chained command is never read as the tests: its exit status and output are the chain\'s', async ($, on) => {
  // Every case passed, but `false` made the chain exit non-zero: not a failed run, not a pass.
  stub(on, { stdout: PASSING, isError: true })
  await flowTest($, 'flow test -o json . && false')
  let ui = await band($)
  expect(await ui.find({ type: 'Text', text: /\? unknown · chained command; run flow test on its own/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /✗ failed|✓ passed/ })).toBeUndefined()
  await ui.unmount()
  // The tests never ran: no test failure to show.
  await flowTest($, 'cd missing && flow test .')
  ui = await band($)
  expect(await ui.find({ type: 'Text', text: /✗ failed/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /chained command/ })).toBeDefined()
  await ui.unmount()
})

test('a chained run replaces an earlier verdict rather than leaving it', async ($, on) => {
  stub(on, { stdout: PASSING })
  await flowTest($)
  await flowTest($, 'flow validate . && flow test -o json .')
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /✓ passed/ })).toBeUndefined()
  await ui.unmount()
})

test('partial output and a timed-out run are unknown, not a pass', async ($, on) => {
  stub(on, { stdout: PASSING, persistedOutputPath: '/tmp/out.txt' })
  await flowTest($)
  let ui = await band($)
  expect(await ui.find({ type: 'Text', text: /\? unknown · the result could not be read/ })).toBeDefined()
  await ui.unmount()
  await flowTest($, 'flow test -o json ./other')
  ui = await band($)
  expect(await ui.find({ type: 'Text', text: /✓ passed/ })).toBeUndefined()
  await ui.unmount()
})

test('a timed-out run is unknown', async ($, on) => {
  stub(on, { stdout: PASSING, timedOutAfterMs: 120000 })
  await flowTest($)
  const ui = await band($)
  expect(await ui.find({ type: 'Text', text: /\? unknown · the run did not finish/ })).toBeDefined()
  await ui.unmount()
})
