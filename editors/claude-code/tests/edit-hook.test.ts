import { expect, test } from 'claude-code/testing'

const clean = '{"file":"a.flow.yaml","diagnostics":[]}\n'
const broken =
  '{"file":"a.flow.yaml","diagnostics":[{"line":4,"column":5,"message":"unknown task"}]}\n'

// Answer the edit itself and `flow validate` in Claude Code's place.
const stub = (on: any, stdout: string, seen: string[][] = []) => {
  on('tool.call', () => ({ result: 'ok' }))
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    return { value: { exitCode: stdout === clean ? 0 : 1, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
}

test('an edit that breaks a Flowfile tells the model what is wrong', async ($, on) => {
  const seen: string[][] = []
  stub(on, broken, seen)

  const ran = await $.tool.call({ tool: 'Edit', file_path: 'a.flow.yaml', old_string: 'x', new_string: 'y' })

  expect(seen[0]).toEqual(['flow', 'validate', '-o', 'jsonl', '--', 'a.flow.yaml'])
  expect((ran.context ?? []).join('\n')).toContain('line 4: unknown task')
})

test('a clean Flowfile adds nothing for the model to read', async ($, on) => {
  stub(on, clean)

  const ran = await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })

  expect(ran.context ?? []).toEqual([])
})

test('files that are not Flowfiles are never validated', async ($, on) => {
  const seen: string[][] = []
  stub(on, clean, seen)

  await $.tool.call({ tool: 'Write', file_path: 'notes.md', content: 'x' })
  await $.tool.call({ tool: 'Write', file_path: 'workflow.test.yaml', content: 'x' })

  expect(seen).toEqual([])
})

test('the flow binary is configurable', { options: { flowBinary: '/opt/flow' } }, async ($, on) => {
  const seen: string[][] = []
  stub(on, clean, seen)

  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })

  expect(seen[0][0]).toBe('/opt/flow')
})

test('validation can be turned off', { options: { validateOnEdit: false } }, async ($, on) => {
  const seen: string[][] = []
  stub(on, broken, seen)

  const ran = await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })

  expect(seen).toEqual([])
  expect(ran.context ?? []).toEqual([])
})

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const

test('the pane lists the newest Flowfiles and stays bounded', async ($, on) => {
  stub(on, clean)
  for (let i = 0; i < 60; i++) {
    await $.tool.call({ tool: 'Write', file_path: `f${i}.flow.yaml`, content: 'x' })
  }
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Text', text: /f59\.flow\.yaml/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /f10\.flow\.yaml/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /f9\.flow\.yaml/ })).toBeUndefined()
  expect(await ui.find({ type: 'Text', text: /f0\.flow\.yaml/ })).toBeUndefined()
  await ui.unmount()
})

test('the pane shows a problem with its line', async ($, on) => {
  stub(on, broken)
  await $.tool.call({ tool: 'Write', file_path: 'a.flow.yaml', content: 'x' })
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Text', text: /a\.flow\.yaml invalid \(1\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /4:5 unknown task/ })).toBeDefined()
  await ui.unmount()
})
