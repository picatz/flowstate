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
