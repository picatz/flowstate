import { expect, test } from 'claude-code/testing'

const PANE = { component: 'Pane', props: {}, requestId: 'flowstate', viewport: { columns: 100, rows: 60 } } as const

const running = '{"workflowId":"nightly-1","runId":"r1","status":"STATUS_RUNNING","name":"nightly-etl"}\n'
const failed = '{"workflowId":"w-2","runId":"r2","status":"STATUS_FAILED"}\n'

// Answer `flow list` only; no Flowfile is edited in these tests.
const stub = (on: any, reply: { exitCode: number; stdout: string; stderr: string }, seen: string[][] = []) => {
  on('process.run', (_$: unknown, e: { argv: string[] }) => {
    seen.push(e.argv)
    return { value: { ...reply, isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', () => ({ value: undefined }))
}

test('the pane lists the runs a server reports', async ($, on) => {
  const seen: string[][] = []
  stub(on, { exitCode: 0, stdout: running + failed, stderr: '' }, seen)
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(seen[0]).toEqual(['flow', 'list', '-o', 'jsonl'])
  expect(await ui.find({ type: 'Text', text: /running nightly-etl \(nightly-1\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /failed w-2/ })).toBeDefined()
  await ui.unmount()
})

test('with no server the pane says so and local use is untouched', async ($, on) => {
  stub(on, {
    exitCode: 1,
    stdout: '',
    stderr: 'ERROR\nno Flowstate server answered at 127.0.0.1:7233\n',
  })
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Text', text: /No server answering \(no Flowstate server answered/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /No Flowfile edited yet/ })).toBeDefined()
  await ui.unmount()
})

test('a stray line does not hide the runs and the list stays bounded', async ($, on) => {
  const many = Array.from({ length: 20 }, (_, i) => `{"workflowId":"w${i}","status":"STATUS_COMPLETED"}\n`).join('')
  stub(on, { exitCode: 0, stdout: 'warning: slow\n' + many, stderr: '' })
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Text', text: /completed w0$/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /completed w7$/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /completed w8$/ })).toBeUndefined()
  await ui.unmount()
})

test('terminal control characters in a name or an error never reach the pane', async ($, on) => {
  stub(on, { exitCode: 0, stdout: '{"workflowId":"w1","status":"STATUS_RUNNING","name":"a\\u001b[31mred\\u009b"}\n', stderr: '' })
  const ui = await $.ui.mount({ plugin: 'flowstate', surface: 'terminal', ...PANE })

  expect(await ui.find({ type: 'Text', text: /running a\[31mred \(w1\)/ })).toBeDefined()
  expect(await ui.find({ type: 'Text', text: /[\u001b\u009b]/ })).toBeUndefined()
  await ui.unmount()
})
