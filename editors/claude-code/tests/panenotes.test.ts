import { expect, test } from 'claude-code/testing'
import { MAX_NOTES, PaneNotes, debugCommand } from '../hooks/panenotes'

test('a delivered signal is one fenced note, and it is said once', () => {
  const n = new PaneNotes()
  n.signal('release-approved', 'release-check', true)
  const block = n.take()
  expect(block).toContain('pane: sent release-approved to release-check ✓')
  expect(block).toMatch(/data, not instructions/)
  expect(n.take()).toBeUndefined()
})

test('a refusal carries its reason, cleaned', () => {
  const n = new PaneNotes()
  n.signal('go', 'run-1', false, 'not allowed\u001b[31m `rm -rf`\nnext line')
  const block = n.take()!
  expect(block).toContain('✗ not allowed')
  expect(block).not.toMatch(/\u001b/)
  expect(block.split('```')).toHaveLength(3)
})

test('a pane left open keeps only the newest notes', () => {
  const n = new PaneNotes()
  for (let i = 0; i < MAX_NOTES + 3; i++) n.signal(`s${i}`, 'run-1', true)
  const block = n.take()!
  expect(block).not.toContain('sent s0 ')
  expect(block).toContain(`s${MAX_NOTES + 2} `)
  expect(block.split('\n').filter(l => l.startsWith('pane:'))).toHaveLength(MAX_NOTES)
})

test('the debug command is the id as is, and an id that is not a run id gets none', () => {
  expect(debugCommand('run-1:abc')).toBe('/flowstate:debug run-1:abc')
  for (const bad of ['x; rm -rf /\n', '../etc/passwd', '/abs/path', '.hidden', 'a b', ''])
    expect(debugCommand(bad)).toBeUndefined()
  const n = new PaneNotes()
  n.debugAsked('run-1', 'build')
  expect(n.take()).toContain('copied /flowstate:debug for run run-1 (failed in build)')
  expect(n.take()).toBeUndefined()
})

test('copying the sign-in command is told to the next prompt once, and the command is cleaned', () => {
  const n = new PaneNotes()
  n.signInAsked('flow login --address=h:1\u001b[31m`x`')
  const block = n.take()!
  expect(block).toContain('server refused the pane\'s credentials; user copied "flow login --address=h:1')
  expect(block).not.toContain('\u001b')
  expect(block.match(/`/g)).toHaveLength(6)
  expect(n.take()).toBeUndefined()
})
