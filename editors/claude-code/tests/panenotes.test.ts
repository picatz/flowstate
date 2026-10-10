import { expect, test } from 'claude-code/testing'
import { MAX_NOTES, PaneNotes } from '../hooks/panenotes'

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
