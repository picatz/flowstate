import { SIGN_IN_LINE, signInCommand } from '../hooks/signin'
import { expect, test } from 'claude-code/testing'

test('the sign-in command targets the address the pane read, or the CLI default when none is set', () => {
  expect(signInCommand('flow', 'flowstate.example.com:9233')).toBe('flow login --address=flowstate.example.com:9233')
  expect(signInCommand('flow', '')).toBe('flow login --address=localhost:9233')
  expect(signInCommand('flow', '[::1]:9233')).toBe('flow login --address=[::1]:9233')
  expect(signInCommand('/opt/flow/bin/flow', '10.0.0.5')).toBe('/opt/flow/bin/flow login --address=10.0.0.5')
})

test('an address or binary that is not safe to paste into a shell gets no command', () => {
  for (const address of ['a b', 'x;rm -rf ~', '$(id)', '`id`', '-x', 'a|b', 'a\nb', 'host:99999999', 'h/o', 'a@b', '[::1', 'x'.repeat(300)]) {
    expect(signInCommand('flow', address)).toBeUndefined()
  }
  for (const flow of ['fl ow', 'flow;id', '$(flow)', '', '../'.repeat(100)]) expect(signInCommand(flow, 'h:1')).toBeUndefined()
})

test('the card tells the person what to do without carrying a credential', () => {
  expect(SIGN_IN_LINE).toMatch(/flow login/)
  expect(SIGN_IN_LINE).not.toMatch(/token=|bearer [A-Za-z0-9]/i)
})
