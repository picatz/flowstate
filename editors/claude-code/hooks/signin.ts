import { DEFAULT_ADDRESS } from './guard'

/** An address safe to put in a command the person pastes into a terminal: a host or IPv4 with an optional port, or a bracketed IPv6. */
const COPYABLE_ADDRESS = /^(?:[A-Za-z0-9][A-Za-z0-9._-]{0,253}|\[[0-9A-Fa-f:.]{2,45}\])(?::[0-9]{1,5})?$/
/** A `flow` the person's shell resolves as one word. */
const COPYABLE_BINARY = /^[A-Za-z0-9._/-]{1,200}$/

/**
 * The command that signs in to the server the pane is reading, for the person to run in their own terminal: `flow login` is
 * the device grant (RFC 8628), which prints a URL and a code and waits for approval in a browser for minutes, longer than a mod's
 * process can run and not something the pane should hold credentials for. Undefined when the address is not one that is safe to
 * paste, so the card then names the verb and no button. An unset `FLOWSTATE_ADDRESS` is the CLI's default address, as it is for
 * the reads that were just refused.
 */
export const signInCommand = (flow: string, address: string): string | undefined => {
  const target = address === '' ? DEFAULT_ADDRESS : address
  return COPYABLE_ADDRESS.test(target) && COPYABLE_BINARY.test(flow) ? `${flow} login --address=${target}` : undefined
}

/** What the card says, one line, for a server that refused the pane's credentials. */
export const SIGN_IN_LINE =
  'The server did not accept your credentials. Run flow login in a terminal (a flow server dev --auth stack printed its own sign command at startup), then the pane reads again.'
