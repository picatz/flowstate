import { clean } from './runs'

/** The address `flow` falls back to when neither `--address` nor `FLOWSTATE_ADDRESS` names one. */
export const DEFAULT_ADDRESS = 'localhost:9233'

/** What a command may name before the guard stops listing it; the rest are counted. */
const MAX_LISTED = 5

/** Wrappers can nest (`sudo bash -c "env X=1 flow run ..."`), but not without bound. */
const MAX_DEPTH = 3

/**
 * The verbs that change the world on a server, from `flow --help`. `run` is
 * the server venue unless it is `run local`; `schedule` is gated by subcommand
 * so `schedule list` and `describe` stay free. Everything else (validate, fmt,
 * lint, test, tasks, compile, timeline, list, graph, get, watch) reads.
 */
const SERVER_VERBS = new Set(['run', 'signal', 'cancel', 'terminate'])
const SCHEDULE_CHANGES = new Set(['create', 'delete', 'pause', 'resume', 'trigger'])

/** One server-side verb found in a command. */
export interface ServerAction {
  /** `flow run`, `flow schedule delete`, ... */
  verb: string
  /** The `--address` or `FLOWSTATE_ADDRESS=` the command itself set; absent when the session's environment decides. */
  address?: string
  /** The run or schedule the verb names, when it is the first argument. */
  subject?: string
  /** True when a bare `FLOWSTATE_ADDRESS=x` earlier in the command may have changed the address, so the session's is not known to apply. */
  addressMayDiffer?: boolean
}

/** What a shell command does to a server. */
export interface Analysis {
  actions: ServerAction[]
  /** True when the command may run a server verb the parser could not see: a substitution, an `eval`, an unbalanced quote. */
  uncertain: boolean
}

interface Tokens {
  segments: string[][]
  /** Parallel to `segments`: whether a word began inside quotes or after a backslash, so a leading `>` in it is text, not a redirection. */
  quoted: boolean[][]
  /** False when the text used a construct this tokenizer does not follow. */
  exact: boolean
}

/**
 * Splits a shell command into simple commands and their words. It follows
 * quotes, backslashes, comments, and the separators `; & | ( )` and newline,
 * and nothing else: a `$(...)`, a backtick, or an unterminated quote makes the
 * result inexact, so the caller can ask instead of trusting what it saw.
 */
export const tokenize = (raw: string): Tokens => {
  const segments: string[][] = []
  const quoted: boolean[][] = []
  let words: string[] = []
  let flags: boolean[] = []
  let word = ''
  let inWord = false
  let startQuoted = false
  let exact = true

  const endWord = () => {
    if (inWord) (words.push(word), flags.push(startQuoted))
    word = ''
    inWord = false
    startQuoted = false
  }
  const endSegment = () => {
    endWord()
    if (words.length > 0) (segments.push(words), quoted.push(flags))
    words = []
    flags = []
  }

  for (let i = 0; i < raw.length; i++) {
    const c = raw[i]
    if (c === '\\') {
      if (raw[i + 1] === '\n') i++
      else if (i + 1 < raw.length) {
        if (!inWord) startQuoted = true
        word += raw[++i]
        inWord = true
      }
      continue
    }
    if (c === "'" || c === '"') {
      const close = c
      if (!inWord) startQuoted = true
      inWord = true
      i++
      while (i < raw.length && raw[i] !== close) {
        if (close === '"' && raw[i] === '\\' && i + 1 < raw.length) {
          const next = raw[i + 1]
          // Inside double quotes a backslash only escapes these four.
          if ('"\\$`'.includes(next)) i++
          else if (next === '\n') {
            i += 2
            continue
          }
        } else if (close === '"' && (raw[i] === '`' || (raw[i] === '$' && raw[i + 1] === '('))) {
          exact = false
        }
        word += raw[i++]
      }
      if (i >= raw.length) exact = false
      continue
    }
    if (c === '`' || (c === '$' && raw[i + 1] === '(')) exact = false
    if (c === '#' && !inWord) {
      while (i < raw.length && raw[i] !== '\n') i++
      i--
      continue
    }
    // `2>&1`, `>&2`, and `&>file` redirect; the `&` there is not a separator.
    const redirecting = c === '&' && (raw[i - 1] === '>' || raw[i - 1] === '<' || raw[i + 1] === '>')
    if (c === ' ' || c === '\t') endWord()
    else if (!redirecting && ';&|()\n'.includes(c)) endSegment()
    else (word += c), (inWord = true)
  }
  endSegment()
  return { segments, quoted, exact }
}

export const basename = (path: string): string => path.replace(/^.*[\\/]/, '')
const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/
/** A redirection operator with a word attached (`2>log`, `>>out`, `2>&1`) or standing alone (`>`, `<<<`, `&>`). */
const REDIRECT = /^(?:\d*|&)[<>]/
const REDIRECT_ALONE = /^(?:\d*|&)[<>]+&?$/
/** Words that precede a command without being one. */
const KEYWORDS = new Set(['{', '}', '!', 'if', 'then', 'else', 'elif', 'while', 'until', 'do', 'time', 'command', 'exec', 'nohup', 'builtin'])
const SHELLS = new Set(['sh', 'bash', 'zsh', 'dash', 'ksh'])
const INTERPRETER = /^(?:python[0-9.]*|node|perl|ruby)$/
/** Commands whose arguments are text to show or search, never a command to run. */
const DISPLAY = new Set(['echo', 'printf', 'cat', 'grep', 'egrep', 'fgrep', 'rg', 'man', 'which', 'whereis', 'type', 'ls', 'head', 'tail', 'less', 'more', 'wc'])

/** Drops redirections so they cannot hide a verb: `flow 2>/dev/null run x`, `>out flow run x`. */
const withoutRedirects = (words: string[], quoted: boolean[]): { words: string[]; odd: boolean } => {
  const kept: string[] = []
  let odd = false
  let evals = false
  for (let i = 0; i < words.length; i++) {
    const w = words[i]
    // A quoted word is text, the script after `-c` is a command, and `eval` runs its arguments: none is a redirection.
    const text = quoted[i] || evals || /^-[A-Za-z]*c[A-Za-z]*$/.test(kept[kept.length - 1] ?? '')
    if (w === 'eval') evals = true
    if (text || !REDIRECT.test(w)) kept.push(w)
    else if (REDIRECT_ALONE.test(w)) i++
    // A dropped target that holds whitespace or a separator may have swallowed a command.
    else if (/[\s;&|()]/.test(w.replace(/^(?:\d*|&)[<>]+&?/, ''))) odd = true
  }
  return { words: kept, odd }
}

/** A word that holds whitespace is a command line, never an assignment or an option. */
const hasSpace = (w: string): boolean => /\s/.test(w)
/** Drops an option glued to the command line it carries: `-S"flow run x"`, `--split-string=...`, `-vS...`, `-c...`, a git alias `alias.x=!...`. */
const withoutOptionPrefix = (w: string): string => w.replace(/^(?:--split-string=|-[A-Za-z]*[cS]|[\w.-]+=!)/, '')
/** Whether `env` is given `-S`/`--split-string`, which runs a command line the parse may not have followed. */
const splitsString = (words: string[]): boolean => {
  for (let i = 0; i < words.length; i++) {
    if (basename(words[i]) !== 'env') continue
    for (let j = i + 1; j < words.length && words[j].startsWith('-'); j++) {
      if (/^(?:--split-string(?:=|$)|-[A-Za-z]*S)/.test(words[j])) return true
    }
  }
  return false
}

const setEnv = (env: Map<string, string>, assignment: string) => {
  const eq = assignment.indexOf('=')
  env.set(assignment.slice(0, eq), assignment.slice(eq + 1))
}

/**
 * Strips what runs a command without being it: leading `VAR=value`, shell
 * keywords, `env`, `sudo`, `timeout`. A prefix assignment lands in `env`, which
 * is this command's alone; `export` also lands in `exported`, which later
 * commands inherit. A bare `VAR=value` is a shell variable `flow` never sees,
 * so it changes neither past the command it prefixes.
 */
const unwrap = (words: string[], env: Map<string, string>, exported: Map<string, string>): string[] => {
  let i = 0
  while (i < words.length) {
    const w = words[i]
    if (ASSIGNMENT.test(w) && !hasSpace(w)) {
      setEnv(env, w)
      i++
    } else if (KEYWORDS.has(w)) {
      i++
    } else if (w === 'export') {
      for (let j = i + 1; j < words.length; j++) {
        if (ASSIGNMENT.test(words[j])) (setEnv(env, words[j]), setEnv(exported, words[j]))
      }
      return []
    } else if (w === 'env' || w === 'sudo' || w === 'timeout') {
      i++
      // Options, and for `timeout` its duration; `env -u NAME` and `sudo -u user` take a value.
      while (i < words.length && !hasSpace(words[i]) && (words[i].startsWith('-') || (w === 'timeout' && /^\d/.test(words[i])))) {
        const takesValue = w !== 'timeout' && ['-u', '-C', '-g', '-h', '-p'].includes(words[i])
        i += takesValue ? 2 : 1
      }
    } else {
      break
    }
  }
  return i === 0 ? words : words.slice(i)
}

/** The last `--address` in an argument list; `--` ends the options. */
const addressFlag = (args: string[]): string | undefined => {
  let address: string | undefined
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--') break
    if (args[i] === '--address') address = args[i + 1] ?? address
    else if (args[i].startsWith('--address=')) address = args[i].slice('--address='.length)
  }
  return address
}

/** The server verb a `flow` argument list runs, if any. */
const classify = (args: string[], env: Map<string, string>): ServerAction | undefined => {
  // Global flags take no value except `--address`, so skipping dashes finds the verb.
  let i = 0
  while (args[i]?.startsWith('-')) i += args[i] === '--address' ? 2 : 1
  const verb = args[i]
  const rest = args.slice(i + 1)
  // Only a bare `--help`/`-h` right after the verb is a help request; later it may be a value.
  if (verb === undefined || rest[0] === '-h' || rest[0] === '--help') return undefined

  let name: string
  if (verb === 'schedule') {
    if (!SCHEDULE_CHANGES.has(rest[0] ?? '')) return undefined
    name = `schedule ${rest[0]}`
  } else if (SERVER_VERBS.has(verb)) {
    // Only the token right after `run` can be the `local` venue.
    if (verb === 'run' && rest[0] === 'local') return undefined
    name = verb
  } else {
    return undefined
  }

  const first = rest[verb === 'schedule' ? 1 : 0]
  return {
    verb: `flow ${name}`,
    address: addressFlag(args) ?? env.get('FLOWSTATE_ADDRESS'),
    subject: first !== undefined && !first.startsWith('-') ? first : undefined,
  }
}

const escapeRe = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

/** The index of the script a shell runs with `-c`: the word after the flag cluster that holds `c`; -1 when none. */
const shellScriptAt = (words: string[], from: number): number => {
  for (let i = from + 1; i < words.length - 1; i++) if (/^-[A-Za-z]*c[A-Za-z]*$/.test(words[i])) return i + 1
  return -1
}

/** Heads whose arguments are file names or text, never a command: the wrapper pass leaves them alone. */
const ARGS_ONLY = new Set(['cp', 'mv', 'rm', 'mkdir', 'touch', 'test', '['])
/** Git subcommands that never execute their arguments: what follows is a message, a path, or a ref. */
const GIT_TEXT_ONLY = new Set([
  'commit', 'log', 'show', 'diff', 'add', 'status', 'tag', 'branch', 'checkout', 'switch', 'restore', 'stash', 'push', 'pull', 'fetch', 'clone',
  'remote', 'config', 'blame', 'grep', 'describe', 'cherry-pick', 'merge', 'reset', 'rm', 'mv', 'apply', 'am', 'format-patch', 'shortlog',
])
const TEST_RUNNERS = new Set(['bun', 'npm', 'pnpm', 'yarn'])

/** A command longer than this is asked about, unread: the parse would spend more than the question is worth. */
export const MAX_COMMAND = 64 * 1024

/**
 * Finds the server-side `flow` verbs a Bash command runs. `flowBinary` is the
 * plugin's option; the bare name `flow` is always recognized too. The parse is
 * conservative: whatever it could not follow, but that still names the binary
 * beside a gated verb, is reported as `uncertain` so the caller asks.
 */
export const analyzeCommand = (command: string, flowBinary = 'flow'): Analysis => {
  const names = new Set(['flow', basename(flowBinary)])
  if (command.length > MAX_COMMAND) {
    return { actions: [], uncertain: [...names].some(n => command.includes(n)) }
  }
  const actions: ServerAction[] = []
  let uncertain = false
  // A bare `FLOWSTATE_ADDRESS=x` is invisible to `flow`, yet the variable may already be exported in the user's shell.
  let addressAssigned = false
  const add = (action: ServerAction | undefined) => {
    if (!action) return
    if (addressAssigned && action.address === undefined) action.addressMayDiffer = true
    actions.push(action)
  }
  const mentions = (w: string): boolean => [...names].some(n => w.includes(n))

  const walk = (text: string, depth: number, base: Map<string, string>) => {
    const { segments, quoted, exact } = tokenize(text)
    if (!exact) uncertain = true
    const exported = new Map(base)
    const recurse = (script: string, env: Map<string, string>) => {
      if (depth >= MAX_DEPTH) uncertain = true
      else walk(script, depth + 1, env)
    }
    for (let s = 0; s < segments.length; s++) {
      const env = new Map(exported)
      const stripped = withoutRedirects(segments[s], quoted[s])
      if (stripped.odd) uncertain = true
      if (stripped.words.every(w => ASSIGNMENT.test(w)) && stripped.words.some(w => w.startsWith('FLOWSTATE_ADDRESS='))) addressAssigned = true
      if (splitsString(stripped.words)) uncertain = true
      const words = unwrap(stripped.words, env, exported)
      if (words.length === 0) continue
      const head = basename(words[0])
      if (names.has(head)) {
        add(classify(words.slice(1), env))
        continue
      }
      if (SHELLS.has(head)) {
        const at = shellScriptAt(words, 0)
        // Without `-c` the script comes from a file or stdin (`echo "flow run x" | sh`).
        if (at < 0) uncertain = true
        else recurse(words[at], env)
        continue
      }
      if (head === 'eval') {
        recurse(words.slice(1).join(' '), env)
        continue
      }
      // The command comes from input or a variable, or from an interpreter's own code.
      if (head === 'xargs' || words[0].includes('$')) uncertain = true
      if (INTERPRETER.test(head) && words.some((w, i) => i > 0 && /^(?:-c|-e|-E|--eval)$/.test(w))) uncertain = true
      if (DISPLAY.has(head) || ARGS_ONLY.has(head)) continue
      if (TEST_RUNNERS.has(head) && words[1] === 'test') continue
      // `git commit -m "flow run x"` is text; `bisect run`, `rebase --exec`, `-c alias.x=!...` and `git flow` hand over to a command.
      if (head === 'git' && GIT_TEXT_ONLY.has(words[1] ?? '') && !(words[1] === 'config' && words.some(w => w.includes('alias')))) continue

      // The wrapper pass: the binary behind something this parser does not model
      // (`go run ./cmd/flow`, `nice -n 5 flow`, `ssh h flow`, `find -exec flow`),
      // or a command line passed as one word (`ssh h "flow run x"`, `env -S "flow run x"`).
      const consumed = new Set<number>()
      for (let i = 0; i < words.length; i++) {
        if (consumed.has(i)) continue
        const w = words[i]
        if (/\s/.test(w)) {
          if (mentions(w)) recurse(withoutOptionPrefix(w), env)
        } else if (i === 0) {
          continue
        } else if (names.has(basename(w))) {
          add(classify(words.slice(i + 1), env))
        } else if (SHELLS.has(basename(w))) {
          const at = shellScriptAt(words, i)
          if (at >= 0) {
            consumed.add(at)
            recurse(words[at], env)
          }
        }
      }
    }
  }
  walk(command, 0, new Map())

  if (!uncertain) return { actions, uncertain }
  // The parse is incomplete. Only a command that also names the binary beside a
  // gated verb is worth a question; `echo $(date)` is not.
  return { actions, uncertain: namesVerb(command, names) }
}

/**
 * Whether a line of the text runs the binary with a gated verb, or has a
 * variable standing in for the binary beside one. Each line is scanned once
 * per pattern, so the work is linear.
 */
const namesVerb = (text: string, names: Set<string>): boolean => {
  // The binary as a whole word, optional dash-options (`-v`, `--address host:1`), then a gated verb:
  // `flow run local` and `flow to run` are not one, and nothing but whitespace may sit between.
  const gated = `(?:run\\b(?![ \\t]+local\\b)|(?:signal|cancel|terminate)\\b|schedule[ \\t]+(?:${[...SCHEDULE_CHANGES].join('|')})\\b)`
  const tight = new RegExp(`(?:^|[^A-Za-z0-9_.-])(?:${[...names].map(escapeRe).join('|')})(?:[ \\t]+-\\S*(?:[ \\t]+[^-\\s]\\S*)?)*[ \\t]+${gated}`)
  // A variable standing for the binary (`$FLOW run x`) names no binary at all.
  const variable = new RegExp(`\\$\\{?\\w+\\}?[ \\t]+${gated}`)
  for (let start = 0; start <= text.length; ) {
    let end = text.indexOf('\n', start)
    if (end < 0) end = text.length
    // A line this long is not scanned (the patterns backtrack): naming the binary is reason enough to ask.
    if (end - start > MAX_LINE) {
      if ([...names].some(n => text.slice(start, end).includes(n))) return true
      start = end + 1
      continue
    }
    const line = text.slice(start, end)
    if (tight.test(line) || variable.test(line)) return true
    start = end + 1
  }
  return false
}

/**
 * The question put to the user. Names every verb and where it points; the
 * address is the command's own, then `FLOWSTATE_ADDRESS` from the session, then
 * the default. Text from the command is cleaned, since it reaches a terminal.
 */
export const askReason = (analysis: Analysis, sessionAddress: string | undefined): string => {
  const lines = analysis.actions.slice(0, MAX_LISTED).map(a => {
    const address = clean(a.address ?? sessionAddress ?? '', 120)
    const where = a.addressMayDiffer
      ? 'a server (address may be overridden in this command)'
      : address !== '' ? `server ${address}` : `the default server ${DEFAULT_ADDRESS}`
    const what = a.subject !== undefined && a.subject !== '' ? ` ${clean(a.subject, 80)}` : ''
    return `${clean(a.verb, 40)}${what} acts on ${where}`
  })
  const more = analysis.actions.length - lines.length
  if (more > 0) lines.push(`and ${more} more`)
  if (analysis.uncertain) {
    lines.push('this command could not be fully read, so it may also run a flow verb that changes a server')
  }
  return `Flowstate: ${lines.join('; ')}. Confirm before it changes anything. Local verbs (validate, test, run local) never ask.`
}

/** One apparent secret: where, and what kind. The value itself is never kept. */
export interface SecretFinding {
  line: number
  what: string
}

/**
 * Token shapes that no ordinary Flowfile text looks like. Each is a prefix
 * the issuer documents plus enough characters to rule out a word.
 */
export const TOKEN_SHAPES: readonly { name: string; pattern: RegExp }[] = [
  { name: 'a GitHub token', pattern: /\bgh[pousr]_[A-Za-z0-9]{36,}/ },
  { name: 'a GitHub fine-grained token', pattern: /\bgithub_pat_[A-Za-z0-9_]{22,}/ },
  { name: 'a Slack token', pattern: /\bxox[baprs]-[A-Za-z0-9-]{10,}/ },
  { name: 'an AWS access key id', pattern: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/ },
  { name: 'an API key (sk-)', pattern: /(?<![A-Za-z0-9])sk-(?:ant-|proj-)?[A-Za-z0-9_-]{20,}/ },
  { name: 'a private key', pattern: /-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----/ },
]

/** Most of a text the scan reads; a larger Flowfile edit is refused unread, since the work must be bounded where it is spent. */
export const MAX_SCAN = 256 * 1024
/** Most of one line the key scan reads; a longer line is searched for token shapes whole, and refused if it holds a credential key word. */
const MAX_LINE = 4096
/** Most body lines of a block scalar the scan reads. */
const MAX_BODY = 64

/** The finding for text over MAX_SCAN; line 0 marks it. */
export const TOO_LARGE: SecretFinding = { line: 0, what: 'text too large to scan' }

/** Where a token shape can begin; a long line is searched for these with `indexOf`, then each hit is matched in a bounded window. */
const SHAPE_PREFIXES: readonly (readonly [string, number])[] = [
  ['ghp_', 0], ['gho_', 0], ['ghu_', 0], ['ghs_', 0], ['ghr_', 0], ['github_pat_', 1], ['xox', 2], ['AKIA', 3], ['ASIA', 3], ['sk-', 4], ['-----BEGIN ', 5],
]
/** How much of a line after a prefix hit is matched against its shape. */
const SHAPE_WINDOW = 512
/** Any word that could name a credential key, found anywhere in a line too long to parse. */
const CREDENTIAL_WORD = /passw(?:or)?d|pwd|secret|token|credential|(?:api|access|private)[_.-]?key/i
export const LONG_LINE = 'line too long to scan'

/** Token shapes anywhere in a long line, in time linear in its length. */
const longLineShapes = (line: string, add: (what: string) => void) => {
  const done = new Set<number>()
  for (const [prefix, shape] of SHAPE_PREFIXES) {
    if (done.has(shape)) continue
    for (let at = line.indexOf(prefix); at >= 0; at = line.indexOf(prefix, at + 1)) {
      // One char of context before the hit, so `\b` and the look-behind see what precede it.
      const m = TOKEN_SHAPES[shape].pattern.exec(line.slice(Math.max(0, at - 1), at + SHAPE_WINDOW))
      if (m && looksReal(m[0])) {
        add(TOKEN_SHAPES[shape].name)
        done.add(shape)
        break
      }
    }
  }
}

/** A key that names a credential: `password`, `db_password`, `client-secret`, `token`, `secret_key`, or camelCase `apiKey`. */
const CREDENTIAL_KEY_SEP =
  /(?:^|[_.-])(?:password|passwd|pwd|secret|secret[_.-]?key|access[_.-]?key|private[_.-]?key|auth[_.-]?token|token|api[_.-]?key|credentials?|client[_.-]?secret|aws[_.-]?secret[_.-]?access[_.-]?key)$/i
const CREDENTIAL_KEY_CAMEL = /(?:^|[a-z0-9])(?:[Ss]ecret|[Aa]ccess|[Pp]rivate|[Aa]pi|[Cc]lient)(?:Key|Secret)$|[a-z0-9](?:Password|Passwd|Pwd|Secret|Token|Credentials?)$/
const isCredentialKey = (key: string): boolean => CREDENTIAL_KEY_SEP.test(key) || CREDENTIAL_KEY_CAMEL.test(key)
// No lazy quantifier before the end anchor: the value is trimmed by hand, so a long run of spaces costs one pass.
const KEY_VALUE = /^(\s*)(?:-\s+)?["']?([A-Za-z0-9_.-]+)["']?\s*:\s+(\S.*)$/
/** `{user: a, password: b}` and `{"password": "b"}`: a key after `{` or `,`, then a quoted value or one up to the next `,` or `}`. */
const INLINE_PAIR = /[{,]\s*["']?([A-Za-z0-9_.-]+)["']?\s*:\s*("[^"]*"|'[^']*'|[^,}\s][^,}]*)/g
const BLOCK_INDICATOR = /^[|>][+\-0-9]*(?:\s+#.*)?$/
/** What a value that is not a credential looks like: a placeholder, a bare number, an env-style constant, a path into the run. */
const PLACEHOLDER = /^(?:<.*>|\*+|x+|changeme|change-me|example|placeholder|todo|redacted|your[-_ ].*|true|false|null)$/i
const NOT_A_VALUE = /^(?:\d+|[A-Z][A-Z0-9_]*|[A-Za-z_]\w*(?:\.\w+|\[\w+\])+)$/

/** A value after the colon without quotes or a trailing comment; `undefined` for a block scalar, a flow collection, or an alias. */
const scalar = (value: string): { text: string; quoted: boolean } | undefined => {
  const q = value[0]
  if (q === '"' || q === "'") {
    const end = value.indexOf(q, 1)
    return end > 0 ? { text: value.slice(1, end), quoted: true } : undefined
  }
  if ('|>{[&*!'.includes(q)) return undefined
  // A comment starts at a `#` after whitespace.
  for (let i = value.indexOf('#'); i > 0; i = value.indexOf('#', i + 1)) {
    if (value[i - 1] === ' ' || value[i - 1] === '\t') return { text: value.slice(0, i).trimEnd(), quoted: false }
  }
  return { text: value.trimEnd(), quoted: false }
}

/** Whether a value is a credential's literal: long enough, no reference, no placeholder; an unquoted one with whitespace is prose. */
const isLiteral = (value: string, quoted: boolean): boolean =>
  value.length >= 8 && (quoted || !/\s/.test(value)) && !value.includes('${') && !NOT_A_VALUE.test(value) && !PLACEHOLDER.test(value)

const indentOf = (line: string): number => line.length - line.trimStart().length

/** Fewer than four distinct characters is a placeholder (`ghp_xxxx...`), not a token. */
const looksReal = (match: string): boolean => new Set(match.slice(-20)).size > 3

/**
 * Looks for secrets in text about to be written to a Flowfile: the token
 * shapes in TOKEN_SHAPES, and a credential-named key holding a literal: a
 * plain or quoted string, a block scalar, a value on the next line, or a pair in
 * an inline map. A `${...}` expression is never a finding, so
 * `${secret('env:TOKEN')}` passes. Text over MAX_SCAN is not read and yields
 * TOO_LARGE; a line over 4 KiB is searched whole for token shapes and, if it
 * holds a credential key word, refused as too long to scan.
 */
export const findSecrets = (text: string): SecretFinding[] => {
  if (text.length > MAX_SCAN) return [TOO_LARGE]
  const found: SecretFinding[] = []
  const seen = new Set<string>()
  const add = (line: number, what: string) => {
    if (!seen.has(`${line}:${what}`) && seen.add(`${line}:${what}`)) found.push({ line, what })
  }
  const lines = text.split('\n')
  const long = lines.map(l => l.length > MAX_LINE)
  const keyFinding = (key: string) => `the key ${clean(key, 40)} holding a literal value`

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    if (long[i]) {
      longLineShapes(line, what => add(i + 1, what))
      if (CREDENTIAL_WORD.test(line)) add(i + 1, LONG_LINE)
      continue
    }
    for (const { name, pattern } of TOKEN_SHAPES) {
      const m = pattern.exec(line)
      if (m && looksReal(m[0])) add(i + 1, name)
    }

    for (const pair of line.matchAll(INLINE_PAIR)) {
      const value = scalar(pair[2])
      if (isCredentialKey(pair[1]) && value !== undefined && isLiteral(value.text, value.quoted)) add(i + 1, keyFinding(pair[1]))
    }

    const kv = KEY_VALUE.exec(line)
    if (!kv || !isCredentialKey(kv[2])) continue
    const raw = kv[3].trimEnd()
    if (BLOCK_INDICATOR.test(raw)) {
      // The indented lines below are the value.
      const indent = kv[1].length
      for (let j = i + 1; j < lines.length && j <= i + MAX_BODY; j++) {
        const body = lines[j].trim()
        if (body === '') continue
        if (indentOf(lines[j]) <= indent) break
        if (isLiteral(body, true)) add(i + 1, keyFinding(kv[2]))
      }
      continue
    }
    const value = scalar(raw)
    if (value !== undefined && isLiteral(value.text, value.quoted)) add(i + 1, keyFinding(kv[2]))
  }

  // `token:` alone, the string on the next line.
  for (let i = 0; i + 1 < lines.length; i++) {
    const m = long[i] ? null : /^(\s*)(?:-\s+)?["']?([A-Za-z0-9_.-]+)["']?\s*:\s*$/.exec(lines[i])
    if (!m || !isCredentialKey(m[2])) continue
    for (let j = i + 1; j < lines.length && j <= i + 2; j++) {
      const next = lines[j].trim()
      if (next === '' || next.startsWith('#')) continue
      if (indentOf(lines[j]) > m[1].length && !next.startsWith('- ') && !KEY_VALUE.test(next) && !next.endsWith(':')) {
        const value = scalar(next)
        if (value !== undefined && isLiteral(value.text, value.quoted)) add(i + 1, keyFinding(m[2]))
      }
      break
    }
  }
  return found.sort((a, b) => a.line - b.line)
}

/** The text an Edit, Write, or MultiEdit puts into a file; anything else yields none. */
export const writtenText = (input: unknown): string[] => {
  if (typeof input !== 'object' || input === null) return []
  const { content, new_string, edits } = input as { content?: unknown; new_string?: unknown; edits?: unknown }
  const texts = [content, new_string]
  if (Array.isArray(edits)) for (const edit of edits) texts.push((edit as { new_string?: unknown } | null)?.new_string)
  return texts.filter((t): t is string => typeof t === 'string')
}

/** Most edits in one MultiEdit, and most of one edit's `old_string` or `new_string`, that are applied; more is refused as too large. */
export const MAX_EDITS = 64
export const MAX_EDIT_STRING = 64 * 1024
/** What `afterEdits` answers when applying the edits would pass MAX_SCAN or the edit limits. */
export const EDIT_TOO_LARGE = Symbol('edit too large')

const oversizedEdits = (input: unknown): boolean => {
  if (typeof input !== 'object' || input === null) return false
  const { old_string, new_string, edits } = input as Record<string, unknown>
  const list = Array.isArray(edits) ? edits : [{ old_string, new_string }]
  if (list.length > MAX_EDITS) return true
  return list.some(e => [e?.old_string, e?.new_string].some(v => typeof v === 'string' && v.length > MAX_EDIT_STRING))
}

/**
 * The file as an Edit or MultiEdit leaves it, from the file as it is now; `undefined`
 * when an edit does not apply (its `old_string` is absent or empty), so the caller
 * falls back to the text written; EDIT_TOO_LARGE when the result would pass MAX_SCAN
 * at any step, or the edits pass their limits. Replacement is by position, never by
 * pattern, and the size is computed before a `replace_all` builds anything.
 */
export const afterEdits = (original: string, input: unknown): string | typeof EDIT_TOO_LARGE | undefined => {
  if (typeof input !== 'object' || input === null) return undefined
  if (oversizedEdits(input) || original.length > MAX_SCAN) return EDIT_TOO_LARGE
  const { old_string, new_string, replace_all, edits } = input as Record<string, unknown>
  const list = Array.isArray(edits) ? edits : [{ old_string, new_string, replace_all }]
  let text = original
  for (const edit of list) {
    const e = edit as { old_string?: unknown; new_string?: unknown; replace_all?: unknown } | null
    if (typeof e?.old_string !== 'string' || typeof e.new_string !== 'string' || e.old_string === '') return undefined
    const at = text.indexOf(e.old_string)
    if (at < 0) return undefined
    if (e.replace_all !== true) {
      text = text.slice(0, at) + e.new_string + text.slice(at + e.old_string.length)
    } else {
      const parts: string[] = []
      let from = 0
      let size = 0
      for (let hit = at; hit >= 0; hit = text.indexOf(e.old_string, from)) {
        parts.push(text.slice(from, hit))
        size += hit - from + e.new_string.length
        // Counted as it goes, so a replacement that multiplies the text stops here, not after it is built.
        if (size > MAX_SCAN) return EDIT_TOO_LARGE
        from = hit + e.old_string.length
      }
      parts.push(text.slice(from))
      text = parts.join(e.new_string)
    }
    if (text.length > MAX_SCAN) return EDIT_TOO_LARGE
  }
  return text
}

/**
 * Every apparent secret in what a tool call writes, de-duplicated. With `current`,
 * the file's text before an Edit or MultiEdit, the file as the edit leaves it is
 * scanned and lines are the file's; else each piece written, with lines relative to it.
 */
export const secretsIn = (input: unknown, current?: string): SecretFinding[] => {
  if (oversizedEdits(input)) return [TOO_LARGE]
  const after = current === undefined ? undefined : afterEdits(current, input)
  if (after === EDIT_TOO_LARGE) return [TOO_LARGE]
  const texts = after === undefined ? writtenText(input) : [after]
  if (texts.reduce((n, t) => n + t.length, 0) > MAX_SCAN) return [TOO_LARGE]
  const seen = new Set<string>()
  return texts.flatMap(findSecrets).filter(f => {
    const key = `${f.line}:${f.what}`
    return !seen.has(key) && seen.add(key)
  })
}

/** Whether every finding was in the file before the edit, so the edit did not add the secret: the fix is the same either way, the message differs. */
export const alreadyPresent = (before: string | undefined, findings: SecretFinding[]): boolean => {
  if (before === undefined || findings.length === 0 || findings.some(f => f.line === 0)) return false
  const had = new Map<string, number>()
  for (const f of findSecrets(before)) had.set(f.what, (had.get(f.what) ?? 0) + 1)
  for (const f of findings) {
    const n = had.get(f.what) ?? 0
    if (n === 0) return false
    had.set(f.what, n - 1)
  }
  return true
}

/** The refusal: where, what kind, the fix. It never repeats the matched text. */
export const denyReason = (file: string, findings: SecretFinding[], already = false): string => {
  if (findings.some(f => f.line === 0)) {
    return `Flowstate refused this edit to ${clean(file, 120)}: it is too large to scan for secrets (over ${MAX_SCAN / 1024} KiB), so it was not made. Split the Flowfile or make a smaller edit.`
  }
  const listed = findings
    .slice(0, MAX_LISTED)
    .map(f => `line ${f.line}: ${f.what}`)
    .join('; ')
  const more = findings.length - MAX_LISTED
  const subject = already
    ? `the Flowfile already holds a literal secret, not added by this edit (${listed}${more > 0 ? `; and ${more} more` : ''}), so any edit to it is refused until it is replaced`
    : `it appears to write a secret into the Flowfile (${listed}${more > 0 ? `; and ${more} more` : ''})`
  return [
    `Flowstate refused this edit to ${clean(file, 120)}: ${subject}.`,
    "A Flowfile is committed and its values reach durable history, so reference the secret instead: use ${secret('scheme:name')}, such as ${secret('env:GITHUB_TOKEN')}, and have the operator supply the value where the run happens.",
    'If this is a harmless look-alike, change its spelling so it no longer matches a credential.',
  ].join(' ')
}

/** Whether a Bash command that could not be checked should still ask: only text that names the binary. */
export const namesFlow = (command: unknown, flowBinary = 'flow'): boolean =>
  typeof command === 'string' && (/\bflow/.test(command) || command.includes(basename(flowBinary)))

export const UNCHECKED_BASH = 'Flowstate could not check this flow command, so it asks first.'
export const UNCHECKED_EDIT = 'Flowstate could not check this Flowfile edit for secrets, so it was not made. Try again.'
