/**
 * Live refresh for the open run. The pane re-reads `flow list` and `flow timeline`
 * on a bounded schedule while the run is not over, and a one-second local tick
 * redraws elapsed time between reads. Pure and injectable, so the schedule, the
 * stop conditions and the no-overlap rule are tested without a clock.
 */

/** The local redraw period: elapsed time moves here, no process is started. */
export const TICK_MS = 1000
/** A read is due this often at first, backing off to the slower period while nothing changes. */
export const READ_FAST_MS = 2000
export const READ_SLOW_MS = 10_000
/** Hard stop: this many reads in a row with no change in the run. About ten minutes at the slow period. */
export const MAX_IDLE_READS = 60
/** The pane draws on every tick it is open; this long without a draw means it is closed. */
export const DRAW_GRACE_MS = 5000

/** The wait before the next read after `idle` reads that found nothing new: 2s three times, then doubling to 10s. */
export const readDelay = (idle: number): number => {
  const n = Number.isFinite(idle) ? Math.max(0, Math.trunc(idle)) : 0
  return Math.min(READ_SLOW_MS, READ_FAST_MS * 2 ** Math.min(10, Math.floor(n / 3)))
}

/** A run is polled only while it is running or waiting; a final, unknown or missing status is not. */
export const isLive = (kind: string): boolean => kind === 'running' || kind === 'waiting'

export interface PollDeps {
  /** Calls `fn` once after `ms`; returns the cancel. */
  after: (ms: number, fn: () => void) => () => void
  /** Redraws the pane; `read` asks the draw to re-read the CLI, otherwise it may reuse its last read. Resolves when done. */
  redraw: (read: boolean) => Promise<void>
}

/**
 * One poller per pane. `start` is idempotent; `stop` ends it, and a stale timer
 * (an older generation) does nothing when it fires. Ticks never overlap: the next
 * is scheduled only after the redraw it asked for settled, and a tick that finds
 * a draw still in flight skips its redraw.
 */
export class Poller {
  private gen = 0
  private on = false
  private cancel: (() => void) | undefined
  private idle = 0
  private sinceRead = 0
  private print = ''
  private drawnAt = 0
  private drawing = false
  /** Reads asked for since `start`. */
  reads = 0
  ticks = 0

  private deps!: PollDeps

  constructor(private readonly clock: () => number = Date.now) {}

  get active(): boolean {
    return this.on
  }

  /** Called by the draw when it begins and ends. */
  drawBegan(): void {
    this.drawing = true
  }
  drawEnded(): void {
    this.drawing = false
    this.drawnAt = this.clock()
  }

  /** The draw reports the run's fingerprint; a change restarts the backoff. A run that is over stops the poll. */
  seen(print: string, live: boolean): void {
    if (print !== this.print) {
      this.print = print
      this.idle = 0
    }
    if (!live) this.stop()
  }

  start(deps: PollDeps): void {
    if (this.on) return
    this.deps = deps
    this.on = true
    this.gen++
    this.idle = 0
    this.sinceRead = 0
    this.reads = 0
    this.ticks = 0
    this.drawnAt = this.clock()
    this.schedule(this.gen)
  }

  stop(): void {
    this.on = false
    this.gen++
    this.cancel?.()
    this.cancel = undefined
  }

  private schedule(gen: number): void {
    try {
      this.cancel = this.deps.after(TICK_MS, () => {
        void this.tick(gen)
      })
    } catch {
      // No timer to be had (the host refused it): there is no live refresh, and the pane is otherwise unaffected.
      this.stop()
    }
  }

  private async tick(gen: number): Promise<void> {
    if (gen !== this.gen || !this.on) return
    this.ticks++
    // The pane draws each tick it is open; no draw for a while means it was closed, so stop rather than leak a timer.
    // A draw still working is an open pane, however long its reads take.
    if (!this.drawing && this.clock() - this.drawnAt > DRAW_GRACE_MS) return this.stop()
    this.sinceRead += TICK_MS
    let read = false
    if (this.sinceRead >= readDelay(this.idle)) {
      this.sinceRead = 0
      this.idle++
      if (this.idle > MAX_IDLE_READS) return this.stop()
      read = true
      this.reads++
    }
    if (!this.drawing) {
      try {
        await this.deps.redraw(read)
      } catch {
        return this.stop()
      }
    }
    if (gen === this.gen && this.on) this.schedule(gen)
  }
}
