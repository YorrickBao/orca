import { constants } from 'node:fs'
import { open, readdir, stat, type FileHandle } from 'node:fs/promises'

const TRANSCRIPT_READ_MAX_BYTES = 1024 * 1024
export const TRANSCRIPT_LINE_MAX_BYTES = 256 * 1024
const TRANSCRIPT_DIRECTORY_MAX_ENTRIES = 4096
// Why: transcript work must leave libuv threads for the rest of the process even when the disk stalls.
const FILE_JOB_CONCURRENCY = 2
const ROUND_CONCURRENCY = 2
const STALLED_READ_MS = 5_000

/** Bytes appended past `offset`, or the last 1 MB of them when more arrived; 'refused' when stalled reads on other paths left no slot to try. */
export type TranscriptTail = { start: number; size: number; bytes: Buffer } | 'missing' | 'refused'
/** Up to the first 256 KB; `size` is the file length when read. */
export type TranscriptHead = { size: number; bytes: Buffer } | 'missing' | 'unreadable'
/** Hook reads gate a waiting agent, so they start ahead of background polls. */
export type TranscriptReadPriority = 'hook' | 'poll'

/** One turn's fetched evidence; it lives until the turn applies it, so no load can evict it first. */
export class TranscriptReads {
  readonly tails = new Map<string, TranscriptTail>()
  readonly heads = new Map<string, TranscriptHead>()
  readonly directories = new Map<string, string[]>()
  readonly locatedPaths = new Map<string, string>()
  constructor(readonly priority: TranscriptReadPriority) {}
}

type Waiter = { priority: TranscriptReadPriority; start: () => void; fail: () => void }
type Gate = { active: number; stalled: number; readonly limit: number; readonly waiting: Waiter[] }

const STALLED = Symbol('stalled')
const REFUSED = Symbol('refused')
type Stalled = typeof STALLED | typeof REFUSED

const fileJobs: Gate = { active: 0, stalled: 0, limit: FILE_JOB_CONCURRENCY, waiting: [] }
const rounds: Gate = { active: 0, stalled: 0, limit: ROUND_CONCURRENCY, waiting: [] }
const inFlight = new Map<
  string,
  { running?: Promise<void>; result?: Promise<unknown>; queued?: Promise<unknown> }
>()
let applying: TranscriptReads | undefined

function drain(gate: Gate): void {
  while (gate.active < gate.limit && gate.waiting.length > 0) {
    gate.waiting.shift()?.start()
  }
}

function enqueue(gate: Gate, waiter: Waiter): void {
  const firstPoll = gate.waiting.findIndex((entry) => entry.priority === 'poll')
  if (waiter.priority === 'hook' && firstPoll !== -1) {
    gate.waiting.splice(firstPoll, 0, waiter)
  } else {
    gate.waiting.push(waiter)
  }
  drain(gate)
}

/** Runs a whole pane round; a round always settles because each of its file jobs answers within the stall deadline. */
function admitted(priority: TranscriptReadPriority, round: () => Promise<void>): Promise<void> {
  return new Promise((resolve) => {
    const start = (): void => {
      rounds.active += 1
      void round()
        .catch(() => undefined)
        .finally(() => {
          rounds.active -= 1
          drain(rounds)
          resolve()
        })
    }
    enqueue(rounds, { priority, start, fail: resolve })
  })
}

/**
 * Runs one file job; `result` is STALLED once the job has run past the stall deadline, or at once when every
 * slot is held by a stalled job. A stalled job keeps its slot until the OS returns, so a hung share pins at most
 * FILE_JOB_CONCURRENCY threads; `done` settles only then, so the same key is not re-issued meanwhile.
 */
function fileJob<T>(
  priority: TranscriptReadPriority,
  task: () => Promise<T>
): { result: Promise<T | Stalled>; done: Promise<void> } {
  let answer: (value: T | Stalled) => void = () => {}
  let finish: () => void = () => {}
  const result = new Promise<T | Stalled>((resolve) => {
    answer = resolve
  })
  const done = new Promise<void>((resolve) => {
    finish = resolve
  })
  const fail = (): void => {
    answer(REFUSED)
    finish()
  }
  if (fileJobs.stalled >= fileJobs.limit) {
    fail()
    return { result, done }
  }
  const start = (): void => {
    fileJobs.active += 1
    let stalled = false
    const timer = setTimeout(() => {
      stalled = true
      fileJobs.stalled += 1
      answer(STALLED)
      if (fileJobs.stalled >= fileJobs.limit) {
        for (const waiter of fileJobs.waiting.splice(0)) {
          waiter.fail()
        }
      }
    }, STALLED_READ_MS)
    timer.unref?.()
    void task()
      .then(answer, () => answer(STALLED))
      .finally(() => {
        clearTimeout(timer)
        fileJobs.active -= 1
        if (stalled) {
          fileJobs.stalled -= 1
        }
        finish()
        drain(fileJobs)
      })
  }
  enqueue(fileJobs, { priority, start, fail })
  return { result, done }
}

/** One read per key at a time; a request during a read gets one follow-up read, so it sees bytes written before it asked. */
function coalesced<T>(
  key: string,
  priority: TranscriptReadPriority,
  read: () => Promise<T>
): Promise<T | Stalled> {
  let job = inFlight.get(key)
  if (!job) {
    job = {}
    inFlight.set(key, job)
  }
  if (job.queued) {
    return job.queued as Promise<T | Stalled>
  }
  const current = job
  const run = (): Promise<T | Stalled> => {
    current.queued = undefined
    const { result, done } = fileJob(priority, read)
    current.running = done
    current.result = result
    void done.then(() => {
      if (current.running === done) {
        current.running = undefined
        current.result = undefined
        if (!current.queued && inFlight.get(key) === current) {
          inFlight.delete(key)
        }
      }
    })
    return result
  }
  if (!current.running) {
    return run()
  }
  const running = current.running
  const queued = current.result!.then((value) => (value === STALLED ? STALLED : running.then(run)))
  current.queued = queued
  void queued.then(() => {
    if (current.queued === queued) {
      current.queued = undefined
      if (!current.running && inFlight.get(key) === current) {
        inFlight.delete(key)
      }
    }
  })
  return queued as Promise<T | Stalled>
}

/** Admits a pane's whole read round, so retained evidence is bounded by rounds rather than by every pane's partial reads. */
export function admitTranscriptRound(
  reads: TranscriptReads,
  round: () => Promise<void>
): Promise<void> {
  return admitted(reads.priority, round)
}

/** Runs synchronous status code that consumes `reads`; outside such a scope every read is unfetched. */
export function applyTranscriptReads<T>(reads: TranscriptReads | undefined, apply: () => T): T {
  const previous = applying
  applying = reads
  try {
    return apply()
  } finally {
    applying = previous
  }
}

async function openRegularFile(
  filePath: string
): Promise<{ file: FileHandle; size: number } | undefined> {
  // Nonblocking open keeps a FIFO from holding a threadpool thread before the descriptor check.
  const file = await open(
    filePath,
    constants.O_RDONLY | (process.platform === 'win32' ? 0 : constants.O_NONBLOCK)
  )
  try {
    const stats = await file.stat()
    if (stats.isFile()) {
      return { file, size: stats.size }
    }
  } catch {
    // Fall through to close.
  }
  await file.close()
  return undefined
}

function tailKey(filePath: string, offset: number): string {
  return `${offset}\0${filePath}`
}

async function readTail(filePath: string, offset: number): Promise<TranscriptTail> {
  let opened: { file: FileHandle; size: number } | undefined
  try {
    opened = await openRegularFile(filePath)
    if (!opened) {
      return 'missing'
    }
    const { file, size } = opened
    // A shrunken file restarts at 0, which the cursor detects from `size`.
    const from = size < offset ? 0 : offset
    const length = Math.min(size - from, TRANSCRIPT_READ_MAX_BYTES)
    const start = size - from > length ? size - length : from
    const bytes = Buffer.allocUnsafe(length)
    const { bytesRead } = length > 0 ? await file.read(bytes, 0, length, start) : { bytesRead: 0 }
    return { start, size, bytes: bytes.subarray(0, bytesRead) }
  } catch {
    return 'missing'
  } finally {
    // Why: an open handle would block Codex renaming or deleting the rollout on Windows.
    await opened?.file.close().catch(() => undefined)
  }
}

async function readHead(filePath: string): Promise<TranscriptHead> {
  let opened: { file: FileHandle; size: number } | undefined
  try {
    opened = await openRegularFile(filePath)
    if (!opened) {
      return 'unreadable'
    }
    const bytes = Buffer.allocUnsafe(Math.min(opened.size, TRANSCRIPT_LINE_MAX_BYTES))
    const { bytesRead } = await opened.file.read(bytes, 0, bytes.length, 0)
    return { size: opened.size, bytes: bytes.subarray(0, bytesRead) }
  } catch (error) {
    return error instanceof Error && 'code' in error && error.code === 'ENOENT'
      ? 'missing'
      : 'unreadable'
  } finally {
    await opened?.file.close().catch(() => undefined)
  }
}

async function readDirectory(directory: string): Promise<string[]> {
  try {
    const entries = await readdir(directory)
    return entries.length > TRANSCRIPT_DIRECTORY_MAX_ENTRIES
      ? entries.slice(-TRANSCRIPT_DIRECTORY_MAX_ENTRIES)
      : entries
  } catch {
    return []
  }
}

/** Reads a JSONL cursor's new bytes off the event loop into `reads`. */
export async function fetchTranscriptTail(
  reads: TranscriptReads,
  filePath: string,
  offset: number
): Promise<void> {
  const key = tailKey(filePath, offset)
  const value = await coalesced(`tail\0${key}`, reads.priority, () => readTail(filePath, offset))
  // Why: a read that hung past the deadline is unreadable, as a failed read is, never stale "no news".
  reads.tails.set(key, value === STALLED ? 'missing' : value === REFUSED ? 'refused' : value)
}

export async function fetchTranscriptHead(reads: TranscriptReads, filePath: string): Promise<void> {
  const value = await coalesced(`head\0${filePath}`, reads.priority, () => readHead(filePath))
  if (value !== REFUSED) {
    reads.heads.set(filePath, value === STALLED ? 'unreadable' : value)
  }
}

export async function fetchTranscriptDirectory(
  reads: TranscriptReads,
  directory: string
): Promise<void> {
  const entries = await coalesced(`dir\0${directory}`, reads.priority, () =>
    readDirectory(directory)
  )
  if (entries !== REFUSED) {
    reads.directories.set(directory, entries === STALLED ? [] : entries)
  }
}

export async function transcriptFileExists(
  reads: TranscriptReads,
  filePath: string
): Promise<boolean> {
  const exists = await coalesced(`stat\0${filePath}`, reads.priority, async () => {
    try {
      return (await stat(filePath)).isFile()
    } catch {
      return false
    }
  })
  return exists === true
}

/** The applying turn's bytes for this exact cursor position, consumed once; undefined when none were fetched. */
export function takeTranscriptTail(filePath: string, offset: number): TranscriptTail | undefined {
  const key = tailKey(filePath, offset)
  const value = applying?.tails.get(key)
  applying?.tails.delete(key)
  return value
}

/** Fetched bytes without consuming them, for a planner looking ahead within its own round. */
export function peekTranscriptTail(
  reads: TranscriptReads,
  filePath: string,
  offset: number
): TranscriptTail | undefined {
  return reads.tails.get(tailKey(filePath, offset))
}

export function takeTranscriptHead(filePath: string): TranscriptHead | undefined {
  const value = applying?.heads.get(filePath)
  applying?.heads.delete(filePath)
  return value
}

/** Directory listings are shared by every child resolved in one round, so reading does not consume them. */
export function peekTranscriptDirectory(
  directory: string,
  reads: TranscriptReads | undefined = applying
): string[] | undefined {
  return reads?.directories.get(directory)
}

export function peekLocatedTranscriptPath(key: string): string | undefined {
  return applying?.locatedPaths.get(key)
}
