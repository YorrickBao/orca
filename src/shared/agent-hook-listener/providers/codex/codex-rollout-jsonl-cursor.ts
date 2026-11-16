import { extname, isAbsolute } from 'node:path'
import {
  peekTranscriptDirectory,
  takeTranscriptHead,
  takeTranscriptTail,
  TRANSCRIPT_LINE_MAX_BYTES,
  type TranscriptTail
} from '../../transcripts/transcript-read-ahead'

export type JsonlCursor = {
  filePath?: string
  offset: number
  carry: string
}

export type JsonRecord = Record<string, unknown>

/** An absolute `.jsonl` rollout path, trimmed, or undefined. */
export function normalizedTranscriptPath(transcriptPath: string | undefined): string | undefined {
  const normalizedPath = transcriptPath?.trim()
  return normalizedPath && isAbsolute(normalizedPath) && extname(normalizedPath) === '.jsonl'
    ? normalizedPath
    : undefined
}

export function record(value: unknown): JsonRecord | undefined {
  return typeof value === 'object' && value !== null ? (value as JsonRecord) : undefined
}

/** 'pending' while the file or its first line is unwritten; 'unreadable' on other I/O failures; 'unfetched' until a read-ahead ran; undefined once the line is final. */
export function readJsonlFirstRecord(
  filePath: string
):
  | { size: number; record: JsonRecord | 'pending' | undefined }
  | 'pending'
  | 'unreadable'
  | 'unfetched' {
  const head = takeTranscriptHead(filePath)
  if (head === undefined) {
    return 'unfetched'
  }
  if (head === 'missing') {
    return 'pending'
  }
  if (head === 'unreadable') {
    return head
  }
  const newline = head.bytes.indexOf(0x0a)
  if (newline === -1) {
    return {
      size: head.size,
      record: head.bytes.length < TRANSCRIPT_LINE_MAX_BYTES ? 'pending' : undefined
    }
  }
  try {
    return { size: head.size, record: record(JSON.parse(head.bytes.toString('utf8', 0, newline))) }
  } catch {
    return { size: head.size, record: undefined }
  }
}

/** Undefined distinguishes unreadable/vanished files from no new lines; 'unfetched' means no read-ahead ran for this position and 'refused' that it could not run; lineFilter rejects raw substrings before JSON.parse. */
export function readJsonlCursor(
  cursor: JsonlCursor,
  lineFilter?: (line: string) => boolean
): JsonRecord[] | undefined | 'unfetched' | 'refused' {
  if (!cursor.filePath) {
    return undefined
  }
  const tail = takeTranscriptTail(cursor.filePath, cursor.offset)
  return tail === undefined || tail === 'refused'
    ? (tail ?? 'unfetched')
    : decodeJsonlTail(cursor, tail, lineFilter)
}

/** Advances `cursor` over fetched bytes with the carry, skip-prefix and truncation rules; a planner passes a copy to look ahead. */
export function decodeJsonlTail(
  cursor: JsonlCursor,
  tail: Exclude<TranscriptTail, 'refused'>,
  lineFilter?: (line: string) => boolean
): JsonRecord[] | undefined {
  if (tail === 'missing') {
    return undefined
  }
  if (tail.size < cursor.offset) {
    cursor.offset = 0
    cursor.carry = ''
  }
  if (tail.size === cursor.offset) {
    return []
  }
  const start = tail.start
  const bytesRead = tail.bytes.length
  const buffer = tail.bytes
  const skippedPrefix = start !== cursor.offset
  const content = `${skippedPrefix ? '' : cursor.carry}${buffer.toString('utf8', 0, bytesRead)}`
  const lines = content.split('\n')
  cursor.offset = start + bytesRead
  cursor.carry = lines.pop() ?? ''
  if (skippedPrefix) {
    lines.shift()
  }
  const records: JsonRecord[] = []
  for (const line of lines) {
    if (
      (lineFilter && !lineFilter(line)) ||
      Buffer.byteLength(line, 'utf8') > TRANSCRIPT_LINE_MAX_BYTES
    ) {
      continue
    }
    try {
      const parsed = record(JSON.parse(line) as unknown)
      if (parsed) {
        records.push(parsed)
      }
    } catch {
      // A malformed rollout line must not block later lifecycle events.
    }
  }
  return records
}

/** Undefined until a read-ahead listed the directory. */
export function readTranscriptDirectory(directory: string): string[] | undefined {
  return peekTranscriptDirectory(directory)
}
