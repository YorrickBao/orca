import { afterEach, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync, symlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { runProcess } from '@orca/process-host'

const directories: string[] = []

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true })
  }
})

describe('transcript read-ahead', () => {
  it.skipIf(process.platform === 'win32')(
    'rejects writer-less FIFOs and symlinks to them without blocking',
    async () => {
      const directory = mkdtempSync(join(tmpdir(), 'codex-rollout-fifo-'))
      directories.push(directory)
      const fifo = join(directory, 'pipe.jsonl')
      const link = join(directory, 'link.jsonl')
      const created = await runProcess({ program: 'mkfifo', args: [fifo], timeoutMs: 5000 })
      expect(created.code).toBe(0)
      symlinkSync(fifo, link)
      const readerPath = resolve(
        'src/shared/agent-hook-listener/transcripts/transcript-read-ahead.ts'
      )
      const result = await runProcess({
        program: process.execPath,
        args: [
          '--import',
          'tsx',
          '--eval',
          `const assert = require('node:assert/strict');
const { TranscriptReads, fetchTranscriptHead, fetchTranscriptTail } = require(${JSON.stringify(readerPath)});
(async () => {
  for (const path of ${JSON.stringify([fifo, link, directory])}) {
    const reads = new TranscriptReads('poll');
    await fetchTranscriptHead(reads, path);
    await fetchTranscriptTail(reads, path, 0);
    assert.equal(reads.heads.get(path), 'unreadable');
    assert.equal(reads.tails.get('0\\0' + path), 'missing');
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});`
        ],
        timeoutMs: 5000
      })
      expect(result.timedOut).toBe(false)
      expect(result.code, result.stderr).toBe(0)
    }
  )
})
