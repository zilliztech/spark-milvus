import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { devNull, tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';

const exec = promisify(execFile);
const SHA = /^[0-9a-f]{40}$/i;
const ZERO_OID = '0'.repeat(40);
const MAX_GIT_BYTES = 64 * 1024 * 1024;
const UTF8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const PAGE_LIMITS = { read: 12000, files: 200, search: 100 };

function sha(value, name) {
  if (typeof value !== 'string' || !SHA.test(value)) {
    throw new Error(`${name} must be a complete 40-character commit SHA`);
  }
  return value.toLowerCase();
}

function gitEnvironment() {
  const env = {
    GIT_CONFIG_NOSYSTEM: '1',
    GIT_CONFIG_SYSTEM: devNull,
    GIT_CONFIG_GLOBAL: devNull,
    GIT_TERMINAL_PROMPT: '0',
    GIT_NO_REPLACE_OBJECTS: '1',
    LC_ALL: 'C',
  };
  for (const key of ['PATH', 'SystemRoot', 'WINDIR', 'TMPDIR', 'TMP', 'TEMP']) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  return env;
}

async function git(directory, args) {
  const { stdout } = await exec('git', [
    '--no-pager', '--literal-pathspecs',
    '-c', `core.hooksPath=${devNull}`,
    '-c', `core.attributesFile=${devNull}`,
    '-c', 'core.fsmonitor=false',
    '-c', 'credential.helper=',
    '-c', 'credential.interactive=false',
    '-c', 'protocol.allow=never',
    '-c', 'protocol.https.allow=always',
    '-c', 'gc.auto=0',
    '-c', 'maintenance.auto=false',
    '-C', directory,
    ...args,
  ], {
    env: gitEnvironment(),
    encoding: 'buffer',
    maxBuffer: MAX_GIT_BYTES,
    timeout: 120000,
    windowsHide: true,
  });
  return stdout;
}

function decode(buffer, description) {
  try {
    return UTF8.decode(buffer);
  } catch (cause) {
    throw new Error(`${description} contains invalid UTF-8 and requires manual review`, { cause });
  }
}

function validPath(value, { prefix = false } = {}) {
  if (typeof value !== 'string' || value.includes('\0') || value.startsWith('/')) {
    throw new Error('Invalid repository path');
  }
  if (prefix && value === '') return value;
  const parts = (prefix && value.endsWith('/') ? value.slice(0, -1) : value).split('/');
  if (parts.some(part => !part || part === '.' || part === '..' || part.toLowerCase() === '.git')) {
    throw new Error(`Invalid repository path: ${JSON.stringify(value)}`);
  }
  return value;
}

function page(offset, limit, maximum) {
  if (!Number.isSafeInteger(offset) || offset < 0) throw new Error('offset must be a non-negative integer');
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > maximum) {
    throw new Error(`limit must be an integer between 1 and ${maximum}`);
  }
}

function nextOffset(offset, count, total) {
  return offset + count < total ? offset + count : null;
}

function metadata(change) {
  return [
    `Path: ${JSON.stringify(change.path)}`,
    ...(change.previousPath ? [`Previous path: ${JSON.stringify(change.previousPath)}`] : []),
    `Status: ${change.status}`,
    `Old: ${change.oldMode} ${change.oldOid}`,
    `New: ${change.newMode} ${change.newOid}`,
    `Manual review: ${change.reason}`,
  ].join('\n');
}

/** Reads immutable Git objects. No PR files are checked out or executed. */
export class Repository {
  #directory;
  #baseSha;
  #headSha;
  #owned = false;
  #closed = false;
  #commits;
  #mergeBase;
  #trees = new Map();
  #blobs = new Map();

  constructor(directory, baseSha, headSha) {
    if (typeof directory !== 'string' || !directory) throw new Error('A Git repository directory is required');
    this.#directory = path.resolve(directory);
    this.#baseSha = sha(baseSha, 'baseSha');
    this.#headSha = sha(headSha, 'headSha');
  }

  static async fetch(repository, baseSha, headSha) {
    if (typeof repository !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.-]*\/[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(repository)) {
      throw new Error('repository must be a GitHub owner/repository name, not a URL');
    }
    const base = sha(baseSha, 'baseSha');
    const head = sha(headSha, 'headSha');
    const directory = await mkdtemp(path.join(tmpdir(), 'spark-milvus-ai-review-'));
    const result = new Repository(directory, base, head);
    result.#owned = true;
    try {
      await git(directory, ['init', '--bare', '--template=', '--initial-branch=ai-review', '.']);
      await git(directory, [
        'fetch', '--no-tags', '--no-recurse-submodules', '--no-write-fetch-head',
        `https://github.com/${repository}.git`, base, head,
      ]);
      await result.#ensureCommits();
      return result;
    } catch (cause) {
      await result.close();
      throw cause;
    }
  }

  #checkOpen() {
    if (this.#closed) throw new Error('Repository is closed');
  }

  async #ensureCommits() {
    this.#checkOpen();
    this.#commits ??= (async () => {
      for (const oid of new Set([this.#baseSha, this.#headSha])) {
        const type = decode(await git(this.#directory, ['cat-file', '-t', oid]), 'Object type').trim();
        if (type !== 'commit') throw new Error(`SHA ${oid} is not a commit`);
      }
    })();
    return this.#commits;
  }

  async #resolveRef(ref) {
    if (!['head', 'base', 'merge-base'].includes(ref)) throw new Error('ref must be head, base, or merge-base');
    await this.#ensureCommits();
    if (ref === 'head') return this.#headSha;
    if (ref === 'base') return this.#baseSha;
    this.#mergeBase ??= (async () => {
      let output;
      try {
        output = await git(this.#directory, ['merge-base', '--all', this.#baseSha, this.#headSha]);
      } catch (cause) {
        if (cause.code === 1) throw new Error('No common ancestor exists; cannot determine the PR merge base', { cause });
        throw cause;
      }
      const bases = decode(output, 'Merge base').trim().split('\n');
      if (bases.length !== 1) throw new Error('Multiple merge bases require manual review');
      return sha(bases[0], 'merge-base');
    })();
    return this.#mergeBase;
  }

  async #tree(ref) {
    const oid = await this.#resolveRef(ref);
    if (!this.#trees.has(oid)) {
      this.#trees.set(oid, (async () => {
        const output = decode(await git(this.#directory, ['ls-tree', '-r', '-z', '--full-tree', oid]), 'Tree paths');
        const entries = new Map();
        for (const record of output.split('\0')) {
          if (!record) continue;
          const match = /^(\d{6}) (blob|commit) ([0-9a-f]{40})\t([\s\S]*)$/.exec(record);
          if (!match) throw new Error('Unexpected Git tree entry');
          const filePath = validPath(match[4]);
          entries.set(filePath, { mode: match[1], type: match[2], oid: match[3] });
        }
        return entries;
      })());
    }
    return this.#trees.get(oid);
  }

  async #blob(oid) {
    if (oid === ZERO_OID) return { content: '' };
    if (!this.#blobs.has(oid)) {
      this.#blobs.set(oid, (async () => {
        const buffer = await git(this.#directory, ['cat-file', 'blob', sha(oid, 'blob OID')]);
        if (buffer.includes(0)) return { reason: 'Binary content contains NUL bytes and requires manual review' };
        try {
          return { content: UTF8.decode(buffer) };
        } catch {
          return { reason: 'Content is not valid UTF-8 and requires manual review' };
        }
      })());
    }
    return this.#blobs.get(oid);
  }

  async changes() {
    const base = await this.#resolveRef('merge-base');
    const output = decode(await git(this.#directory, [
      'diff', '--raw', '-z', '--abbrev=40', '--no-ext-diff', '--no-textconv',
      '--no-color', '--no-relative', '--find-renames', base, this.#headSha, '--',
    ]), 'Changed paths');
    const records = output.split('\0');
    const changes = [];
    for (let index = 0; index < records.length - 1;) {
      const header = /^:(\d{6}) (\d{6}) ([0-9a-f]{40}) ([0-9a-f]{40}) ([A-Z]\d*)$/.exec(records[index++]);
      if (!header) throw new Error('Unexpected Git raw diff record');
      const firstPath = validPath(records[index++]);
      const renamed = /^[RC]/.test(header[5]);
      const filePath = renamed ? validPath(records[index++]) : firstPath;
      const change = {
        path: filePath,
        ...(renamed ? { previousPath: firstPath } : {}),
        status: header[5],
        oldMode: header[1], newMode: header[2],
        oldOid: header[3], newOid: header[4],
        patch: '', kind: 'text',
      };
      if ([change.oldMode, change.newMode].includes('160000')) {
        change.kind = 'submodule';
        change.reason = 'Submodule commit objects are not fetched; requires manual review';
        change.patch = metadata(change);
      } else {
        const oldBlob = await this.#blob(change.oldOid);
        const newBlob = await this.#blob(change.newOid);
        const binaryReason = oldBlob.reason || newBlob.reason;
        if (binaryReason) {
          change.kind = 'binary';
          change.reason = binaryReason;
        } else if ([change.oldMode, change.newMode].includes('120000')) {
          change.kind = 'symlink';
          change.reason = 'Symbolic link targets are shown as text and are never followed';
        } else if ([oldBlob.content, newBlob.content].some(content => /^version https:\/\/git-lfs.github.com\/spec\/v1\r?\n/.test(content))) {
          change.kind = 'lfs';
          change.reason = 'Git LFS pointers are shown; external LFS objects require manual review';
        }
        if (binaryReason) {
          change.reason = binaryReason;
          change.patch = metadata(change);
        } else {
          change.patch = decode(await git(this.#directory, [
            'diff', '--patch', '--text', '--full-index', '--no-ext-diff', '--no-textconv',
            '--no-color', '--no-relative', '--find-renames', '--src-prefix=a/', '--dst-prefix=b/',
            base, this.#headSha, '--',
            ...(change.previousPath ? [change.previousPath] : []), change.path,
          ]), 'File patch');
        }
      }
      changes.push(change);
    }
    return changes;
  }

  async readFile({ path: filePath, ref = 'head', offset = 0, limit = PAGE_LIMITS.read }) {
    validPath(filePath);
    page(offset, limit, PAGE_LIMITS.read);
    const entry = (await this.#tree(ref)).get(filePath);
    if (!entry) throw new Error(`File path not found at ${ref}: ${JSON.stringify(filePath)}`);
    if (entry.type !== 'blob') throw new Error(`Cannot read submodule path ${JSON.stringify(filePath)} as a file`);
    const blob = await this.#blob(entry.oid);
    if (blob.reason) throw new Error(`Cannot read ${JSON.stringify(filePath)}: ${blob.reason}`);
    const selected = [];
    let total = 0;
    for (const character of blob.content) {
      if (total >= offset && selected.length < limit) selected.push(character);
      total++;
    }
    return {
      path: filePath, ref, mode: entry.mode, content: selected.join(''),
      next_offset: nextOffset(offset, selected.length, total), total_chars: total,
    };
  }

  async listFiles({ ref = 'head', prefix = '', offset = 0, limit = PAGE_LIMITS.files } = {}) {
    validPath(prefix, { prefix: true });
    page(offset, limit, PAGE_LIMITS.files);
    const files = [...(await this.#tree(ref)).keys()].filter(filePath => filePath.startsWith(prefix));
    const selected = files.slice(offset, offset + limit);
    return { files: selected, next_offset: nextOffset(offset, selected.length, files.length), total_files: files.length };
  }

  async search({ pattern, ref = 'head', offset = 0, limit = PAGE_LIMITS.search }) {
    if (typeof pattern !== 'string' || !pattern || pattern.includes('\0')) throw new Error('pattern must be a non-empty literal string without NUL');
    page(offset, limit, PAGE_LIMITS.search);
    const tree = await this.#tree(ref);
    const oid = await this.#resolveRef(ref);
    let output;
    try {
      // Force text for candidate selection: attributes can label valid text as binary.
      output = await git(this.#directory, [
        'grep', '--no-textconv', '--no-color', '--full-name', '--text', '-l', '-z', '-F',
        '-e', pattern, oid, '--',
      ]);
    } catch (cause) {
      if (cause.code !== 1) throw cause;
      output = Buffer.alloc(0);
    }
    const candidates = new Set();
    for (const record of decode(output, 'Search paths').split('\0')) {
      if (!record) continue;
      if (!record.startsWith(`${oid}:`)) throw new Error('Unexpected Git grep path');
      const filePath = validPath(record.slice(oid.length + 1));
      if (!tree.has(filePath)) throw new Error('Git grep returned a path outside the selected tree');
      candidates.add(filePath);
    }
    const matches = [];
    let total = 0;
    let resultBytes = 0;
    for (const [filePath, entry] of tree) {
      if (entry.type !== 'blob') continue;
      // Git grep skips symlink blobs; read their link text explicitly, without following it.
      if (!candidates.has(filePath) && entry.mode !== '120000') continue;
      const blob = await this.#blob(entry.oid);
      if (blob.reason) continue;
      let start = 0;
      let line = 1;
      while (start < blob.content.length) {
        const newline = blob.content.indexOf('\n', start);
        const end = newline === -1 ? blob.content.length : newline;
        const text = blob.content.slice(start, end);
        if (text.includes(pattern)) {
          if (total >= offset && matches.length < limit) {
            const match = `${filePath}:${line}:${text}`;
            resultBytes += Buffer.byteLength(match, 'utf8');
            if (resultBytes > MAX_GIT_BYTES) throw new Error('Search page is too large; use a smaller limit');
            matches.push(match);
          }
          total++;
        }
        start = end + 1;
        line++;
      }
    }
    return { matches, next_offset: nextOffset(offset, matches.length, total), total_matches: total };
  }

  async close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#trees.clear();
    this.#blobs.clear();
    if (this.#owned) await rm(this.#directory, { recursive: true, force: true });
  }
}
