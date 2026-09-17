import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { Repository } from '../repository.mjs';
import { run } from '../main.mjs';

const exec = promisify(execFile);

test('manual dispatch reviews a real documentation diff and publishes a SHA-stamped coverage report', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-review-main-'));
  try {
    const git = async (...args) => (await exec('git', ['-C', root, ...args], { env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null' } })).stdout.trim();
    await git('init');
    await git('config', 'user.name', 'Review Test');
    await git('config', 'user.email', 'review@example.invalid');
    await mkdir(join(root, 'docs'));
    await writeFile(join(root, 'README.md'), 'Repository instructions.\n');
    await writeFile(join(root, 'docs/guide.md'), 'Old instructions.\n');
    await git('add', '.'); await git('commit', '-m', 'base');
    const base = await git('rev-parse', 'HEAD');
    await writeFile(join(root, 'docs/guide.md'), 'New instructions.\n');
    await git('add', '.'); await git('commit', '-m', 'documentation');
    const head = await git('rev-parse', 'HEAD');
    await writeFile(join(root, 'event.json'), JSON.stringify({ inputs: { pr_number: '42' } }));
    const writes = [];
    const fetchImpl = async (url, init) => {
      let data;
      if (url.startsWith('https://model.invalid/')) {
        const body = JSON.parse(init.body);
        const payload = JSON.parse(body.messages[1].content);
        assert.equal(payload.context.targetBranch, 'refactor/v2');
        assert.match(payload.units[0].content, /New instructions/);
        data = { choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ reviewed: payload.units.map(u => u.id), findings: [], limitations: [] }) } }] };
      } else if (init.method !== 'GET') {
        writes.push(JSON.parse(init.body)); data = {};
      } else if (url.includes('/comments?')) data = [];
      else data = { number: 42, state: 'open', title: 'Update documentation', base: { sha: base, ref: 'refactor/v2' }, head: { sha: head } };
      return new Response(JSON.stringify(data), { status: 200 });
    };
    const exitCode = await run({ env: { GITHUB_REPOSITORY: 'example/repo', GITHUB_TOKEN: 'test-only', GITHUB_RUN_ID: '1', GITHUB_EVENT_PATH: join(root, 'event.json'), REVIEW_OUTPUT_DIR: join(root, 'output'), MODEL_API_KEY: 'test-only', MODEL_BASE_URL: 'https://model.invalid' }, fetchImpl, repositoryFactory: async (_name, baseSha, headSha) => new Repository(root, baseSha, headSha) });
    assert.equal(exitCode, 0);
    const report = JSON.parse(await readFile(join(root, 'output/review.json'), 'utf8'));
    assert.equal(report.head, head);
    assert.equal(report.complete, true);
    assert.equal(report.coverage[0].path, 'docs/guide.md');
    assert.equal(report.coverage[0].reviewedBy.length, 2);
    assert.equal(writes.length, 1);
    assert.match(writes[0].body, /LGTM/);
    assert.ok(writes[0].body.includes(head));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('a first-batch model failure preserves pending documentation coverage in artifacts and the PR summary', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-review-failure-'));
  try {
    await writeFile(join(root, 'event.json'), JSON.stringify({ inputs: { pr_number: '42' } }));
    const writes = [];
    const fetchImpl = async (url, init) => {
      let data;
      if (url.startsWith('https://model.invalid/')) {
        data = { choices: [{ finish_reason: 'stop', message: { content: 'The documentation looks correct.' } }] };
      } else if (init.method !== 'GET') {
        writes.push(JSON.parse(init.body)); data = {};
      } else if (url.includes('/comments?')) data = [];
      else data = { number: 42, state: 'open', base: { sha: 'base', ref: 'main' }, head: { sha: 'head' } };
      return new Response(JSON.stringify(data), { status: 200 });
    };
    const repository = {
      changes: async () => [{ path: 'docs/guide.md', kind: 'text', status: 'A', patch: '@@ -0,0 +1 @@\n+Incorrect instructions.\n' }],
      listFiles: async () => ({ files: [] }),
      close: async () => {},
    };
    const exitCode = await run({ env: { GITHUB_REPOSITORY: 'example/repo', GITHUB_TOKEN: 'test-only', GITHUB_RUN_ID: '1', GITHUB_EVENT_PATH: join(root, 'event.json'), REVIEW_OUTPUT_DIR: join(root, 'output'), MODEL_API_KEY: 'test-only', MODEL_BASE_URL: 'https://model.invalid' }, fetchImpl, repositoryFactory: async () => repository });
    assert.equal(exitCode, 1);
    const report = JSON.parse(await readFile(join(root, 'output/review.json'), 'utf8'));
    assert.equal(report.complete, false);
    assert.deepEqual(report.coverage, [{ id: '1', path: 'docs/guide.md', part: 1, reviewedBy: [] }]);
    assert.match(report.limitations.join(' '), /invalid review JSON/);
    assert.equal(writes.length, 1);
    assert.match(writes[0].body, /INCOMPLETE/);
    assert.match(writes[0].body, /Coverage: 0\/1 chunks across 1 changed files/);
    assert.doesNotMatch(writes[0].body, /LGTM/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('a tool loop that exhausts the budget keeps both reviewers\' traces and the failure in the artifact', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-review-loop-'));
  try {
    await writeFile(join(root, 'event.json'), JSON.stringify({ inputs: { pr_number: '42' } }));
    const writes = [];
    const fetchImpl = async (url, init) => {
      let data;
      if (url.startsWith('https://model.invalid/')) {
        data = { choices: [{ finish_reason: 'tool_calls', message: { content: null, tool_calls: [{ id: 'r', function: { name: 'read_file', arguments: '{"path":"CLAUDE.md","offset":12000}' } }] } }], usage: { prompt_tokens: 100, completion_tokens: 10 } };
      } else if (init.method !== 'GET') {
        writes.push(JSON.parse(init.body)); data = {};
      } else if (url.includes('/comments?')) data = [];
      else data = { number: 42, state: 'open', base: { sha: 'base', ref: 'main' }, head: { sha: 'head' } };
      return new Response(JSON.stringify(data), { status: 200 });
    };
    const repository = {
      changes: async () => [{ path: 'docs/guide.md', kind: 'text', status: 'A', patch: '@@ -0,0 +1 @@\n+Incorrect instructions.\n' }],
      listFiles: async () => ({ files: [] }),
      readFile: async () => ({ content: 'guidance', next_offset: null }),
      close: async () => {},
    };
    const exitCode = await run({ env: { GITHUB_REPOSITORY: 'example/repo', GITHUB_TOKEN: 'test-only', GITHUB_RUN_ID: '1', GITHUB_EVENT_PATH: join(root, 'event.json'), REVIEW_OUTPUT_DIR: join(root, 'output'), MODEL_API_KEY: 'test-only', MODEL_BASE_URL: 'https://model.invalid' }, fetchImpl, repositoryFactory: async () => repository });
    assert.equal(exitCode, 1);
    const report = JSON.parse(await readFile(join(root, 'output/review.json'), 'utf8'));
    assert.equal(report.complete, false);
    assert.match(report.limitations.join(' '), /tool budget exhausted/);
    assert.deepEqual(report.coverage, [{ id: '1', path: 'docs/guide.md', part: 1, reviewedBy: [] }]);
    const config = JSON.parse(await readFile(new URL('../config.json', import.meta.url), 'utf8'));
    for (const reviewer of config.reviewers) assert.equal(report.trace.filter(t => t.reviewer === reviewer.id).length, config.maxToolRounds + 1);
    assert.ok(report.trace.every(t => t.stage === 'review' && t.toolCalls[0].name === 'read_file' && t.toolCalls[0].arguments.offset === 12000));
    assert.ok(!JSON.stringify(report.trace).includes('guidance'));
    assert.equal(writes.length, 1);
    assert.match(writes[0].body, /INCOMPLETE/);
    assert.match(writes[0].body, /tool budget exhausted/);
    assert.doesNotMatch(writes[0].body, /toolCalls/);
  } finally { await rm(root, { recursive: true, force: true }); }
});
