import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { chmod, mkdir, mkdtemp, readFile, rename, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { after, before, test } from 'node:test';
import { Repository } from '../repository.mjs';

const exec = promisify(execFile);
const specialPath = ':(glob)* [question]\t"quoted"\n.md';
const longLine = `${'x'.repeat(25000)}TAIL\n`;
let fixture;

async function git(directory, ...args) {
  const { stdout } = await exec('git', ['-C', directory, ...args], {
    encoding: 'utf8',
    env: { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null' },
  });
  return stdout.trim();
}

async function makeRepository() {
  const root = await mkdtemp(path.join(tmpdir(), 'ai-review-repository-test-'));
  const directory = path.join(root, 'repo');
  await mkdir(path.join(directory, 'docs'), { recursive: true });
  await git(directory, 'init', '--initial-branch=main');
  await git(directory, 'config', 'user.name', 'Repository Test');
  await git(directory, 'config', 'user.email', 'repository-test@example.invalid');
  await writeFile(path.join(directory, 'docs/guide.md'), 'Original guide.\nneedle.+ literal\n');
  await writeFile(path.join(directory, 'docs/rename-me.md'), 'Rename me without edits.\n');
  await writeFile(path.join(directory, 'obsolete.md'), 'Remove me.\n');
  await writeFile(path.join(directory, 'script.sh'), '#!/bin/sh\ntrue\n');
  await writeFile(path.join(directory, 'disabled.md'), 'Before attributes.\n');
  await writeFile(path.join(directory, '.gitattributes'), '*.md diff=review-test\ndisabled.md -diff\n');
  await git(directory, 'add', '--all');
  await git(directory, 'commit', '-m', 'Create base');
  const baseSha = await git(directory, 'rev-parse', 'HEAD');
  await writeFile(path.join(directory, 'docs/guide.md'), 'Updated guide.\nneedle.+ literal\nneedle.+ second\n');
  await rename(path.join(directory, 'docs/rename-me.md'), path.join(directory, 'docs/renamed.md'));
  await rm(path.join(directory, 'obsolete.md'));
  await chmod(path.join(directory, 'script.sh'), 0o755);
  await writeFile(path.join(directory, 'disabled.md'), 'After attributes.\n');
  await writeFile(path.join(root, 'outside.txt'), 'OUTSIDE_CONTENT_MUST_NOT_BE_READ');
  await symlink('../outside.txt', path.join(directory, 'external-link'));
  await writeFile(path.join(directory, specialPath), 'Special path, literal needle.+\n');
  await writeFile(path.join(directory, '--leading-option.md'), 'Leading option path.\n');
  await writeFile(path.join(directory, 'binary.dat'), Buffer.from([0, 1, 2, 3]));
  await writeFile(path.join(directory, 'invalid-utf8.txt'), Buffer.from([0xff, 0xfe, 0x41]));
  await writeFile(path.join(directory, 'large.dat'), 'version https://git-lfs.github.com/spec/v1\noid sha256:' + 'a'.repeat(64) + '\nsize 999999999\n');
  await writeFile(path.join(directory, 'long.txt'), longLine);
  await writeFile(path.join(directory, 'unicode.txt'), 'A😀中Z');
  await git(directory, 'add', '--all');
  await git(directory, 'update-index', '--add', '--cacheinfo', `160000,${baseSha},vendor/module`);
  await git(directory, 'commit', '-m', 'Change all file categories');
  const headSha = await git(directory, 'rev-parse', 'HEAD');
  await git(directory, 'config', 'diff.external', 'command-that-must-not-execute');
  await git(directory, 'config', 'diff.review-test.textconv', 'command-that-must-not-execute');
  return { root, directory, baseSha, headSha };
}

before(async () => { fixture = await makeRepository(); });
after(async () => { if (fixture) await rm(fixture.root, { recursive: true, force: true }); });

function repository() {
  return new Repository(fixture.directory, fixture.baseSha, fixture.headSha);
}

test('reads committed blobs instead of current working files', async () => {
  await writeFile(path.join(fixture.directory, 'docs/guide.md'), 'Uncommitted content');
  const repo = repository();
  const head = await repo.readFile({ path: 'docs/guide.md' });
  const base = await repo.readFile({ path: 'docs/guide.md', ref: 'base' });
  assert.equal(head.content, 'Updated guide.\nneedle.+ literal\nneedle.+ second\n');
  assert.equal(base.content, 'Original guide.\nneedle.+ literal\n');
  assert.equal(head.mode, '100644');
  assert.equal(head.next_offset, null);
});

test('includes documentation, renames, deletions, mode-only edits and long line tails', async () => {
  const changes = await repository().changes();
  const byPath = new Map(changes.map(change => [change.path, change]));
  assert.match(byPath.get('docs/guide.md').patch, /\+Updated guide\./);
  assert.match(byPath.get('docs/renamed.md').status, /^R/);
  assert.equal(byPath.get('docs/renamed.md').previousPath, 'docs/rename-me.md');
  assert.equal(byPath.get('obsolete.md').status, 'D');
  assert.equal(byPath.get('obsolete.md').newOid, '0'.repeat(40));
  assert.equal(byPath.get('script.sh').oldMode, '100644');
  assert.equal(byPath.get('script.sh').newMode, '100755');
  assert.match(byPath.get('script.sh').patch, /new mode 100755/);
  assert.match(byPath.get('long.txt').patch, /x{25000}TAIL/);
  assert.equal(byPath.get('disabled.md').kind, 'text');
  assert.match(byPath.get('disabled.md').patch, /\+After attributes\./);
  assert.equal(byPath.get(specialPath).kind, 'text');
  assert.match(byPath.get(specialPath).patch, /\+Special path, literal needle\.\+/);
});

test('classifies binary, invalid UTF-8, links, LFS and gitlinks for explicit handling', async () => {
  const byPath = new Map((await repository().changes()).map(change => [change.path, change]));
  assert.equal(byPath.get('binary.dat').kind, 'binary');
  assert.equal(byPath.get('invalid-utf8.txt').kind, 'binary');
  assert.match(byPath.get('invalid-utf8.txt').reason, /UTF-8/);
  assert.equal(byPath.get('external-link').kind, 'symlink');
  assert.equal(byPath.get('large.dat').kind, 'lfs');
  assert.equal(byPath.get('vendor/module').kind, 'submodule');
  assert.equal(byPath.get('vendor/module').newOid, fixture.baseSha);
  for (const name of ['binary.dat', 'invalid-utf8.txt', 'large.dat', 'vendor/module']) {
    assert.ok(byPath.get(name).reason);
  }
});

test('keeps binary-to-symlink changes classified as incomplete binary content', async t => {
  const isolated = await makeRepository();
  t.after(() => rm(isolated.root, { recursive: true, force: true }));
  await rm(path.join(isolated.directory, 'binary.dat'));
  await symlink('../outside.txt', path.join(isolated.directory, 'binary.dat'));
  await git(isolated.directory, 'add', '--all');
  await git(isolated.directory, 'commit', '-m', 'Replace binary file with a symbolic link');
  const head = await git(isolated.directory, 'rev-parse', 'HEAD');
  const repo = new Repository(isolated.directory, isolated.headSha, head);
  const [change] = await repo.changes();
  assert.equal(change.path, 'binary.dat');
  assert.equal(change.kind, 'binary');
  assert.equal(change.oldMode, '100644');
  assert.equal(change.newMode, '120000');
  assert.match(change.reason, /Binary/);
  assert.match(change.patch, /Manual review/);
});

test('returns symlink text without opening its target and rejects runner paths', async () => {
  const repo = repository();
  assert.equal((await repo.readFile({ path: 'external-link' })).content, '../outside.txt');
  for (const name of ['.git/config', '../outside.txt', '/etc/passwd', 'docs/../guide.md', 'missing.txt']) {
    await assert.rejects(repo.readFile({ path: name }), /path|found|tracked/i);
  }
  await assert.rejects(repo.readFile({ path: 'docs/guide.md', ref: 'HEAD~1' }), /ref/i);
  await assert.rejects(repo.readFile({ path: 'binary.dat' }), /binary/i);
  await assert.rejects(repo.readFile({ path: 'invalid-utf8.txt' }), /UTF-8/i);
  await assert.rejects(repo.readFile({ path: 'vendor/module' }), /submodule/i);
});

test('resolves pathspec characters literally and paginates Unicode without splitting characters', async () => {
  const repo = repository();
  assert.equal((await repo.readFile({ path: specialPath })).content, 'Special path, literal needle.+\n');
  assert.equal((await repo.readFile({ path: '--leading-option.md' })).content, 'Leading option path.\n');
  assert.deepEqual(await repo.readFile({ path: 'unicode.txt', offset: 1, limit: 2 }), {
    path: 'unicode.txt', ref: 'head', mode: '100644', content: '😀中', next_offset: 3, total_chars: 4,
  });
  assert.equal((await repo.readFile({ path: 'unicode.txt', offset: 3, limit: 2 })).content, 'Z');
  let offset = 0;
  let content = '';
  do {
    const page = await repo.readFile({ path: 'long.txt', offset, limit: 7000 });
    content += page.content;
    offset = page.next_offset;
  } while (offset !== null);
  assert.equal(content, longLine);
  await assert.rejects(repo.readFile({ path: 'unicode.txt', offset: -1 }), /offset/i);
  await assert.rejects(repo.readFile({ path: 'unicode.txt', limit: 0 }), /limit/i);
});

test('lists all Git tree entries through stable pages with prefix filtering', async () => {
  const repo = repository();
  let offset = 0;
  const files = [];
  let total;
  do {
    const page = await repo.listFiles({ offset, limit: 3 });
    assert.ok(page.files.length <= 3);
    files.push(...page.files);
    total = page.total_files;
    offset = page.next_offset;
  } while (offset !== null);
  assert.equal(files.length, total);
  assert.equal(new Set(files).size, total);
  assert.ok(files.includes(specialPath));
  assert.ok(files.includes('vendor/module'));
  assert.ok(!files.includes('obsolete.md'));
  assert.deepEqual((await repo.listFiles({ prefix: 'docs/' })).files, ['docs/guide.md', 'docs/renamed.md']);
  assert.ok((await repo.listFiles({ ref: 'base' })).files.includes('obsolete.md'));
});

test('searches literal text from blobs and paginates matches', async () => {
  const repo = repository();
  const first = await repo.search({ pattern: 'needle.+', limit: 2 });
  assert.equal(first.total_matches, 3);
  assert.equal(first.matches.length, 2);
  assert.equal(first.next_offset, 2);
  const second = await repo.search({ pattern: 'needle.+', offset: first.next_offset, limit: 2 });
  assert.equal(second.matches.length, 1);
  assert.equal(second.next_offset, null);
  assert.ok([...first.matches, ...second.matches].includes('docs/guide.md:3:needle.+ second'));
  assert.equal((await repo.search({ pattern: 'OUTSIDE_CONTENT_MUST_NOT_BE_READ' })).total_matches, 0);
  assert.equal((await repo.search({ pattern: 'needle.+', ref: 'base' })).total_matches, 1);
});

test('search candidates retain attributes-marked text, symlink text and special-path pagination', async () => {
  const repo = repository();
  assert.deepEqual((await repo.search({ pattern: 'After attributes.' })).matches, ['disabled.md:1:After attributes.']);
  assert.deepEqual((await repo.search({ pattern: '../outside.txt' })).matches, ['external-link:1:../outside.txt']);
  assert.equal((await repo.search({ pattern: 'OUTSIDE_CONTENT_MUST_NOT_BE_READ' })).total_matches, 0);
  const matches = [];
  let offset = 0;
  do {
    const page = await repo.search({ pattern: 'needle.+', offset, limit: 1 });
    assert.equal(page.total_matches, 3);
    matches.push(...page.matches);
    offset = page.next_offset;
  } while (offset !== null);
  assert.deepEqual(new Set(matches), new Set([
    `${specialPath}:1:Special path, literal needle.+`,
    'docs/guide.md:2:needle.+ literal',
    'docs/guide.md:3:needle.+ second',
  ]));
  assert.ok(!(await repo.search({ pattern: 'A' })).matches.some(match => match.startsWith('invalid-utf8.txt:')));
  assert.equal((await repo.search({ pattern: 'needle.+\n' })).total_matches, 0);
});

test('uses the merge base when the target branch advanced independently', async t => {
  const isolated = await makeRepository();
  t.after(() => rm(isolated.root, { recursive: true, force: true }));
  await git(isolated.directory, 'checkout', '-b', 'advanced-base', isolated.baseSha);
  await writeFile(path.join(isolated.directory, 'base-only.txt'), 'Not part of the PR.\n');
  await git(isolated.directory, 'add', '--all');
  await git(isolated.directory, 'commit', '-m', 'Advance target branch');
  const advancedBase = await git(isolated.directory, 'rev-parse', 'HEAD');
  const repo = new Repository(isolated.directory, advancedBase, isolated.headSha);
  assert.ok(!(await repo.changes()).some(change => change.path === 'base-only.txt'));
  assert.equal((await repo.readFile({ path: 'docs/guide.md', ref: 'merge-base' })).content, 'Original guide.\nneedle.+ literal\n');
});

test('reports missing common ancestry after an unrelated force push', async t => {
  const isolated = await makeRepository();
  t.after(() => rm(isolated.root, { recursive: true, force: true }));
  await git(isolated.directory, 'checkout', '--orphan', 'unrelated');
  await git(isolated.directory, 'commit', '-m', 'Unrelated history');
  const unrelated = await git(isolated.directory, 'rev-parse', 'HEAD');
  const repo = new Repository(isolated.directory, isolated.baseSha, unrelated);
  await assert.rejects(repo.changes(), /common ancestor|merge base/i);
});

test('rejects arbitrary URLs and revision syntax before fetching', async () => {
  for (const repo of ['https://example.com/repo', 'owner/repo/extra', '../repo', '-x/repo']) {
    await assert.rejects(Repository.fetch(repo, fixture.baseSha, fixture.headSha), /repository/i);
  }
  assert.throws(() => new Repository(fixture.directory, 'HEAD', fixture.headSha), /SHA/i);
  await assert.rejects(Repository.fetch('owner/repo', fixture.baseSha, '--upload-pack=evil'), /SHA/i);
});

test('does not delete repositories supplied by the caller on close', async () => {
  const repo = repository();
  await repo.close();
  await repo.close();
  assert.ok((await stat(fixture.directory)).isDirectory());
  assert.equal(await readFile(path.join(fixture.directory, 'unicode.txt'), 'utf8'), 'A😀中Z');
});
