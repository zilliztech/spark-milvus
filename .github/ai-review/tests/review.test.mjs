import test from 'node:test';
import assert from 'node:assert/strict';
import { diffLines, planReview, runReview } from '../review.mjs';
import { Model } from '../model.mjs';

const change = (path, patch, kind = 'text') => ({ path, patch, kind, status: 'M' });
const config = { batchChars: 2000, reviewers: [{ id: 'architecture', focus: 'contracts' }, { id: 'storage', focus: 'resources' }] };
const finding = { path: 'docs/read.md', line: 2, side: 'RIGHT', priority: 'P1', title: 'Incorrect delete instructions', scenario: 'Following the documented command returns deleted rows.', evidence: [{ path: 'reader.scala', ref: 'head', line: 12, detail: 'The documented option disables deletion filtering.' }] };
const patch = '@@ -1,2 +1,2 @@\n title\n-old\n+new\n';

test('all document, generated and lock changes survive chunking including a long line', () => {
  const changes = [change('docs/a.md', 'x'.repeat(6001)), change('dist/a.js', 'generated'), change('package-lock.json', 'lock')];
  const plan = planReview(changes, 2000);
  for (const file of changes) assert.equal(plan.units.filter(u => u.path === file.path).map(u => u.content).join(''), file.patch);
  assert.equal(plan.units.filter(u => u.path === 'docs/a.md').length, 4);
  assert.equal(plan.limitations.length, 0);
});

test('binary changes remain in the plan and prevent an unqualified pass', async () => {
  const changes = [change('fixture.parquet', 'Binary files differ', 'binary')];
  const result = await runReview({ changes, config, rules: '', context: {}, repository: {}, model: { complete: async ({ payload }) => ({ reviewed: payload.units.map(u => u.id), findings: [], limitations: [] }) } });
  assert.equal(result.coverage.length, 1);
  assert.equal(result.complete, false);
  assert.match(result.limitations.join(' '), /fixture.parquet/);
});

test('a finding withdrawn during cross-check is not published', async () => {
  const model = { complete: async ({ payload }) => payload.stage === 'review'
    ? { reviewed: payload.units.map(u => u.id), findings: [finding], limitations: [] }
    : { accepted: [], rejected: payload.candidates.map(c => ({ id: c.id, reason: 'The caller prevents this state.' })), limitations: [] } };
  const result = await runReview({ changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: {}, model });
  assert.deepEqual(result.findings, []);
  assert.deepEqual(result.disputed, []);
  assert.equal(result.complete, true);
});

test('independent confirmations publish once, and a disagreement remains visible', async () => {
  const model = { complete: async ({ payload }) => payload.stage === 'review'
    ? { reviewed: payload.units.map(u => u.id), findings: [finding], limitations: [] }
    : { accepted: payload.reviewer === 'architecture' ? payload.candidates.map(c => c.id) : [], rejected: payload.reviewer === 'storage' ? payload.candidates.map(c => ({ id: c.id, reason: 'Needs contract confirmation.' })) : [], limitations: [] } };
  const result = await runReview({ changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: {}, model });
  assert.equal(result.findings.length, 0);
  assert.equal(result.disputed.length, 1);
  model.complete = async ({ payload }) => payload.stage === 'review'
    ? { reviewed: payload.units.map(u => u.id), findings: [finding], limitations: [] }
    : { accepted: payload.candidates.map(c => c.id), rejected: [], limitations: [] };
  const accepted = await runReview({ changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: {}, model });
  assert.equal(accepted.findings.length, 1);
  assert.equal(accepted.findings[0].confirmedBy.length, 2);
});

test('missing coverage or an unclassified candidate fails instead of reporting LGTM', async () => {
  const input = { changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: {} };
  await assert.rejects(runReview({ ...input, model: { complete: async () => ({ reviewed: [], findings: [], limitations: [] }) } }), /coverage/i);
  await assert.rejects(runReview({ ...input, model: { complete: async ({ payload }) => payload.stage === 'review'
    ? { reviewed: payload.units.map(u => u.id), findings: [finding], limitations: [] }
    : { accepted: [], rejected: [], limitations: [] } } }), /candidate/i);
});

test('a Git tool failure remains incomplete even when the model claims a clean review', async () => {
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async (_url, init) => {
    const messages = JSON.parse(init.body).messages;
    const payload = JSON.parse(messages[1].content);
    const choice = messages.some(m => m.role === 'tool')
      ? { finish_reason: 'stop', message: { content: JSON.stringify({ reviewed: payload.units.map(u => u.id), findings: [], limitations: [] }) } }
      : { finish_reason: 'tool_calls', message: { content: null, tool_calls: [{ id: 'read', function: { name: 'read_file', arguments: '{"path":"reader.scala"}' } }] } };
    return new Response(JSON.stringify({ choices: [choice] }), { status: 200 });
  } });
  const result = await runReview({ changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: { readFile: async () => { throw new Error('Git read timed out'); } }, model });
  assert.equal(result.complete, false);
  assert.match(result.limitations.join(' '), /Git read timed out/);
});

test('type-change file headers are not interpreted as diff content lines', () => {
  const patch = 'diff --git a/link b/link\ndeleted file mode 100644\n--- a/link\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\ndiff --git a/link b/link\nnew file mode 120000\n--- /dev/null\n+++ b/link\n@@ -0,0 +1 @@\n+target\n';
  assert.deepEqual([...diffLines(patch)].sort(), ['LEFT:1', 'RIGHT:1']);
  assert.ok(diffLines('@@ -0,0 +1 @@\n+++ actual source text\n').has('RIGHT:1'));
});

test('the trace names the reviewer, stage and batch of every round and is saved as it grows', async () => {
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async (_url, init) => {
    const messages = JSON.parse(init.body).messages;
    const payload = JSON.parse(messages[1].content);
    const choice = messages.some(m => m.role === 'tool')
      ? { finish_reason: 'stop', message: { content: JSON.stringify({ reviewed: payload.units.map(u => u.id), findings: [], limitations: [] }) } }
      : { finish_reason: 'tool_calls', message: { content: null, tool_calls: [{ id: 'read', function: { name: 'read_file', arguments: '{"path":"reader.scala"}' } }] } };
    return new Response(JSON.stringify({ choices: [choice] }), { status: 200 });
  } });
  const saved = [];
  const result = await runReview({ changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: { readFile: async () => ({ content: 'val reader = 1' }) }, model, onProgress: async r => { saved.push(r.trace.length); } });
  assert.equal(result.complete, true);
  assert.equal(result.trace.length, 4);
  assert.deepEqual([...new Set(result.trace.map(t => t.reviewer))].sort(), ['architecture', 'storage']);
  assert.ok(result.trace.every(t => t.stage === 'review' && t.batch === 0 && t.group === null));
  assert.deepEqual(result.trace.filter(t => t.reviewer === 'storage').map(t => t.round), [0, 1]);
  assert.ok(saved.includes(1), 'saved after the first round');
  assert.ok(!JSON.stringify(result.trace).includes('val reader'));
});

test('a failing reviewer waits for its sibling, so both traces are complete before the failure surfaces', async () => {
  const model = { complete: async ({ payload, onRound }) => {
    await onRound({ round: 0, toolCalls: [] });
    if (payload.reviewer === 'architecture') throw new Error('Model tool budget exhausted; review incomplete');
    await new Promise(resolve => setTimeout(resolve, 20));
    await onRound({ round: 1, toolCalls: [] });
    return { reviewed: payload.units.map(u => u.id), findings: [], limitations: [] };
  } };
  let last;
  await assert.rejects(runReview({ changes: [change(finding.path, patch)], config, rules: '', context: {}, repository: {}, model, onProgress: async r => { last = r; } }), /budget/);
  assert.deepEqual(last.trace.map(t => `${t.reviewer}:${t.round}`).sort(), ['architecture:0', 'storage:0', 'storage:1']);
});
