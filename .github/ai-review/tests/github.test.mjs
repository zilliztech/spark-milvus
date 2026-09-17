import test from 'node:test';
import assert from 'node:assert/strict';
import { GitHub, renderSummary } from '../github.mjs';

const pr = { number: 4, state: 'open', base: { sha: 'a'.repeat(40), ref: 'refactor/v2' }, head: { sha: 'b'.repeat(40) } };
const runUrl = 'https://github.com/example/repo/actions/runs/12';
const empty = { complete: true, findings: [], disputed: [], limitations: [], coverage: [{ path: 'docs/a.md', reviewedBy: ['a', 'b'] }] };
const json = data => new Response(JSON.stringify(data), { status: 200 });

test('a user cannot impersonate the summary bot; all comment pages are inspected', async () => {
  const writes = [];
  const github = new GitHub('example/repo', 'test-only', async (url, init) => {
    if (init.method !== 'GET') { writes.push({ url, body: JSON.parse(init.body) }); return json({}); }
    if (url.includes('/issues/4/comments')) {
      if (url.endsWith('page=1')) return json(Array.from({ length: 100 }, (_, id) => ({ id, body: '<!-- ai-review -->', user: { type: 'User', login: 'attacker' } })));
      return json([{ id: 777, body: '<!-- ai-review -->', user: { type: 'Bot', login: 'github-actions[bot]' } }]);
    }
    if (url.includes('/pulls/4/comments')) return json([]);
    return json(pr);
  });
  await github.publish(pr, empty, runUrl);
  assert.equal(writes.length, 1);
  assert.ok(writes[0].url.endsWith('/issues/comments/777'));
  assert.match(writes[0].body.body, /LGTM/);
});

test('a changed PR head prevents any publication', async () => {
  const github = new GitHub('example/repo', 'test-only', async (_url, init) => {
    assert.equal(init.method, 'GET');
    return json({ ...pr, head: { sha: 'c'.repeat(40) } });
  });
  await assert.rejects(github.publish(pr, empty, runUrl), /changed/);
});

test('inline findings are attached to the reviewed SHA and not repeated', async () => {
  const writes = [];
  const result = { ...empty, findings: [{ id: 'abc', path: 'docs/a.md', line: 2, side: 'RIGHT', priority: 'P1', title: 'Wrong command', scenario: 'The command fails.', evidence: [{ path: 'reader.scala', ref: 'head', line: 3, detail: 'No such option.' }] }] };
  let existing = [];
  const github = new GitHub('example/repo', 'test-only', async (url, init) => {
    if (init.method !== 'GET') { writes.push({ url, body: JSON.parse(init.body) }); return json({}); }
    if (url.includes('/pulls/4/comments')) return json(existing);
    if (url.includes('/issues/4/comments')) return json([]);
    return json(pr);
  });
  await github.publish(pr, result, runUrl);
  const review = writes.find(w => w.url.endsWith('/reviews'));
  assert.equal(review.body.commit_id, 'b'.repeat(40));
  assert.equal(review.body.comments[0].side, 'RIGHT');
  existing = [{ body: review.body.comments[0].body, user: { type: 'Bot', login: 'github-actions[bot]' } }];
  writes.length = 0;
  await github.publish(pr, result, runUrl);
  assert.equal(writes.filter(w => w.url.endsWith('/reviews')).length, 0);
});

test('unsupported content and reviewer disagreements cannot produce LGTM', () => {
  assert.doesNotMatch(renderSummary(pr, { ...empty, complete: false, limitations: ['Binary file: a.png'] }, runUrl), /LGTM/);
  assert.doesNotMatch(renderSummary(pr, { ...empty, disputed: [{ priority: 'P2', path: 'x', title: 'Contract unclear', scenario: 'Failure possible' }] }, runUrl), /LGTM/);
});

test('reviewer caveats are published without INCOMPLETE and without an unqualified LGTM', () => {
  const body = renderSummary(pr, { ...empty, caveats: ['storage: The workflow was not executed.'] }, runUrl);
  assert.match(body, /No confirmed findings; 1 reviewer caveat/);
  assert.match(body, /\*\*Reviewer caveats\*\*/);
  assert.match(body, /storage: The workflow was not executed\./);
  assert.doesNotMatch(body, /INCOMPLETE/);
  assert.doesNotMatch(body, /LGTM/);
});

test('a finding reported by both reviewers says so in the summary and inline', async () => {
  const finding = { id: 'abc', path: 'docs/a.md', line: 2, side: 'RIGHT', priority: 'P1', title: 'Wrong command', scenario: 'The command fails.', reportedBy: ['a', 'b'], evidence: [{ path: 'reader.scala', ref: 'head', line: 3, detail: 'No such option.' }] };
  assert.match(renderSummary(pr, { ...empty, findings: [finding] }, runUrl), /Wrong command · reported by both reviewers/);
  const writes = [];
  const github = new GitHub('example/repo', 'test-only', async (url, init) => {
    if (init.method !== 'GET') { writes.push({ url, body: JSON.parse(init.body) }); return json({}); }
    if (url.includes('/comments')) return json([]);
    return json(pr);
  });
  await github.publish(pr, { ...empty, findings: [finding] }, runUrl);
  assert.match(writes.find(w => w.url.endsWith('/reviews')).body.comments[0].body, /reported by both reviewers/);
});
