import test from 'node:test';
import assert from 'node:assert/strict';
import { Model } from '../model.mjs';

function response(message, finish_reason = 'stop') {
  return new Response(JSON.stringify({ choices: [{ message, finish_reason }] }), { status: 200 });
}

test('tool output is returned to the model without exposing filesystem or arbitrary tools', async () => {
  let calls = 0;
  const model = new Model({ url: 'https://model.invalid/v1', key: 'test-only', model: 'test', fetchImpl: async (_url, init) => {
    const sent = JSON.parse(init.body);
    assert.deepEqual(sent.response_format, { type: 'json_object' });
    if (calls++ === 0) return response({ content: null, tool_calls: [{ id: '1', type: 'function', function: { name: 'read_file', arguments: '{"path":"docs/a.md"}' } }] }, 'tool_calls');
    assert.equal(sent.messages.at(-1).content, '{"content":"document from Git"}');
    return response({ content: '{"findings":[]}' });
  } });
  assert.deepEqual(await model.complete({ system: '', payload: {}, tools: { read_file: async () => ({ content: 'document from Git' }) } }), { findings: [], toolFailures: [] });
});

test('invalid JSON remains a visible failure if the gateway ignores the requested format', async () => {
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async () => response({ content: 'The documentation looks correct.' }) });
  await assert.rejects(model.complete({ system: 'Return JSON.', payload: {}, tools: {} }), /invalid review JSON/);
});

test('truncated model responses and exhausted tools fail visibly', async () => {
  const truncated = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async () => response({ content: '{"findings":[]}' }, 'length') });
  await assert.rejects(truncated.complete({ system: '', payload: {}, tools: {} }), /truncat/i);
  const loop = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', maxToolRounds: 1, fetchImpl: async () => response({ content: null, tool_calls: [{ id: '1', function: { name: 'read_file', arguments: '{}' } }] }, 'tool_calls') });
  await assert.rejects(loop.complete({ system: '', payload: {}, tools: { read_file: async () => ({ content: 'x' }) } }), /budget/i);
});

test('redirects cannot forward the model credential to another origin', async () => {
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async (_url, init) => {
    assert.equal(init.redirect, 'error');
    return new Response('', { status: 302, headers: { location: 'https://other.invalid' } });
  } });
  const rounds = [];
  await assert.rejects(model.complete({ system: '', payload: {}, tools: {}, onRound: async entry => { rounds.push(entry); } }), /302/);
  assert.equal(rounds.length, 1);
  assert.equal(rounds[0].httpStatus, 302);
});

test('every round is traced with tool names, arguments and result sizes, never response text', async () => {
  let calls = 0;
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async () => calls++ === 0
    ? new Response(JSON.stringify({ choices: [{ message: { content: null, tool_calls: [{ id: '1', function: { name: 'read_file', arguments: '{"path":"docs/a.md"}' } }] }, finish_reason: 'tool_calls' }], usage: { prompt_tokens: 1200, completion_tokens: 30 } }), { status: 200 })
    : response({ content: '{"findings":[]}' }) });
  const rounds = [];
  await model.complete({ system: '', payload: {}, tools: { read_file: async () => ({ content: 'document from Git' }) }, onRound: async entry => { rounds.push(entry); } });
  assert.equal(rounds.length, 2);
  assert.equal(rounds[0].round, 0);
  assert.equal(rounds[0].finishReason, 'tool_calls');
  assert.deepEqual(rounds[0].usage, { promptTokens: 1200, completionTokens: 30 });
  assert.deepEqual(rounds[0].toolCalls, [{ name: 'read_file', arguments: { path: 'docs/a.md' }, resultChars: '{"content":"document from Git"}'.length }]);
  assert.equal(rounds[1].finishReason, 'stop');
  assert.equal(rounds[1].contentChars, '{"findings":[]}'.length);
  assert.deepEqual(rounds[1].toolCalls, []);
  assert.ok(!JSON.stringify(rounds).includes('document from Git'));
  assert.ok(!JSON.stringify(rounds).includes('findings'));
});

test('an exhausted tool budget leaves every requested call in the trace, including the unexecuted last round', async () => {
  const rounds = [];
  const loop = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', maxToolRounds: 2, fetchImpl: async () => response({ content: null, tool_calls: [{ id: '1', function: { name: 'search', arguments: '{"pattern":"delete"}' } }] }, 'tool_calls') });
  await assert.rejects(loop.complete({ system: '', payload: {}, tools: { search: async () => ({ matches: [] }) }, onRound: async entry => { rounds.push(entry); } }), /budget/i);
  assert.deepEqual(rounds.map(r => r.round), [0, 1, 2]);
  assert.equal(rounds[0].toolCalls[0].resultChars, '{"matches":[]}'.length);
  assert.deepEqual(rounds.at(-1).toolCalls, [{ name: 'search', arguments: { pattern: 'delete' } }]);
});

test('malformed tool arguments are traced as text and reported as a tool failure', async () => {
  let calls = 0;
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async () => calls++ === 0
    ? response({ content: null, tool_calls: [{ id: '1', function: { name: 'read_file', arguments: '{"path": docs/a.md}' } }] }, 'tool_calls')
    : response({ content: '{"findings":[]}' }) });
  const rounds = [];
  const result = await model.complete({ system: '', payload: {}, tools: { read_file: async () => ({ content: 'x' }) }, onRound: async entry => { rounds.push(entry); } });
  assert.equal(result.toolFailures.length, 1);
  assert.deepEqual(rounds[0].toolCalls[0].arguments, { unparsed: '{"path": docs/a.md}' });
  assert.match(rounds[0].toolCalls[0].error, /JSON/);
});

test('a transport failure is traced by error class only, never by message or host', async () => {
  const model = new Model({ url: 'https://model.invalid', key: 'test-only', model: 'test', fetchImpl: async () => { const error = new Error('fetch failed: model.invalid'); error.name = 'TimeoutError'; throw error; } });
  const rounds = [];
  await assert.rejects(model.complete({ system: '', payload: {}, tools: {}, onRound: async entry => { rounds.push(entry); } }), /fetch failed/);
  assert.equal(rounds.length, 1);
  assert.equal(rounds[0].error, 'TimeoutError');
  assert.equal(rounds[0].httpStatus, null);
  assert.ok(!JSON.stringify(rounds).includes('model.invalid'));
});
