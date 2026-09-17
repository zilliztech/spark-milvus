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
  await assert.rejects(model.complete({ system: '', payload: {}, tools: {} }), /302/);
});
