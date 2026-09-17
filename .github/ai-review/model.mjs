const properties = {
  path: { type: 'string' }, ref: { type: 'string', enum: ['head', 'base', 'merge-base'] },
  offset: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1 },
};
const definitions = {
  read_file: { description: 'Read a tracked file from a fixed Git commit. Follow next_offset to read more. Symlinks return their target text only.', properties, required: ['path'] },
  list_files: { description: 'List tracked paths, including documentation. Follow next_offset for more.', properties: { ref: properties.ref, prefix: { type: 'string' }, offset: properties.offset, limit: properties.limit } },
  search: { description: 'Search literal text in fixed Git objects. Follow next_offset for more.', properties: { ref: properties.ref, pattern: { type: 'string' }, offset: properties.offset, limit: properties.limit }, required: ['pattern'] },
};

// Tool arguments are model output, never gateway response text; keep them readable even when malformed.
function describeArguments(raw) {
  if (typeof raw !== 'string') return raw ?? null;
  try { return JSON.parse(raw); } catch { return { unparsed: raw.slice(0, 500) }; }
}

function describeUsage(usage) {
  return usage && typeof usage === 'object' ? { promptTokens: usage.prompt_tokens ?? null, completionTokens: usage.completion_tokens ?? null } : null;
}

export class Model {
  constructor({ url, key, model, fetchImpl = fetch, maxToolRounds = 16 }) {
    const endpoint = new URL(url);
    if (endpoint.protocol !== 'https:' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new Error('Model URL must be an HTTPS endpoint without credentials or query');
    this.url = `${url.replace(/\/$/, '').replace(/\/v1$/, '')}/v1/chat/completions`;
    this.key = key;
    this.model = model;
    this.fetch = fetchImpl;
    this.maxToolRounds = maxToolRounds;
  }

  async complete({ system, payload, tools, onRound = async () => {} }) {
    const messages = [{ role: 'system', content: system }, { role: 'user', content: JSON.stringify(payload) }];
    const toolFailures = [];
    const available = Object.keys(tools).map(name => {
      if (!definitions[name]) throw new Error(`Unknown tool: ${name}`);
      const { description, properties, required = [] } = definitions[name];
      return { type: 'function', function: { name, description, parameters: { type: 'object', properties, required, additionalProperties: false } } };
    });
    for (let round = 0; round <= this.maxToolRounds; round++) {
      const started = Date.now();
      // The last request forbids tools, so an exhausted budget still yields a structured answer; review.mjs records the exhaustion.
      const forced = round === this.maxToolRounds;
      // One trace entry per request: which tools were asked for and how the round ended. Model and gateway text is never copied into it.
      const entry = { round, forced, elapsedMs: 0, httpStatus: null, finishReason: null, usage: null, contentChars: 0, toolCalls: [] };
      const record = async () => { entry.elapsedMs = Date.now() - started; await onRound(entry); };
      const fail = async message => { await record(); throw new Error(message); };
      let response;
      try {
        response = await this.fetch(this.url, {
          method: 'POST', redirect: 'error', signal: AbortSignal.timeout(180_000),
          headers: { Authorization: `Bearer ${this.key}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: this.model, messages, max_tokens: 8192, response_format: { type: 'json_object' },
            ...(available.length ? { tools: available, ...(forced ? { tool_choice: 'none' } : {}) } : {}),
          }),
        });
      } catch (error) {
        // Only the error class is traced: a transport message can name the gateway host, which is a secret.
        entry.error = error.name || 'Error';
        await record();
        throw error;
      }
      entry.httpStatus = response.status;
      // Never log gateway response bodies: they can contain echoed request credentials.
      if (!response.ok) await fail(`Model request failed: HTTP ${response.status}`);
      const data = await response.json();
      const choice = data.choices?.[0];
      const message = choice?.message;
      entry.finishReason = choice?.finish_reason ?? null;
      entry.usage = describeUsage(data.usage);
      entry.contentChars = typeof message?.content === 'string' ? message.content.length : 0;
      entry.toolCalls = (message?.tool_calls || []).map(call => ({ name: call.function?.name ?? null, arguments: describeArguments(call.function?.arguments) }));
      if (!choice || choice.finish_reason === 'length') await fail('Model response missing or truncated');
      if (message?.tool_calls?.length) {
        if (round === this.maxToolRounds) await fail('Model tool budget exhausted; review incomplete');
        if (message.tool_calls.length > 32) await fail('Too many tool calls; review incomplete');
        messages.push(message);
        for (const [index, call] of message.tool_calls.entries()) {
          let result;
          const name = call.function?.name;
          if (!Object.hasOwn(tools, name)) await fail('Model requested an unavailable tool');
          try {
            result = await tools[name](JSON.parse(call.function.arguments));
          } catch (error) {
            result = { error: error.message };
            toolFailures.push(`${name}: ${error.message}`);
            entry.toolCalls[index].error = error.message;
          }
          const content = JSON.stringify(result);
          entry.toolCalls[index].resultChars = content.length;
          messages.push({ role: 'tool', tool_call_id: call.id, content });
        }
        await record();
        continue;
      }
      if (choice.finish_reason !== 'stop' || typeof message?.content !== 'string') await fail('Model did not finish a structured review');
      const text = message.content.trim().replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
      let parsed;
      try { parsed = JSON.parse(text); } catch { await fail('Model returned invalid review JSON'); }
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') await fail('Model returned invalid review object');
      await record();
      // Execution state is trusted program state; model output can neither erase nor inject it.
      return { ...parsed, toolFailures, exhaustedAfter: forced ? this.maxToolRounds : 0 };
    }
    throw new Error('Model review incomplete');
  }
}
