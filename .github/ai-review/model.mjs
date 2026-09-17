const properties = {
  path: { type: 'string' }, ref: { type: 'string', enum: ['head', 'base', 'merge-base'] },
  offset: { type: 'integer', minimum: 0 }, limit: { type: 'integer', minimum: 1 },
};
const definitions = {
  read_file: { description: 'Read a tracked file from a fixed Git commit. Follow next_offset to read more. Symlinks return their target text only.', properties, required: ['path'] },
  list_files: { description: 'List tracked paths, including documentation. Follow next_offset for more.', properties: { ref: properties.ref, prefix: { type: 'string' }, offset: properties.offset, limit: properties.limit } },
  search: { description: 'Search literal text in fixed Git objects. Follow next_offset for more.', properties: { ref: properties.ref, pattern: { type: 'string' }, offset: properties.offset, limit: properties.limit }, required: ['pattern'] },
};

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

  async complete({ system, payload, tools }) {
    const messages = [{ role: 'system', content: system }, { role: 'user', content: JSON.stringify(payload) }];
    const toolFailures = [];
    const available = Object.keys(tools).map(name => {
      if (!definitions[name]) throw new Error(`Unknown tool: ${name}`);
      const { description, properties, required = [] } = definitions[name];
      return { type: 'function', function: { name, description, parameters: { type: 'object', properties, required, additionalProperties: false } } };
    });
    for (let round = 0; round <= this.maxToolRounds; round++) {
      const response = await this.fetch(this.url, {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(180_000),
        headers: { Authorization: `Bearer ${this.key}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: this.model, messages, max_tokens: 8192, response_format: { type: 'json_object' },
          ...(available.length ? { tools: available } : {}),
        }),
      });
      // Never log gateway response bodies: they can contain echoed request credentials.
      if (!response.ok) throw new Error(`Model request failed: HTTP ${response.status}`);
      const data = await response.json();
      const choice = data.choices?.[0];
      if (!choice || choice.finish_reason === 'length') throw new Error('Model response missing or truncated');
      const message = choice.message;
      if (message?.tool_calls?.length) {
        if (round === this.maxToolRounds) throw new Error('Model tool budget exhausted; review incomplete');
        if (message.tool_calls.length > 32) throw new Error('Too many tool calls; review incomplete');
        messages.push(message);
        for (const call of message.tool_calls) {
          let result;
          const name = call.function?.name;
          if (!Object.hasOwn(tools, name)) throw new Error('Model requested an unavailable tool');
          try {
            result = await tools[name](JSON.parse(call.function.arguments));
          } catch (error) {
            result = { error: error.message };
            toolFailures.push(`${name}: ${error.message}`);
          }
          messages.push({ role: 'tool', tool_call_id: call.id, content: JSON.stringify(result) });
        }
        continue;
      }
      if (choice.finish_reason !== 'stop' || typeof message?.content !== 'string') throw new Error('Model did not finish a structured review');
      const text = message.content.trim().replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
      let parsed;
      try { parsed = JSON.parse(text); } catch { throw new Error('Model returned invalid review JSON'); }
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') throw new Error('Model returned invalid review object');
      // Execution failures are trusted program state; model output cannot erase them.
      return { ...parsed, toolFailures };
    }
    throw new Error('Model review incomplete');
  }
}
