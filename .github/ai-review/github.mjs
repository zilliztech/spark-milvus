const marker = '<!-- ai-review -->';
const isReviewer = comment => comment.user?.type === 'Bot' && comment.user?.login === 'github-actions[bot]';
const escape = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('@', '&#64;');

function findingBody(finding) {
  return `**[${finding.priority}] ${escape(finding.title)}**\n\n${escape(finding.scenario)}\n\n${(finding.evidence || []).map(e => `- ${escape(e.path)}:${e.line} (${e.ref}): ${escape(e.detail)}`).join('\n')}\n\n<!-- ai-review-finding:${finding.id} -->`;
}

export function renderSummary(pr, result, runUrl) {
  const paths = new Set(result.coverage.map(c => c.path));
  const checked = result.coverage.filter(c => c.reviewedBy.length === 2).length;
  const verdict = !result.complete ? 'INCOMPLETE — human inspection or another run is required'
    : result.findings.length ? `${result.findings.length} confirmed finding(s)`
    : result.disputed.length ? `${result.disputed.length} disagreement(s) require human review` : 'LGTM';
  let body = `${marker}\n<!-- reviewed-sha: ${pr.head.sha} -->\n**AI Review** | ${verdict}\n\nCommit: \`${pr.head.sha}\` · Base: \`${pr.base.sha}\`\nCoverage: ${checked}/${result.coverage.length} chunks across ${paths.size} changed files.\n[Full findings and coverage artifact](${runUrl})\n`;
  // Keep the complete report in the artifact; never present omitted text as reviewed/clean.
  let omitted = 0;
  const append = section => { if (body.length + section.length < 58000) body += section; else omitted++; };
  for (const finding of result.findings) append(`\n- **${finding.priority}** ${escape(finding.path)}${finding.line ? `:${finding.line}` : ''}: ${escape(finding.title)} — ${escape(finding.scenario)}\n`);
  if (result.disputed.length) append('\n**Reviewer disagreements — human decision required**\n');
  for (const finding of result.disputed) append(`\n- ${escape(finding.path)}: ${escape(finding.title)} — ${escape(finding.scenario)}\n${(finding.rejections || []).map(r => `  ${escape(r.reviewer)}: ${escape(r.reason)}`).join('\n')}\n`);
  if (result.limitations.length) append('\n**Incomplete inspection**\n');
  for (const limitation of result.limitations) append(`\n- ${escape(limitation)}\n`);
  if (omitted) body += `\n${omitted} report section(s) exceed the comment limit; see the complete artifact above.\n`;
  return body;
}

export class GitHub {
  constructor(repository, token, fetchImpl = fetch) {
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) throw new Error('Invalid GitHub repository');
    this.repository = repository;
    this.token = token;
    this.fetch = fetchImpl;
  }

  async request(path, method = 'GET', body) {
    const response = await this.fetch(`https://api.github.com/repos/${this.repository}/${path}`, {
      method, redirect: 'error', signal: AbortSignal.timeout(60_000),
      headers: { Authorization: `Bearer ${this.token}`, Accept: 'application/vnd.github+json', 'Content-Type': 'application/json', 'X-GitHub-Api-Version': '2022-11-28' },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (!response.ok) throw new Error(`GitHub ${method} failed: HTTP ${response.status}`);
    return response.status === 204 ? null : response.json();
  }

  async paginate(path) {
    const all = [];
    for (let page = 1; ; page++) {
      const items = await this.request(`${path}?per_page=100&page=${page}`);
      if (!Array.isArray(items)) throw new Error('Invalid GitHub page');
      all.push(...items);
      if (items.length < 100) return all;
    }
  }

  async getPR(number) {
    if (!Number.isSafeInteger(number) || number < 1) throw new Error('Invalid PR number');
    return this.request(`pulls/${number}`);
  }

  async assertCurrent(pr) {
    const current = await this.getPR(pr.number);
    if (current.state !== 'open' || current.head.sha !== pr.head.sha || current.base.sha !== pr.base.sha) throw new Error('PR changed or closed; stale review will not be published');
  }

  async publish(pr, result, runUrl) {
    await this.assertCurrent(pr);
    const comments = await this.paginate(`issues/${pr.number}/comments`);
    if (result.complete && result.findings.length) {
      const existing = await this.paginate(`pulls/${pr.number}/comments`);
      const pending = result.findings.filter(f => f.line !== null && !existing.some(c => isReviewer(c) && c.body?.includes(`<!-- ai-review-finding:${f.id} -->`)));
      const inline = pending.map(f => ({ path: f.path, line: f.line, side: f.side, body: findingBody(f) })).filter(c => c.body.length < 60000);
      for (let offset = 0; offset < inline.length; offset += 20) {
        await this.assertCurrent(pr);
        await this.request(`pulls/${pr.number}/reviews`, 'POST', { commit_id: pr.head.sha, event: 'COMMENT', comments: inline.slice(offset, offset + 20) });
      }
    }
    await this.assertCurrent(pr);
    const summary = comments.find(c => isReviewer(c) && c.body?.startsWith(marker));
    const body = renderSummary(pr, result, runUrl);
    if (summary) await this.request(`issues/comments/${summary.id}`, 'PATCH', { body });
    else await this.request(`issues/${pr.number}/comments`, 'POST', { body });
  }
}
