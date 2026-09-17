import { readFile, writeFile, mkdir, appendFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { GitHub, renderSummary } from './github.mjs';
import { Model } from './model.mjs';
import { Repository } from './repository.mjs';
import { runReview } from './review.mjs';

const directory = dirname(fileURLToPath(import.meta.url));
export async function run({ env = process.env, fetchImpl = fetch, repositoryFactory = Repository.fetch } = {}) {
  const output = env.REVIEW_OUTPUT_DIR;
  if (!output) throw new Error('REVIEW_OUTPUT_DIR is required');
  await mkdir(output, { recursive: true });
  const github = new GitHub(env.GITHUB_REPOSITORY, env.GITHUB_TOKEN, fetchImpl);
  const runUrl = `https://github.com/${env.GITHUB_REPOSITORY}/actions/runs/${env.GITHUB_RUN_ID}`;
  let pr, repository;
  let exitCode = 0;
  let report = { complete: false, findings: [], disputed: [], limitations: ['Review has not completed.'], coverage: [] };

  let saving = Promise.resolve();
  function save(result) {
    report = result;
    // Both reviewers report progress concurrently; one write at a time keeps review.json from interleaving two snapshots.
    saving = saving.catch(() => {}).then(async () => {
      await writeFile(join(output, 'review.json'), JSON.stringify({ head: pr?.head.sha, base: pr?.base.sha, ...result }, null, 2));
      if (pr) await writeFile(join(output, 'review.md'), renderSummary(pr, result, runUrl));
    });
    return saving;
  }

  try {
    const event = JSON.parse(await readFile(env.GITHUB_EVENT_PATH, 'utf8'));
    pr = await github.getPR(Number(event.inputs?.pr_number || event.pull_request?.number));
    if (pr.state !== 'open') throw new Error('PR is closed');
    const config = JSON.parse(await readFile(join(directory, 'config.json'), 'utf8'));
    const rules = await readFile(join(directory, 'rules.md'), 'utf8');
    if (!env.MODEL_API_KEY || !env.MODEL_BASE_URL) throw new Error('Model gateway secrets are missing');
    repository = await repositoryFactory(env.GITHUB_REPOSITORY, pr.base.sha, pr.head.sha);
    const changes = await repository.changes();
    await writeFile(join(output, 'changes.json'), JSON.stringify(changes, null, 2));
    const background = [];
    for (const path of ['AGENTS.md', 'CLAUDE.md', 'README.md']) {
      // A missing legacy entry is normal. Other read errors remain visible failures.
      const matches = await repository.listFiles({ ref: 'base', prefix: path, limit: 1 });
      if (matches.files.includes(path)) background.push(await repository.readFile({ path, ref: 'base', limit: 12000 }));
    }
    await save(report);
    const model = new Model({ url: env.MODEL_BASE_URL, key: env.MODEL_API_KEY, model: config.model, maxToolRounds: config.maxToolRounds, fetchImpl });
    const result = await runReview({ changes, config, rules, context: { repository: env.GITHUB_REPOSITORY, targetBranch: pr.base.ref, base: pr.base.sha, head: pr.head.sha, title: pr.title, description: pr.body, background }, repository, model, onProgress: save });
    await github.publish(pr, result, runUrl);
    if (!result.complete) exitCode = 1;
  } catch (error) {
    const message = error.message || 'Review failed';
    await save({ ...report, complete: false, limitations: [...report.limitations, message] });
    console.error(JSON.stringify({ error: message }));
    if (pr) {
      try { await github.publish(pr, report, runUrl); }
      catch (publicationError) { console.error(JSON.stringify({ publicationError: publicationError.message })); }
    }
    exitCode = 1;
  } finally {
    if (env.GITHUB_STEP_SUMMARY && pr) await appendFile(env.GITHUB_STEP_SUMMARY, renderSummary(pr, report, runUrl));
    if (repository) await repository.close();
  }

  return exitCode;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  process.exitCode = await run();
}
