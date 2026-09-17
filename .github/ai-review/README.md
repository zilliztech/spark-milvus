# GitHub AI review

This dependency-free Node 22 program reviews the complete PR diff with two
independent reviewers, then cross-checks their findings. Repository-specific
policy is in `rules.md`; models, reviewer responsibilities and batch/tool budgets
are in `config.json`. There are no default file exclusions. Each push reviews
the whole PR again, including documentation, locks and generated files.

The workflow executes code from the upstream repository's trusted default
branch. It fetches PR commits into a separate bare Git repository and never
checks out or executes PR files. Model tools read Git objects, with explicit
pagination. They cannot access runner files, credentials, arbitrary commands or
arbitrary network endpoints. GitHub publication is performed by the program.

## Deployment

1. Merge `.github/ai-review/`, `ai-review.yml`, `ai-review-tests.yml` and the
   PR-Agent coordination change into the upstream default branch (`main`). A PR
   into `refactor/v2` alone does not deploy the default-branch program.
2. Keep the existing `ANTHROPIC_API_KEY` and `ANTHROPIC_BASE_URL` repository
   secrets. The gateway must support the configured model's OpenAI-compatible
   chat-completions API with function tools and JSON output
   (`response_format: {"type": "json_object"}`). Secret values are never copied
   into the repository or artifacts.
3. Set repository variable `ENABLE_AI_REVIEW=true`. PR-Agent then keeps its
   description and labels while disabling automatic review and code suggestions.
4. Use the **AI Review** workflow's manual PR-number input to validate a real
   open PR. Subsequent opens/pushes/reopens/ready events trigger automatically.

To turn off the new reviewer, set `ENABLE_AI_REVIEW=false`; PR-Agent resumes its
previous review behavior. No credential changes are required. The first
deployment PR is reviewed by the existing process because this workflow always
loads the already trusted default-branch implementation.

## Results and limitations

The bot updates one marked summary, stamps the reviewed head and base SHAs, and
deduplicates confirmed inline findings. Both reviewers must confirm an issue
after cross-checking before it is published inline. Two reviewers describing the
same changed line describe one finding: the more severe description is kept, the
evidence is merged and both reporters are named. Disagreements remain visible in
the summary and artifact. Old inline threads are not automatically resolved; the
summary is the result for the explicitly stamped revision.

Every text diff chunk is submitted to both reviewers. This is an accounting of
the supplied review inputs, not a guarantee that an AI can identify every bug.
Completeness is what the program observed: missing coverage, truncated output,
model/API failures, Git tool failures and unsupported content fail the run.
Binary files, submodule objects and Git LFS payloads receive metadata inspection
and an explicit human-inspection limitation; they cannot produce an unqualified
LGTM. When a reviewer has used its whole tool budget, its last request forbids
tools, the answer it gives is published, and the exhaustion is recorded as a
limitation: the run is incomplete but not empty. What a reviewer itself says it
could not verify is published as a caveat; caveats do not make a run
incomplete. Confirmed findings alone do not fail the job or submit
REQUEST_CHANGES; branch protection is configured separately by maintainers.

The `ai-review-<PR>-<attempt>` artifact contains the complete diff inventory,
structured findings, reviewer disagreements, limitations, per-chunk coverage and
the per-round model trace: for every request, the reviewer, stage, batch,
elapsed time, HTTP status, finish reason, token usage, and each requested tool
with its arguments and result size. Model and gateway response text is never
recorded. The report is saved after every round, so a cancelled/timed-out run
keeps the trace up to its last completed request. Such a run is not a completed
review; consult its workflow status and stamped summary SHA.
Comments that exceed GitHub's size limit link to the full artifact rather than
claiming that omitted report details are absent.

Increasing batch size or tool budgets changes cost and latency. No file-count
or extension filter reduces review scope. A run exceeding the workflow's time
limit must be treated as incomplete and rerun with an appropriate reviewed
configuration. Tests never call a paid model or publish GitHub comments.

## Local validation

```bash
node --test .github/ai-review/tests/*.test.mjs
```

Tests use real temporary Git repositories and model/HTTP fixtures. They cover
file completeness, special paths, symlink handling, binary limitations, model
truncation, cross-check decisions, stale commits and comment ownership.
