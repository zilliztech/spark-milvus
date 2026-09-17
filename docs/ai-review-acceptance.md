# AI review acceptance

This page records how the AI review workflow is exercised on a
documentation-only pull request.

AI review excludes Markdown and HTML files by default. Pull requests that only
change documentation do not receive an AI review, so maintainers must review
their content manually.

Run the reviewer's own tests before changing it:

    node --test .github/ai-review/*.test.mjs
