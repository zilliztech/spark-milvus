---
name: spark-milvus-sbt
description: Use when preparing any Spark-Milvus commit, or reviewing or changing its sbt build, module wiring, shared settings, Spark version matrix, test configurations, assembly or publication metadata.
---

# Maintain the Spark-Milvus build

Read the repository [AGENTS.md](../../../AGENTS.md), then
[sbt principles and practices](../../../docs/design/engineering/sbt.html). The document owns
the build conventions; this skill applies them. Consult
[modules.md](../../../docs/design/architecture/modules.md) when a change affects module
boundaries or compatibility, and [contributing.md](../../../docs/contributing.md)
for build commands and environment constraints.

For commit preparation without build changes, go directly to
[validation](#validate-before-committing); the build review steps
below apply when build settings change.

## Establish the requested scope

For discussion or review, stay read-only. For an authorized change, identify the
maintenance problem the change will solve before choosing helpers or files.
Record the current commit and local diff; preserve concurrent work and use the
fixed baseline when comparing results.

Review the affected module declarations, settings, dependencies and caller
scripts together. Do not infer effective sbt values from a single file or a
historical successful build.

## Make the build locally understandable

Keep code dependencies, publication policy and service-dependent tests visible
at module declarations. Extract a setting when its users share one rule; a
single-use implementation block can also be named when that makes a module's
purpose clearer. Do not optimize for line count, a fixed number of files, or
removing every repeated literal.

Use the document's actual patterns: one factory for Spark lines, direct apps and
integration declarations, an explicit publication decision, and named root
run/assembly/publication settings. Read helper implementations before changing
their scope; names must account for their effects.

Keep module IDs, directories, dependency scopes and exclusions, version values,
task aggregation, resource paths and artifact coordinates stable during a
structural cleanup unless changing them is part of the request. Account for the
temporary root artifact and upstream JNI binding described in the document.
Follow AGENTS.md's removal rule; a grep result alone does not authorize removing
a user-facing contract.

Update the canonical sbt document when a convention changes and record the
decision in [the design log](../../../docs/design/README.md#6-决策日志). Link to
existing policy instead of copying it into AGENTS.md or this skill. Keep code
and build comments in English, and the design document in Chinese.

## Validate before committing

Before every commit, including documentation-only commits, run formatting and
then the formatting checks using the
[pre-commit formatting commands](../../../docs/contributing.md#formatting).
Use the repository's pinned Scalafmt configuration and Java 21. Serialize
formatting tasks: the four Spark projects share `spark-base`, and concurrent
writes have truncated a source file. Do not run multiple formatter processes
against the same checkout.

Review the resulting diff before staging; a passing formatter check does not
detect a source file accidentally reduced to empty content. Resolve formatting
failures before committing, preserve concurrent work, and keep unrelated
formatting repairs separate from functional changes. `git diff --check` is
required too, but does not replace `scalafmtCheckAll`.

After formatting and checks pass, run the full root unit test suite with Java
21 using the [unit test commands](../../../docs/contributing.md#unit-tests).
It must pass before every commit, including documentation-only commits;
compilation needed by the test task is part of this verification. Focused
`testOnly` runs help during development but do not replace the full suite.

Do not commit after a test failure, aborted suite or incomplete run. Fix the
cause and rerun the full root suite. Do not add exclusions, filters, ignored
tests or cancellation conditions to obtain a passing result. Report the actual
test and suite counts, including existing native-library and UAT cancellations
and their reasons, as described in contributing.md; canceled tests did not pass.

When build settings change, compare the before/after module graph, aggregation
and affected dependency or publication settings. Additional Scala cross-version
checks, integration services, native builds and publication follow the session's
scope and authorization. Report static checks separately from commands actually
run, and do not claim runtime compatibility from compilation or POM correctness
from JAR contents.

Read back the final diff and current branch state before reporting completion.
Creating this skill or changing the build does not authorize a marketplace PR,
external notifications, publishing artifacts or deploying services.
