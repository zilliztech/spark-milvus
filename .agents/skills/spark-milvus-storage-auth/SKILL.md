---
name: spark-milvus-storage-auth
description: Use when touching object storage credentials, provider chains, per-bucket configuration, the fs.* property bag handed to milvus-storage, or any code that builds a Hadoop Configuration for S3, OSS, GCS or Azure.
---

# Object storage authentication

Read [AGENTS.md](../../../AGENTS.md), then
[the mechanism and design](../../../docs/design/architecture/storage-auth.html).
That document owns the conventions and records the measured facts behind them;
this skill applies it. The wider storage layer is in
[storage-access.html](../../../docs/design/architecture/storage-access.html).

## Before changing anything

The credential code looks redundant and is not. Three of its branches exist for
scenarios that are easy to miss: a backfill job spans a source bucket and the
Milvus bucket which may belong to different accounts; a managed runtime may
already have selected an AssumeRole provider that must not be replaced by the
pod's ambient identity; and static keys need their provider pinned or a global
chain shadows them. The explicit IRSA chain works around an AWS SDK v1 default
chain that picks up the node's instance profile before the projected
service-account token.

Find out why a line exists before removing it. `git log --follow` on the file
and the commit messages behind the fix are usually explicit.

## The five rules

1. Never construct credentials; pass the Configuration you were given.
2. If you must write configuration, write per-bucket or per-account keys only,
   never global ones.
3. Never overwrite a provider that is already set; fill in only when absent.
4. Never bake in a default endpoint, bucket or key.
5. One copy of the logic. Credential construction is currently duplicated in six
   places; do not add a seventh.

## Both sides or neither

The JVM reaches object storage through Hadoop; milvus-storage reaches it through
its own SDK in the same pod. Both discover the same identity when neither is
explicitly configured. They diverge when one side is configured and the other is
not, so configure both consistently or leave both alone. The native side takes
location and mode — `fs.use_iam`, `fs.cloud_provider`, `fs.address`,
`fs.bucket_name`, `fs.root_path`, `fs.region` — and no secrets when IAM is in
use.

## Verifying a change

A credential change cannot be validated by compilation. State which of the
scenarios above the change affects, and say plainly what was checked and what
was not. Instantiation of a provider class fails at connect time, not compile
time, so a class name change needs a run against the real backend before it is
called done. Removing a dependency that supplies a provider class is the same
kind of change as removing the provider itself.
