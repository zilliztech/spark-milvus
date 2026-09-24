---
name: spark-milvus-storage-auth
description: Use when touching object storage credentials, provider chains, per-bucket configuration, the fs.* property bag handed to milvus-storage, or any code that builds a Hadoop Configuration for S3, OSS, GCS or Azure.
---

# Object storage authentication

Read [AGENTS.md](../../../AGENTS.md), then
[the mechanism and design](../../../docs/design/architecture/storage-auth.html).
That document owns the conventions and records the measured facts behind them;
this skill applies it.

The connector reaches object storage through milvus-storage's C filesystem: see
[spark-milvus-storage-access](../spark-milvus-storage-access/SKILL.md). Hadoop
keys survive only as translation input in `HadoopStorageKeys`, and the Hadoop
`FileSystem` is used only by the backfill app and by search query-file reads;
this skill covers that residue. A change here should not make it harder to
delete.

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
5. One copy of the logic. `core.credential.StorageProperties` is the only
   output and `HadoopStorageKeys` the only translation; the backfill app's
   Hadoop-side copy is a documented interim state. Do not add another.

## Both sides or neither

The connector reaches object storage through milvus-storage's filesystem
(`core.io.NativeObjectStore`); only the backfill app's source reads and the
query-file reads still go through Hadoop in the same pod. Both discover the same
identity when neither is explicitly configured. They diverge when one side is
configured and the other is not, so configure both consistently or leave both
alone. The native side takes
location and mode and, when IAM is in use, no secrets; the full key-name table
is section 3.3 of
[storage-access.html](../../../docs/design/architecture/storage-access.html).

## Verifying a change

A credential change cannot be validated by compilation. State which of the
scenarios above the change affects, and say plainly what was checked and what
was not. Instantiation of a provider class fails at connect time, not compile
time, so a class name change needs a run against the real backend before it is
called done. Removing a dependency that supplies a provider class is the same
kind of change as removing the provider itself.
