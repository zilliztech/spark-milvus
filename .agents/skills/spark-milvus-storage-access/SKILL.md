---
name: spark-milvus-storage-access
description: Use when opening, reading, listing or writing a file in object storage from the connector — snapshot JSON, segment manifests, delete files, parquet footers, column groups — or when normalising a storage path, or when building the fs.* / extfs.* property bag handed to milvus-storage.
---

# Storage access

Read [AGENTS.md](../../../AGENTS.md), then
[the design](../../../docs/design/architecture/storage-access.html). That
document owns the decision and the evidence; this skill applies it. Credentials
under Hadoop are in
[spark-milvus-storage-auth](../spark-milvus-storage-auth/SKILL.md).

## The decision

Every file is opened through milvus-storage's C filesystem, reached from the JVM
by JNI in `native-storage`. There is no second path. Do not add a Hadoop
`FileSystem.get`, a `java.io.FileInputStream`, or a parquet `InputFile` adapter
to reach storage — four such paths exist today and removing them is the work.

The route was chosen because the C layer already carries a complete filesystem
API, manifest parsing, transactions, and credential providers for six clouds
including Aliyun RAM/STS/OIDC, Tencent STS and Huawei STS. Rebuilding any of
that in Scala is the stovepipe AGENTS.md forbids.

## Three packages, three jobs

- `core.path` turns any of the six path spellings into `(bucket, key)`. Format
  knowledge — `_delta/`, `_metadata/manifest-N.avro` — does not live here.
- `core.credential` turns user options and `spark.hadoop.fs.*` into the property
  map. It is the only place that parses credentials. The key-name table is
  section 3.3 of the design.
- `core.io` holds the JNI handle and offers open, read, list, stat, create.

A format package asks for bytes at a path and gets bytes. If the words
credential, endpoint, or FileSystem appear in a format package's signature, the
layering is wrong.

## core takes one namespace and knows no clouds

`core.credential` accepts `fs.*` (and `extfs.<name>.*` for more than one bucket),
validates what is required, and hands the map to the C layer unchanged. No cloud
name appears in its code, and neither does `s3a`, `oss` or `abfs`. Per-cloud
dispatch happens in C and only in C: `CreateArrowFileSystem` picks a filesystem
producer from `fs.storage_type` and `fs.cloud_provider`, and the S3 producer
then picks a credential provider per cloud. Supporting one more cloud changes
the C dispatch table and no Scala.

The JNI layer marshals values and nothing else. A cloud name or a `fs.*` key
whose meaning is interpreted inside `native-storage` is a bug, not an
optimisation: it gives cloud knowledge a second home, which is the stovepipe
AGENTS.md forbids.

Reading `spark.hadoop.fs.s3a.*` and renaming keys is NOT core's job. That
translation belongs either to whoever produces the configuration (the managed
platform can emit `fs.*` directly) or to a layer-3 shim that exists only so
current deployments keep working and carries a written end condition. Putting it
in core hands back the very job Hadoop's per-scheme dispatch used to do for us,
and does it per-cloud. Section 3.3 of the design has the mapping table and says
who owns it.

The values themselves are reusable: the platform passes a role ARN, a session
name and an endpoint — never an access key, secret or session token — and the
machine identity is the web-identity token already projected into the pod (IRSA
on EKS, RRSA on ACK), which the C layer reads the same way. Reusable values do
not make it core's job to go read them.

## Multiple buckets

Use `extfs.<name>.<property>` to register one configuration per bucket and let
`loon_filesystem_get(properties, path)` match on address plus bucket. Never back
up, overwrite and restore global keys — that is what backfill does today against
`hadoop-aliyun`, it is unsafe under concurrency, and it is the pattern this
design exists to remove.

## Gaps in the C layer

These are gaps in the C layer today, not design choices. milvus-storage is our
own repository, so the answer to each is to fix it when it is needed, never to
work around it above.

- No `fs.session_token` property, so a caller cannot hand ready-made temporary
  credentials to the C layer. This does not affect the normal path — each
  process holds a provider that calls STS and renews itself, which is what both
  Hadoop and the C layer do and what keeps long jobs alive. It only blocks a
  user who wants to pass their own STS credentials as options.
- `fs.iam_endpoint` is read into config and marked deprecated; the Aliyun STS
  endpoint is hard-coded to `https://sts.aliyuncs.com/`.
- Aliyun session duration is not configurable; `load_frequency` is ignored on
  that path.

## The three boundaries

Each boundary carries one kind of thing, and nothing else:

- Spark ↔ layer 3: a serializable `InputPartition`, and `ColumnarBatch`.
- layer 3 ↔ core: `(bucket, key)`, the `fs.*` map, Arrow batches.
- core ↔ JNI: `Map[String, String]`, a path, `ArrowArray` / `ArrowSchema`.
- JNI ↔ C: `const char*` arrays, a process-local handle, `ArrowArrayStream`.

A handle is a pointer inside this process. It must never reach an
`InputPartition`, which Spark serializes and ships. Put the description there —
segment path, manifest version, column names, the `fs.*` map — and open the
reader on the executor.

## Predicates are evaluated on the JVM side

`core.io` does not expose `loon_segment_reader_get_filtered_stream`, and that is
deliberate rather than an omission. The C base class
`FormatReader::set_predicate` accepts a predicate, returns OK and ignores it;
only `VortexFormatReader` overrides it and Vortex is out of scope, so a
predicate over parquet column groups silently does nothing. That is a defect in
the C layer — the base should return `NotImplemented` — and it is worth
reporting upstream, but fixing it would not change this design: `core.expr`
evaluates predicates because R7 replicates Milvus's own `Plan.g4` semantics,
which the C layer does not carry. I/O is saved by statistics-based pruning
(R10), not by this entry point.

## Row-group pruning can only happen in C

There is no row-group parameter anywhere in the FFI. `loon_segment_reader_open`
takes segment path, version, schema, needed columns, config and properties —
nothing that says "read only these row groups". Reading parquet footers on the
JVM side and computing which groups to skip produces a result with nowhere to
go, so R10 is not a question of where to put the code: pruning has to happen on
the side that reads. It needs Milvus to write statistics files registered in the
manifest's `stats`, and milvus-storage's reader to prune with them. Section 5 of
docs/design/README.md tracks both.

Do not reach for `Manifest.stats` expecting min/max. It maps a stat name
(`"bloom_filter.100"`) to `{paths, metadata}` — paths to auxiliary files.
`ColumnGroupFile` carries `path`, `start_index`, `end_index` and a free-form
`properties`; `LoonChunkMetadata` carries `number_of_rows` and
`estimated_memsz`. Table row count and byte size (what R13 actually asks for)
come from those, and work today.

## Long jobs

Credentials renew themselves per process; nothing expires mid-job. The AWS path
re-assumes within 60 seconds of expiry, the Aliyun OIDC chain within 180 and
re-reads the projected token file each time, so kubelet rotating it is picked
up. Do not build a path that assumes once in the driver and ships the resulting
access key, secret and session token to the executors — those do expire, and
that is the one shape this design rejects.

## Verifying a change

Compilation proves nothing here. A credential or filesystem change is done when
it has run against the real backend. Section 8 of the design lists the three
checks that have not been run yet — if your change depends on one of them, say
so rather than calling it verified.
