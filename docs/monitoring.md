# Operational monitoring

The connector reports read progress and column replacement milestones in Spark
SQL metrics. An optional Spark plugin also aggregates executor observations on
the driver for Spark's metrics sinks. Existing JNI and Arrow diagnostics remain
available for performance analysis.

Use Spark's job, stage, executor, CPU, memory and GC metrics alongside these
connector metrics. The connector does not infer object-storage request bytes,
latency or retries from Arrow buffers or JNI timing.

## Read metrics

| SQL metric | Meaning |
| --- | --- |
| `milvus.read.segments.planned` | Segments in the current scan plan, after pruning. Replanning replaces the value. |
| `milvus.read.segments.completed` | Partitions that finished reading successfully, including early termination by a limit or consumer. Exhaustion is reported separately. |
| `milvus.read.rows.decoded` | Physical rows in batches returned by the native reader. |
| `milvus.read.rows.emitted` | Rows delivered at the outer connector reader, after connector deletes, predicates and local limits. This is not the final SQL result count. |
| `milvus.read.rows.deleted` | Rows removed by delete records. |
| `milvus.read.rows.filtered` | Remaining rows removed by connector predicates. Deleted rows are excluded from this count. |
| `milvus.read.ends.exhausted` | Readers that reached the end of a segment. |
| `milvus.read.ends.limit` | Readers stopped by a connector limit. |
| `milvus.read.ends.consumer_stop` | Readers closed early by their Spark consumer. |
| `milvus.read.ends.cancelled` | Readers stopped after task cancellation. |
| `milvus.read.ends.failed` | Readers that failed. |
| `milvus.read.failure.phase` | First failing boundary per reported reader: open, delete loading, read, filter, conversion or close. |
| `milvus.read.cleanup.failures` | Distinct observed resource-cleanup failures. |

A successful `LIMIT` query can leave most of the snapshot unread. Neither
`segments.completed` nor query success means every planned physical row was
visited. Repeated metric sampling and repeated reader closure do not add rows or
terminal events. A local limit can shorten a batch after the reader has counted
its deleted and filtered rows. Under early termination, those counts and the
emitted rows can therefore add up to fewer than the decoded rows.

## Write metrics

| SQL metric | Meaning |
| --- | --- |
| `milvus.write.rows.expected` | Stored rows that must be replaced, including deleted rows. |
| `milvus.write.segments.expected` | Nonempty segments that must be replaced. |
| `milvus.write.rows.accepted` | Input rows accepted by writer attempts. These rows may still be buffered. |
| `milvus.write.rows.files_ready` | Rows in completed files from Spark-adopted commit messages, after the driver validates complete coverage. |
| `milvus.write.files.ready` | Files in those validated messages. |
| `milvus.write.rows.manifest_committed` | Rows in segments whose new manifest revision was confirmed. |
| `milvus.write.segments.manifest_committed` | Segment manifest commits acknowledged by storage. |
| `milvus.write.segments.manifest_unknown` | Segment commit outcomes that could not be confirmed. |
| `milvus.write.segments.not_committed` | Segment manifests confirmed not committed, or not attempted. |
| `milvus.write.receipt.published` | `1` only after receipt publication is confirmed. |

Driver phase, first failure phase/category, last progress timestamp, task
failures and cleanup failures provide context for these milestones. A failure
counter counts observed failures, not the number of invalid input rows.

File creation, manifest commit and receipt publication are separate milestones.
A write can fail after some manifests commit, and an ambiguous storage response
can leave an outcome unknown. The metrics preserve those states. Receipt
publication confirms the connector's storage result; the caller still owns
submission to Milvus and online visibility. A later cleanup failure does not
undo a confirmed receipt.

Spark 4 exposes task and driver write metrics in the SQL UI. Spark 3.5 exposes
task write metrics but has no driver write metric callback; use the plugin below
for driver milestones on that version.

## Driver collection and export

Enable the plugin when starting the application:

```text
--conf spark.plugins=com.zilliz.spark.connector.metrics.MilvusMetricsPlugin
```

If other Spark plugins are configured, add this class to their comma-separated
list. Make the connector JAR available to the driver and every executor before
Spark starts.

The plugin sends cumulative task observations through Spark's plugin RPC
channel. The driver combines these with Spark task completion events and its
own write milestones. Prometheus scrapes the driver's metrics endpoint; it does
not need to discover individual executors for these connector metrics.

Configure Spark's driver Prometheus sink, for example in `metrics.properties`:

```properties
driver.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
driver.sink.prometheusServlet.path=/metrics/prometheus
```

Pass that file through `spark.metrics.conf` using your cluster's file
distribution mechanism. Scrape `/metrics/prometheus` on the actual driver UI
address and port. This is the driver metrics sink, distinct from Spark's
`/metrics/executors/prometheus` endpoint. Apply the deployment's usual network
access controls to the driver UI.

Spark's servlet transforms Dropwizard registry names into Prometheus sample
names: it replaces punctuation with underscores, adds a `metrics_` prefix and
includes the Spark application/driver/plugin namespace. Numeric gauges have
`Value`/`Number` suffixes and counters have a `Count` suffix. Inspect the actual
endpoint before writing recording rules; SQL metric names are not literal
Prometheus sample names. The plugin does not add an HTTP server or external
metrics dependency.

The plugin registers the following bounded families in the driver registry.
The servlet adds its namespace and suffixes to these names:

| Registry name or family | Type and interpretation |
| --- | --- |
| `milvus_read_rows_{decoded,emitted,deleted,filtered}_total` | Counters of observed work across all attempts. |
| `milvus_read_segments_completed_total` | Counter of successful, deduplicated logical input segments. |
| `milvus_write_rows_accepted_total` | Counter of rows accepted across writer attempts. |
| `milvus_write_task_{failures,validation_failures,cleanup_failures}_total` | Observed writer attempt failure counters. Validation failures count attempts, not bad rows. |
| `milvus_write_rows_files_ready_total`, `milvus_write_files_ready_total` | Counters of driver-validated ready rows and files. |
| `milvus_write_{rows,segments}_manifest_committed_total` | Counters of confirmed commits. |
| `milvus_write_segments_manifest_unknown_total` | Counter of observed unknown segment outcomes; later resolution does not subtract history. |
| `milvus_write_segments_not_committed_total` | Counter of confirmed uncommitted or unattempted segment outcomes. |
| `milvus_write_receipt_published_total` | Counter of confirmed receipts. |
| `milvus_{read,write}_operations_{success,failed,cancelled,unknown}_total` | Operation outcome counters. Write success means a confirmed receipt, even if subsequent cleanup fails. |
| `milvus_{read,write}_ends_<reason>_total` | Observed attempt termination counters, independent of logical completion. |
| `milvus_{read,write}_failures_<phase>_<reason>_total` | Failure event counters with fixed phase/reason vocabularies. |
| `milvus_cleanup_failures_total` | Observed cleanup failures. |
| `milvus_{read,write}_<phase>_active` | Number of unfinished operations in the phase. |
| `milvus_{read,write}_<phase>_oldest_progress_timestamp_seconds` | Earliest last-progress time among those operations, or zero when none are active. |
| `milvus_telemetry_dropped_total`, `milvus_telemetry_incomplete_operations` | Tracking-limit drops and current operations with known incomplete telemetry. |

Driver state gauges also expose the SQL plan and milestone values with dots
replaced by underscores, summed over tracked operations. They are retired when
the SQL execution ends. Use the lifetime milestone counters for rates and
alerts that must observe short operations between scrapes. Counters reset when
the driver process restarts. Milestone counters add each operation's highest
reported value, so replaying an outcome callback does not add it again.

The executor reporter records `milvus_telemetry_send_failures_total` and logs a
rate-limited warning if it cannot contact the driver. Those transport failures
cannot be reliably reported through the channel that failed; collect executor
logs or the executor's own metrics sink to observe them.

| Spark setting | Default | Purpose |
| --- | --- | --- |
| `spark.milvus.metrics.reportIntervalMillis` | `10000` | Periodic cumulative task reporting interval. A final acknowledged report is attempted at reader/writer completion. |
| `spark.milvus.metrics.maxOperations` | `1024` | Maximum simultaneously tracked driver operations. |
| `spark.milvus.metrics.maxTrackedTasks` | `100000` | Maximum task observations/results per driver operation and observations per executor reporter. |

All limits must be positive. Reaching a limit increments the drop counter;
retained observations continue to be reported. The driver marks affected
tracked operations as incomplete instead of inventing missing progress.

## Retries and alert interpretation

Task work and logical completion have different meanings. Work counters include
observed failed or speculative attempts. Logical completed partitions use
Spark-adopted successful attempts; validated files and manifest outcomes come
from the driver. Task snapshots carry attempt identity and sequence numbers so
duplicate or reordered reports do not inflate counters.

Progress timestamps advance when work advances, not when a heartbeat arrives.
Operations retain independent progress state so a busy concurrent scan cannot
hide a stalled write. The exported metric families use bounded phases and
failure reasons, without snapshot paths, segment IDs, file names or exception
messages as labels.

Executor loss can discard observations that were never sent. Missing telemetry
is not interpreted as completed segments. Work counts are a lower bound after
abrupt loss, and an entirely missing observation cannot always be detected.
SQL success alone does not prove complete telemetry. These metrics are operational signals;
the published write receipt remains the durable result record. Driver metrics
are available for the lifetime of the Spark application; retain scrape data
externally if it must outlive the driver.

Operation tracking starts when Spark prepares the physical reader factory or
batch writer. Errors during earlier schema inference or logical planning remain
visible through Spark errors and logs, without a connector operation counter.
Task cancellation is reported through the read termination reason; SQL-level
failures, including cancellations, follow Spark's execution outcome.

Useful alerts distinguish failures, unknown commit outcomes, cleanup failures
and active operations with no progress. Choose stall thresholds for the
workload: one large native batch or manifest operation can legitimately take
longer than the scrape interval. Treat `manifest_unknown > 0` as an outcome to
investigate, not a safe instruction to replay a write.

## Existing diagnostics

The scan still exposes `milvus.jni.calls`, `milvus.jni.nanos`,
`milvus.arrow.batches`, `milvus.arrow.bytes`, `milvus.copies`,
`milvus.copied.bytes` and `milvus.arrow.allocated.max`. Arrow bytes describe
buffers handed to the JVM. Allocator usage is the maximum over tasks.
