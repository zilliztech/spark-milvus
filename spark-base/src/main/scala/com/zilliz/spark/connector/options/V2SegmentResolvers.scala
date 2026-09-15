package com.zilliz.spark.connector.options

import com.zilliz.milvus.storage.compat.v2.FooterV2SegmentResolver
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.snapshot.{Segment, V2SegmentResolver}

/** The compat implementation of [[V2SegmentResolver]], handed to
  * `SnapshotCatalog` by the Spark layer: core does not depend on compat
  * (capability K1), this package registers compat into core.
  */
object V2SegmentResolvers {

  /** Materializes Storage V2 packed segments from the snapshot's Avro plus each
    * parquet footer. `applyDeletes = false` skips the delta logs, which is what
    * backfill wants when it aligns new column groups to physical rows.
    */
  def footer(applyDeletes: Boolean): V2SegmentResolver = new V2SegmentResolver {
    def resolve(
        manifestPaths: Seq[String],
        bucket: String,
        store: ObjectStore,
        manifestSchemaVersion: Int
    ): Either[Throwable, Seq[Segment]] =
      FooterV2SegmentResolver.loadV2Segments(
        manifestPaths,
        bucket,
        store,
        manifestSchemaVersion = manifestSchemaVersion,
        applyDeletes = applyDeletes
      )
  }
}
