package com.zilliz.milvus.storage.manifest

import org.apache.avro.Schema

/** The full Milvus segment record schemas shared by readers and offline
  * writers. Readers choose their projection; writers choose their output
  * version. Neither policy belongs to the file-format registry.
  */
object SnapshotSegmentSchema {

  private val resources: Map[Int, String] = Map(
    1 -> "/milvus-segment-manifest-v1.avsc",
    2 -> "/milvus-segment-manifest-v2.avsc",
    3 -> "/milvus-segment-manifest-v3.avsc",
    4 -> "/milvus-segment-manifest-v4.avsc",
    5 -> "/milvus-segment-manifest-v5.avsc"
  )

  val supportedVersions: Seq[Int] = resources.keys.toSeq.sorted

  /** Returns an independently owned full schema. Avro schemas are mutable, so a
    * caller must not be able to change another reader or writer's schema.
    */
  def schemaFor(version: Int): Schema = {
    val resource = resources.getOrElse(
      version,
      throw new IllegalArgumentException(
        s"Unsupported Milvus manifest schema version $version"
      )
    )
    val in = Option(getClass.getResourceAsStream(resource)).getOrElse {
      throw new IllegalStateException(
        s"bundled AVSC resource $resource not found on classpath"
      )
    }
    try new Schema.Parser().parse(in)
    finally in.close()
  }
}
