package com.zilliz.milvus.storage.write.commit

/** What a job manifest hands to Milvus when its segments are registered.
  *
  * Registration takes two forms and Milvus offers one of them today: a segment
  * the job wrote into (backfill) is registered by its id and new manifest
  * version through `BatchUpdateManifest`; a segment the job created has no id
  * and needs `RegisterSegments`, which Milvus does not have yet
  * (docs/design/README.md section 5). A job holding such a segment is refused
  * here rather than half registered.
  */
object Registration {

  final case class Item(segmentId: Long, manifestVersion: Long)

  /** The `BatchUpdateManifest` items of `manifest`, or the reason it cannot be
    * registered that way.
    */
  def backfillItems(manifest: JobManifest): Either[String, Seq[Item]] = {
    val unregistrable = manifest.segments.filter(_.segmentId.isEmpty)
    if (unregistrable.nonEmpty) {
      Left(
        s"job ${manifest.jobId} wrote ${unregistrable.size} new segment(s) (" +
          unregistrable.map(_.basePath).mkString(", ") +
          "); registering a new segment needs Milvus's RegisterSegments, which does not exist yet"
      )
    } else if (manifest.segments.isEmpty) {
      Left(s"job ${manifest.jobId} wrote no segment")
    } else {
      Right(
        manifest.segments.map(s => Item(s.segmentId.get, s.manifestVersion))
      )
    }
  }
}
