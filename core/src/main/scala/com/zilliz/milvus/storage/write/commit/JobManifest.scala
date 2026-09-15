package com.zilliz.milvus.storage.write.commit

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** One segment a write job produced, as its task reported it.
  *
  * `basePath` is the segment directory as a key relative to the bucket;
  * `manifestVersion` is the version `ManifestTransaction` committed for it.
  * `segmentId` is Milvus's id of the segment when the job wrote into an
  * existing one (backfill), and absent for a new segment, which has no id until
  * Milvus registers it.
  */
final case class CommittedSegment(
    @JsonProperty("partition_id") partitionId: Int,
    @JsonProperty("base_path") basePath: String,
    @JsonProperty("manifest_version") manifestVersion: Long,
    @JsonProperty("row_count") rowCount: Long,
    // Jackson reads a small number into an Option as an Integer unless told
    // the content type, and unboxing that as Long throws.
    @JsonProperty("segment_id") @JsonInclude(JsonInclude.Include.NON_ABSENT)
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    segmentId: Option[Long] = None
)

/** What one write job produced: every segment with its manifest version and row
  * count. Written to `staging/{job}/manifest.json` by [[Committer]] and read
  * back by the registration procedure (capability A4), which hands the segments
  * to Milvus.
  */
final case class JobManifest(
    @JsonProperty("job_id") jobId: String,
    @JsonProperty("created_at") createdAtMillis: Long,
    @JsonProperty("segments") segments: Seq[CommittedSegment]
) {
  def rowCount: Long = segments.map(_.rowCount).sum

  def toJson: String = JobManifest.mapper.writeValueAsString(this)
}

object JobManifest {
  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.enable(SerializationFeature.INDENT_OUTPUT)
    m
  }

  def fromJson(json: String): Either[Throwable, JobManifest] =
    try Right(mapper.readValue(json, classOf[JobManifest]))
    catch { case e: Exception => Left(e) }
}
