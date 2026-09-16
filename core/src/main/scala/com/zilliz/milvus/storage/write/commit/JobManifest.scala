package com.zilliz.milvus.storage.write.commit

import java.util.Locale

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** The collection that owns one staging job. */
final case class JobOwner(
    @JsonProperty("database") database: String,
    @JsonProperty("collection") collection: String
) {
  require(
    Option(database).exists(_.trim.nonEmpty),
    "job owner database must not be empty"
  )
  require(
    Option(collection).exists(_.trim.nonEmpty),
    "job owner collection must not be empty"
  )
}

sealed trait JobWriteMode { def name: String }

object JobWriteMode {
  case object Append extends JobWriteMode { override val name = "append" }
  case object Backfill extends JobWriteMode { override val name = "backfill" }

  def fromName(name: String): Option[JobWriteMode] =
    Option(name).map(_.trim.toLowerCase(Locale.ROOT)) match {
      case Some(value) if value == Append.name   => Some(Append)
      case Some(value) if value == Backfill.name => Some(Backfill)
      case _                                     => None
    }
}

/** Immutable ownership written before executor tasks start. */
final case class JobOwnerManifest(
    @JsonProperty("format_version") formatVersion: Int,
    @JsonProperty("job_id") jobId: String,
    @JsonProperty("created_at") createdAtMillis: Long,
    @JsonProperty("owner") owner: JobOwner,
    @JsonProperty("write_mode") writeMode: String
) {
  def toJson: String = JobManifest.mapper.writeValueAsString(this)
}

object JobOwnerManifest {
  val CurrentVersion = 1

  def fromJson(json: String): Either[Throwable, JobOwnerManifest] =
    JobManifest.read(json, classOf[JobOwnerManifest])
}

/** The driver's liveness record for a staging job. */
final case class JobHeartbeat(
    @JsonProperty("format_version") formatVersion: Int,
    @JsonProperty("job_id") jobId: String,
    @JsonProperty("updated_at") updatedAtMillis: Long
) {
  def toJson: String = JobManifest.mapper.writeValueAsString(this)
}

object JobHeartbeat {
  val CurrentVersion = 1

  def fromJson(json: String): Either[Throwable, JobHeartbeat] =
    JobManifest.read(json, classOf[JobHeartbeat])
}

final case class JobDescriptor(owner: JobOwner, writeMode: JobWriteMode)

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
    @JsonProperty("segments") segments: Seq[CommittedSegment],
    @JsonProperty("format_version")
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    formatVersion: Option[Int] = None,
    @JsonProperty("owner") @JsonInclude(JsonInclude.Include.NON_ABSENT)
    owner: Option[JobOwner] = None,
    @JsonProperty("write_mode")
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    writeMode: Option[String] = None
) {
  def rowCount: Long = segments.map(_.rowCount).sum

  def toJson: String = JobManifest.mapper.writeValueAsString(this)
}

object JobManifest {
  val CurrentVersion = 2

  private[commit] val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.enable(SerializationFeature.INDENT_OUTPUT)
    m
  }

  def fromJson(json: String): Either[Throwable, JobManifest] =
    read(json, classOf[JobManifest])

  private[commit] def read[A](
      json: String,
      clazz: Class[A]
  ): Either[Throwable, A] =
    try Right(mapper.readValue(json, clazz))
    catch { case e: Exception => Left(e) }
}
