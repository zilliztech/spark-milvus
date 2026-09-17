package com.zilliz.milvus.storage.stats

import scala.util.control.NonFatal

import com.zilliz.milvus.storage.expr.{
  And,
  Comparison,
  ComparisonOperator,
  FieldRef,
  In,
  Literal,
  Or,
  PredicateExpr
}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.V3ManifestReader
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.{
  Segment,
  SegmentLayout,
  SegmentStatistics,
  Snapshot
}
import io.milvus.grpc.schema.{DataType, FieldSchema}

sealed trait PrimaryKeyValue extends Product with Serializable

object PrimaryKeyValue {
  final case class LongValue(value: Long) extends PrimaryKeyValue
  final case class StringValue(value: String) extends PrimaryKeyValue
}

/** A finite set of primary-key values that can still satisfy a predicate. */
final case class PrimaryKeyFilter(
    fieldId: Long,
    dataType: DataType,
    values: Set[PrimaryKeyValue]
) {
  def intersect(other: PrimaryKeyFilter): PrimaryKeyFilter = {
    require(
      fieldId == other.fieldId && dataType == other.dataType,
      s"cannot intersect primary-key filters for ($fieldId, $dataType) and " +
        s"(${other.fieldId}, ${other.dataType})"
    )
    copy(values = values intersect other.values)
  }
}

object PrimaryKeyFilter {
  import PrimaryKeyValue._

  /** Extracts only constraints that are safe for segment pruning. An AND can
    * retain a constraint from one side; an OR needs constraints on both sides.
    */
  def fromPredicate(
      predicate: PredicateExpr,
      primaryKey: FieldSchema
  ): Option[PrimaryKeyFilter] = {
    val dataType = primaryKey.dataType
    if (dataType != DataType.Int64 && dataType != DataType.VarChar) return None
    values(predicate, primaryKey.fieldID, dataType).map(found =>
      PrimaryKeyFilter(primaryKey.fieldID, dataType, found)
    )
  }

  private def values(
      predicate: PredicateExpr,
      fieldId: Long,
      dataType: DataType
  ): Option[Set[PrimaryKeyValue]] = predicate match {
    case Comparison(field, operator, literal)
        if matches(field, fieldId, dataType) &&
          (operator == ComparisonOperator.EqualTo ||
            operator == ComparisonOperator.EqualNullSafe) =>
      literalValues(Vector(literal), dataType)

    case In(field, literals) if matches(field, fieldId, dataType) =>
      literalValues(literals, dataType)

    case And(left, right) =>
      (
        values(left, fieldId, dataType),
        values(right, fieldId, dataType)
      ) match {
        case (Some(l), Some(r)) => Some(l intersect r)
        case (Some(l), None)    => Some(l)
        case (None, Some(r))    => Some(r)
        case _                  => None
      }

    case Or(left, right) =>
      for {
        l <- values(left, fieldId, dataType)
        r <- values(right, fieldId, dataType)
      } yield l union r

    case _ => None
  }

  private def matches(
      field: FieldRef,
      fieldId: Long,
      dataType: DataType
  ): Boolean = field.fieldId == fieldId && field.dataType == dataType

  private def literalValues(
      literals: Vector[Literal],
      dataType: DataType
  ): Option[Set[PrimaryKeyValue]] = {
    val out = Set.newBuilder[PrimaryKeyValue]
    literals.foreach {
      case Literal.NullValue => // A primary key cannot be null.
      case Literal.IntegerValue(value) if dataType == DataType.Int64 =>
        out += LongValue(value)
      case Literal.StringValue(value) if dataType == DataType.VarChar =>
        out += StringValue(value)
      case _ => return None
    }
    Some(out.result())
  }
}

/** Cached segment-level primary-key Bloom inputs for one fixed snapshot.
  *
  * Construction reads every relevant statistics file once. Calls to [[prune]]
  * only evaluate the cached filters, so repeated Spark runtime filtering never
  * reopens object storage.
  */
final class PrimaryKeyBloomPruner private (
    snapshot: Snapshot,
    fieldId: Long,
    dataType: DataType,
    inputs: Map[Long, PrimaryKeyBloomPruner.SegmentInput]
) {
  import PrimaryKeyBloomPruner._
  import PrimaryKeyValue._

  def prune(filter: PrimaryKeyFilter): Snapshot = {
    require(
      filter.fieldId == fieldId && filter.dataType == dataType,
      s"the cached statistics are for ($fieldId, $dataType), not " +
        s"(${filter.fieldId}, ${filter.dataType})"
    )
    if (filter.values.isEmpty) return snapshot.retainDataSegments(Set.empty)

    val retained = snapshot.dataSegments.iterator
      .filter { segment =>
        inputs.get(segment.id) match {
          case Some(Usable(stats)) =>
            filter.values.exists(value =>
              stats.exists(stat => mightContain(stat, value))
            )
          case _ => true
        }
      }
      .map(_.id)
      .toSet
    snapshot.retainDataSegments(retained)
  }

  private def mightContain(
      stats: PrimaryKeyStats,
      value: PrimaryKeyValue
  ): Boolean = value match {
    case LongValue(v)   => stats.mightContainLong(v)
    case StringValue(v) => stats.mightContainString(v)
  }
}

object PrimaryKeyBloomPruner extends com.zilliz.milvus.storage.Logging {
  private sealed trait SegmentInput
  private final case class Usable(stats: Vector[PrimaryKeyStats])
      extends SegmentInput
  private case object Unavailable extends SegmentInput

  /** A conservative cache used when the statistics store itself cannot be
    * opened. Every segment is retained for every non-empty candidate set.
    */
  def unavailable(
      snapshot: Snapshot,
      primaryKey: FieldSchema
  ): PrimaryKeyBloomPruner =
    new PrimaryKeyBloomPruner(
      snapshot,
      primaryKey.fieldID,
      primaryKey.dataType,
      snapshot.dataSegments.iterator.map(_.id -> Unavailable).toMap
    )

  def load(
      snapshot: Snapshot,
      primaryKey: FieldSchema,
      v3ReadVersions: Map[Long, Long],
      bucket: String,
      endpoint: String,
      store: ObjectStore
  ): PrimaryKeyBloomPruner = {
    require(
      store != null,
      "an ObjectStore is required to load Bloom statistics"
    )
    val fieldId = primaryKey.fieldID
    val dataType = primaryKey.dataType
    require(
      dataType == DataType.Int64 || dataType == DataType.VarChar,
      s"primary-key Bloom pruning does not support $dataType"
    )
    val loaded = snapshot.dataSegments.iterator.map { segment =>
      val input =
        try
          loadSegment(
            segment,
            fieldId,
            dataType,
            v3ReadVersions,
            bucket,
            endpoint,
            store
          )
        catch {
          case NonFatal(e) =>
            logWarning(
              s"Retaining segment ${segment.id}: its primary-key Bloom statistics " +
                s"cannot be used: ${e.getMessage}"
            )
            Unavailable
        }
      segment.id -> input
    }.toMap
    new PrimaryKeyBloomPruner(snapshot, fieldId, dataType, loaded)
  }

  private def loadSegment(
      segment: Segment,
      fieldId: Long,
      dataType: DataType,
      v3ReadVersions: Map[Long, Long],
      bucket: String,
      endpoint: String,
      store: ObjectStore
  ): SegmentInput = {
    val paths = segment.statistics match {
      case SegmentStatistics.Listed(filesByField) =>
        filesByField.getOrElse(fieldId, Seq.empty)
      case SegmentStatistics.InManifest =>
        val (basePath, listedVersion) = segment.layout match {
          case SegmentLayout.Manifest(path, version) => (path, version)
          case _ =>
            throw new IllegalStateException(
              "manifest statistics require a manifest segment layout"
            )
        }
        val version = v3ReadVersions.getOrElse(segment.id, listedVersion)
        if (version <= 0L) {
          throw new IllegalStateException(
            s"manifest version is not pinned: $version"
          )
        }
        V3ManifestReader
          .loadStatistics(basePath, version, bucket, store)
          .fold(error => throw error, identity)
          .get(s"bloom_filter.$fieldId")
          .map(_.paths)
          .getOrElse(Seq.empty)
      case SegmentStatistics.Unknown => Seq.empty
    }

    val selected = relevantPaths(paths)
    if (selected.isEmpty) return Unavailable
    val stats = selected.iterator.flatMap { path =>
      val located = StoragePath.parseMilvus(path, bucket, endpoint)
      if (bucket.nonEmpty && located.hasBucket && located.bucket != bucket) {
        throw new IllegalArgumentException(
          s"statistics path is in bucket '${located.bucket}' but expected '$bucket': $path"
        )
      }
      PrimaryKeyStats.fromBytes(store.readAll(located.key))
    }.toVector
    if (stats.isEmpty) return Unavailable
    stats.foreach { stat =>
      if (stat.fieldId != fieldId || stat.pkType != dataType) {
        throw new IllegalArgumentException(
          s"statistics identify (${stat.fieldId}, ${stat.pkType}), expected " +
            s"($fieldId, $dataType)"
        )
      }
    }
    Usable(stats)
  }

  /** Milvus's StatsResolver prefers the compound file named `1`; without it,
    * every listed file represents part of the segment and must be checked.
    */
  private[stats] def relevantPaths(paths: Seq[String]): Seq[String] = {
    val distinct = paths.distinct
    val compound = distinct.filter(path => basename(path) == "1")
    val selected = if (compound.nonEmpty) compound else distinct
    if (selected.exists(path => path == null || path.trim.isEmpty)) {
      throw new IllegalArgumentException("statistics path must not be blank")
    }
    selected
  }

  private def basename(path: String): String =
    Option(path)
      .map(_.stripSuffix("/"))
      .filter(_.nonEmpty)
      .map(value => value.substring(value.lastIndexOf('/') + 1))
      .getOrElse("")
}
