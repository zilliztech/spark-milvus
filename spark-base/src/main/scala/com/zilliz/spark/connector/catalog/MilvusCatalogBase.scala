package com.zilliz.spark.connector.catalog

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  Table,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.CollectionNotFoundException
import com.zilliz.spark.connector.options.{MilvusOption, SnapshotReference}
import com.zilliz.spark.connector.table.MilvusTables

/** Shared read-only TableCatalog behavior. Spark-line sources implement only
  * the createTable signature preferred by that line and reject it explicitly.
  */
private[catalog] abstract class MilvusCatalogBase(
    tableLoader: (CaseInsensitiveStringMap, SnapshotReference) => Table =
      MilvusCatalogBase.defaultTableLoader
) extends TableCatalog {

  @volatile private var catalogName: String = _
  @volatile private var catalogOptions: ju.Map[String, String] = _

  override def initialize(
      name: String,
      options: CaseInsensitiveStringMap
  ): Unit = {
    if (name == null || name.trim.isEmpty) {
      throw new IllegalArgumentException("Catalog name must not be empty")
    }
    val copied = new ju.HashMap[String, String]()
    copied.putAll(options.asCaseSensitiveMap())
    catalogOptions = ju.Collections.unmodifiableMap(copied)
    catalogName = name
  }

  override def name(): String = initializedName()

  override def loadTable(identifier: Identifier): Table =
    load(identifier, SnapshotReference.Latest)

  override def loadTable(identifier: Identifier, version: String): Table = {
    if (version == null || version.trim.isEmpty) {
      throw new IllegalArgumentException("Snapshot version must not be empty")
    }
    load(identifier, SnapshotReference.Named(version))
  }

  override def loadTable(identifier: Identifier, timestamp: Long): Table =
    load(
      identifier,
      SnapshotReference.AtOrBefore(
        MilvusHybridTimestamp.upperBound(timestamp)
      )
    )

  override def listTables(namespace: Array[String]): Array[Identifier] =
    unsupported("listing tables")

  override def alterTable(
      identifier: Identifier,
      changes: TableChange*
  ): Table = unsupported("altering tables")

  override def dropTable(identifier: Identifier): Boolean =
    unsupported("dropping tables")

  override def renameTable(
      oldIdentifier: Identifier,
      newIdentifier: Identifier
  ): Unit = unsupported("renaming tables")

  protected final def unsupportedCreate(): Nothing =
    unsupported("creating tables")

  private def load(
      identifier: Identifier,
      snapshotReference: SnapshotReference
  ): Table = {
    val (database, collection) = tableName(identifier)
    val merged = new ju.HashMap[String, String]()
    initializedOptions().asScala.foreach { case (key, value) =>
      if (!MilvusCatalogBase.TableNameOptions.exists(_.equalsIgnoreCase(key))) {
        merged.put(key, value)
      }
    }
    merged.put(MilvusOption.MilvusDatabaseName, database)
    merged.put(MilvusOption.MilvusCollectionName, collection)
    try tableLoader(new CaseInsensitiveStringMap(merged), snapshotReference)
    catch {
      case error: Exception if MilvusCatalogBase.isMissing(error) =>
        throw new NoSuchTableException(identifier)
    }
  }

  private def tableName(identifier: Identifier): (String, String) = {
    if (identifier == null) {
      throw new IllegalArgumentException(
        s"Milvus table name must be ${initializedName()}.<database>.<collection>"
      )
    }
    val namespace = Option(identifier.namespace())
      .getOrElse(Array.empty[String])
      .toSeq
      .map(value => Option(value).getOrElse(""))
    val database = namespace.headOption.getOrElse("")
    val collection = Option(identifier.name()).getOrElse("")
    if (
      namespace.length != 1 || database.trim.isEmpty ||
      collection.trim.isEmpty
    ) {
      val actual =
        (Seq(initializedName()) ++ namespace ++ Seq(collection))
          .filter(_.nonEmpty)
          .mkString(".")
      throw new IllegalArgumentException(
        s"Milvus table name must be ${initializedName()}.<database>.<collection>, got '$actual'"
      )
    }
    database -> collection
  }

  private def initializedName(): String =
    Option(catalogName).getOrElse(
      throw new IllegalStateException("MilvusCatalog has not been initialized")
    )

  private def initializedOptions(): ju.Map[String, String] = {
    initializedName()
    catalogOptions
  }

  private def unsupported(operation: String): Nothing =
    throw new UnsupportedOperationException(
      s"MilvusCatalog is read-only and does not support $operation"
    )
}

private[catalog] object MilvusCatalogBase {
  private val TableNameOptions = Seq(
    MilvusOption.MilvusDatabaseName,
    MilvusOption.MilvusCollectionName
  )

  private val defaultTableLoader
      : (CaseInsensitiveStringMap, SnapshotReference) => Table =
    (options, reference) => MilvusTables.load(options, None, reference)

  private def isMissing(error: Throwable): Boolean = {
    val visited = ju.Collections.newSetFromMap(
      new ju.IdentityHashMap[Throwable, java.lang.Boolean]()
    )
    var current = error
    while (current != null && visited.add(current)) {
      current match {
        case _: CollectionNotFoundException => return true
        case _                              =>
      }
      current = current.getCause
    }
    false
  }
}

/** Converts Spark's Unix epoch microseconds to the greatest Milvus HybridTS in
  * the same physical millisecond. Saturating the two ends avoids shift overflow
  * while preserving as-of ordering for every Long Spark can pass.
  */
private[catalog] object MilvusHybridTimestamp {
  private val LogicalBits = 18
  private val LogicalMask = (1L << LogicalBits) - 1L
  private val MaxPhysicalMillis = Long.MaxValue >>> LogicalBits

  def upperBound(epochMicros: Long): Long = {
    val millis = Math.floorDiv(epochMicros, 1000L)
    if (millis < 0L) Long.MinValue
    else if (millis > MaxPhysicalMillis) Long.MaxValue
    else (millis << LogicalBits) | LogicalMask
  }
}
