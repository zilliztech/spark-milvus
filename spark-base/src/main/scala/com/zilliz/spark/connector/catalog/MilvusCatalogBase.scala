package com.zilliz.spark.connector.catalog

import java.{util => ju}
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.sql.catalyst.analysis.{
  NoSuchNamespaceException,
  NoSuchTableException
}
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  NamespaceChange,
  SupportsNamespaces,
  Table,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.{
  CollectionNotFoundException,
  DatabaseNotFoundException
}
import com.zilliz.milvus.client.api.{MilvusClient, MilvusConnectionParams}
import com.zilliz.spark.connector.options.{
  MilvusOption,
  ReadMode,
  SnapshotReference
}
import com.zilliz.spark.connector.table.MilvusTables

/** Shared read-only TableCatalog behavior. Spark-line sources implement only
  * the createTable signature preferred by that line and reject it explicitly.
  */
private[catalog] abstract class MilvusCatalogBase(
    tableLoader: (CaseInsensitiveStringMap, SnapshotReference) => Table =
      MilvusCatalogBase.defaultTableLoader,
    discovery: MilvusCatalogDiscovery = MilvusCatalogDiscovery.default
) extends TableCatalog
    with SupportsNamespaces {

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

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val database = singleNamespace(namespace)
    try {
      discovery
        .listCollections(discoveryOptions(database), database)
        .map(collection => Identifier.of(Array(database), collection))
        .toArray
    } catch {
      case error: Exception if MilvusCatalogBase.isMissingDatabase(error) =>
        throw new NoSuchNamespaceException(Array(database))
    }
  }

  override def listNamespaces(): Array[Array[String]] =
    listNamespaces(Array.empty[String])

  override def listNamespaces(
      namespace: Array[String]
  ): Array[Array[String]] = {
    val path = namespacePath(namespace)
    path.length match {
      case 0 =>
        discovery
          .listDatabases(discoveryOptions())
          .map(database => Array(database))
          .toArray
      case 1 if validName(path.head) =>
        requireDatabase(path.head)
        Array.empty[Array[String]]
      case _ => throw noSuchNamespace(path)
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    val path = namespacePath(namespace)
    if (path.length != 1 || !validName(path.head)) false
    else discovery.listDatabases(discoveryOptions()).contains(path.head)
  }

  override def loadNamespaceMetadata(
      namespace: Array[String]
  ): ju.Map[String, String] = {
    val database = singleNamespace(namespace)
    requireDatabase(database)
    ju.Collections.emptyMap[String, String]()
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: ju.Map[String, String]
  ): Unit = unsupported("creating namespaces")

  override def alterNamespace(
      namespace: Array[String],
      changes: NamespaceChange*
  ): Unit = unsupported("altering namespaces")

  override def dropNamespace(
      namespace: Array[String],
      cascade: Boolean
  ): Boolean = unsupported("dropping namespaces")

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

  private def requireDatabase(database: String): Unit = {
    if (!discovery.listDatabases(discoveryOptions()).contains(database)) {
      throw new NoSuchNamespaceException(Array(database))
    }
  }

  private def singleNamespace(namespace: Array[String]): String = {
    val path = namespacePath(namespace)
    if (path.length != 1 || !validName(path.head)) {
      throw noSuchNamespace(path)
    }
    path.head
  }

  private def namespacePath(namespace: Array[String]): Array[String] =
    Option(namespace).getOrElse(Array.empty[String])

  private def validName(name: String): Boolean =
    name != null && name.trim.nonEmpty

  private def noSuchNamespace(
      namespace: Array[String]
  ): NoSuchNamespaceException =
    new NoSuchNamespaceException(
      namespace.map(name => Option(name).getOrElse(""))
    )

  private def discoveryOptions(
      database: String = ""
  ): CaseInsensitiveStringMap = {
    val options = new ju.HashMap[String, String]()
    initializedOptions().asScala.foreach { case (key, value) =>
      if (!MilvusCatalogBase.TableNameOptions.exists(_.equalsIgnoreCase(key))) {
        options.put(key, value)
      }
    }
    options.put(MilvusOption.MilvusDatabaseName, database)
    val discoveryOptions = new CaseInsensitiveStringMap(options)
    MilvusOption.readMode(discoveryOptions) match {
      case ReadMode.Client => discoveryOptions
      case _ =>
        throw new IllegalArgumentException(
          "MilvusCatalog discovery requires client mode; snapshot and backup options are not supported"
        )
    }
  }

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

  private def isMissing(error: Throwable): Boolean =
    hasCause(error, classOf[CollectionNotFoundException])

  private def isMissingDatabase(error: Throwable): Boolean =
    hasCause(error, classOf[DatabaseNotFoundException])

  private def hasCause(
      error: Throwable,
      expected: Class[_ <: Throwable]
  ): Boolean = {
    val visited = ju.Collections.newSetFromMap(
      new ju.IdentityHashMap[Throwable, java.lang.Boolean]()
    )
    var current = error
    while (current != null && visited.add(current)) {
      if (expected.isInstance(current)) return true
      current = current.getCause
    }
    false
  }
}

private[catalog] trait MilvusCatalogDiscovery {
  def listDatabases(options: CaseInsensitiveStringMap): Seq[String]

  def listCollections(
      options: CaseInsensitiveStringMap,
      database: String
  ): Seq[String]
}

private[catalog] object MilvusCatalogDiscovery {
  val default: MilvusCatalogDiscovery = new MilvusCatalogDiscovery {
    override def listDatabases(
        options: CaseInsensitiveStringMap
    ): Seq[String] =
      withClient(options)(_.listDatabases())

    override def listCollections(
        options: CaseInsensitiveStringMap,
        database: String
    ): Seq[String] =
      withClient(options)(_.showCollections(database))
  }

  private def withClient[A](
      options: CaseInsensitiveStringMap
  )(operation: MilvusClient => Try[A]): A = {
    val client = MilvusClient(connectionParams(options))
    var failure: Throwable = null
    try operation(client).get
    catch {
      case error: Throwable =>
        failure = error
        throw error
    } finally {
      try client.close()
      catch {
        case closeError: Throwable if failure != null =>
          failure.addSuppressed(closeError)
        case closeError: Throwable => throw closeError
      }
    }
  }

  private def connectionParams(
      options: CaseInsensitiveStringMap
  ): MilvusConnectionParams = {
    val uri = value(options, MilvusOption.MilvusUri).trim
    if (uri.isEmpty) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusUri}' is required for catalog discovery"
      )
    }
    MilvusConnectionParams(
      uri = uri,
      token = value(options, MilvusOption.MilvusToken),
      databaseName = value(options, MilvusOption.MilvusDatabaseName),
      serverPemPath = value(options, MilvusOption.MilvusServerPemPath),
      clientPemPath = value(options, MilvusOption.MilvusClientPemPath),
      clientKeyPath = value(options, MilvusOption.MilvusClientKeyPath),
      caPemPath = value(options, MilvusOption.MilvusCaPemPath)
    )
  }

  private def value(
      options: CaseInsensitiveStringMap,
      key: String
  ): String = Option(options.get(key)).getOrElse("")
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
