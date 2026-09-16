package com.zilliz.spark.connector.catalog

import java.{util => ju}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{
  Column,
  Identifier,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.client.CollectionNotFoundException
import com.zilliz.milvus.storage.snapshot.SnapshotNotFoundException
import com.zilliz.spark.connector.options.{MilvusOption, SnapshotReference}

object MilvusCatalogTest {
  final case class LoadCall(
      options: Map[String, String],
      snapshotReference: SnapshotReference
  )
}

object RoutingMilvusCatalog {
  private val calls = ArrayBuffer.empty[SnapshotReference]

  private val table = new Table {
    override def name(): String = "routed"
    override def schema(): StructType = StructType(Nil)
    override def capabilities(): ju.Set[TableCapability] =
      ju.Collections.singleton(TableCapability.BATCH_READ)
  }

  def reset(): Unit = calls.synchronized(calls.clear())
  def references: Seq[SnapshotReference] = calls.synchronized(calls.toSeq)

  def load(
      options: CaseInsensitiveStringMap,
      reference: SnapshotReference
  ): Table = calls.synchronized {
    calls += reference
    table
  }
}

class RoutingMilvusCatalog
    extends MilvusCatalogBase(RoutingMilvusCatalog.load) {
  override def createTable(
      identifier: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = unsupportedCreate()
}

class MilvusCatalogTest extends AnyFunSuite {
  import MilvusCatalogTest.LoadCall

  private val loadedTable = new Table {
    override def name(): String = "loaded"
    override def schema(): StructType = StructType(Nil)
    override def capabilities(): ju.Set[TableCapability] =
      ju.Collections.singleton(TableCapability.BATCH_READ)
  }

  private def catalog(
      calls: ArrayBuffer[LoadCall]
  ): MilvusCatalogBase =
    new MilvusCatalogBase((options, reference) => {
      calls += LoadCall(options.asCaseSensitiveMap().asScala.toMap, reference)
      loadedTable
    }) {
      override def createTable(
          identifier: Identifier,
          columns: Array[Column],
          partitions: Array[Transform],
          properties: ju.Map[String, String]
      ): Table = unsupportedCreate()
    }

  test("identifier names override copied catalog options") {
    val calls = ArrayBuffer.empty[LoadCall]
    val catalogOptions = new ju.HashMap[String, String]()
    catalogOptions.put(MilvusOption.MilvusUri, "http://milvus:19530")
    catalogOptions.put("MILVUS.DATABASE.NAME", "configured-db")
    catalogOptions.put("Milvus.Collection.Name", "configured-collection")
    catalogOptions.put(MilvusOption.FsRootPath, "root")

    val milvus = catalog(calls)
    milvus.initialize("milvus", new CaseInsensitiveStringMap(catalogOptions))
    catalogOptions.put(MilvusOption.MilvusUri, "changed-after-initialize")

    assert(
      milvus.loadTable(
        Identifier.of(Array("database"), "collection")
      ) eq loadedTable
    )
    assert(milvus.name() == "milvus")
    assert(calls.size == 1)
    val call = calls.head
    assert(call.snapshotReference == SnapshotReference.Latest)
    assert(call.options(MilvusOption.MilvusUri) == "http://milvus:19530")
    assert(call.options(MilvusOption.FsRootPath) == "root")
    assert(call.options(MilvusOption.MilvusDatabaseName) == "database")
    assert(call.options(MilvusOption.MilvusCollectionName) == "collection")
    assert(
      call.options.keys.count(
        _.equalsIgnoreCase(MilvusOption.MilvusDatabaseName)
      ) == 1
    )
    assert(
      call.options.keys.count(
        _.equalsIgnoreCase(MilvusOption.MilvusCollectionName)
      ) == 1
    )
  }

  test("load overloads dispatch latest, name, and converted timestamp once") {
    val calls = ArrayBuffer.empty[LoadCall]
    val milvus = catalog(calls)
    milvus.initialize("milvus", CaseInsensitiveStringMap.empty())
    val identifier = Identifier.of(Array("database"), "collection")

    milvus.loadTable(identifier)
    milvus.loadTable(identifier, "snapshot-7")
    milvus.loadTable(identifier, 1000000L)

    assert(
      calls.map(_.snapshotReference) == Seq(
        SnapshotReference.Latest,
        SnapshotReference.Named("snapshot-7"),
        SnapshotReference.AtOrBefore((1000L << 18) | ((1L << 18) - 1L))
      )
    )
  }

  test(
    "hybrid timestamp conversion includes a whole millisecond without overflow"
  ) {
    val logicalMask = (1L << 18) - 1L

    assert(
      MilvusHybridTimestamp.upperBound(999999L) ==
        ((999L << 18) | logicalMask)
    )
    assert(
      MilvusHybridTimestamp.upperBound(1000000L) ==
        ((1000L << 18) | logicalMask)
    )
    assert(MilvusHybridTimestamp.upperBound(-1L) == Long.MinValue)
    assert(MilvusHybridTimestamp.upperBound(Long.MinValue) == Long.MinValue)
    assert(MilvusHybridTimestamp.upperBound(Long.MaxValue) == Long.MaxValue)

    val realSnapshotBoundary = 462324677975474190L
    val realSnapshotMillis = realSnapshotBoundary >>> 18
    assert(
      MilvusHybridTimestamp.upperBound(realSnapshotMillis * 1000L) >=
        realSnapshotBoundary
    )
  }

  test("invalid identifiers and blank versions fail before loading") {
    val calls = ArrayBuffer.empty[LoadCall]
    val milvus = catalog(calls)
    milvus.initialize("milvus", CaseInsensitiveStringMap.empty())

    Seq(
      Identifier.of(Array.empty, "collection"),
      Identifier.of(Array("one", "two"), "collection"),
      Identifier.of(Array(" "), "collection"),
      Identifier.of(Array("database"), " ")
    ).foreach { identifier =>
      val error = intercept[IllegalArgumentException](
        milvus.loadTable(identifier)
      )
      assert(error.getMessage.contains("milvus.<database>.<collection>"))
    }
    assertThrows[IllegalArgumentException](
      milvus.loadTable(
        Identifier.of(Array("database"), "collection"),
        "  "
      )
    )
    assert(calls.isEmpty)
  }

  test("listing and table mutations are explicitly unsupported") {
    val milvus = new MilvusCatalog
    val identifier = Identifier.of(Array("database"), "collection")
    val properties = ju.Collections.emptyMap[String, String]()

    assertThrows[UnsupportedOperationException](milvus.listTables(Array("db")))
    assertThrows[UnsupportedOperationException](
      milvus.createTable(
        identifier,
        Array.empty[Column],
        Array.empty[Transform],
        properties
      )
    )
    assertThrows[UnsupportedOperationException](
      milvus.alterTable(identifier)
    )
    assertThrows[UnsupportedOperationException](milvus.dropTable(identifier))
    assertThrows[UnsupportedOperationException](
      milvus.renameTable(identifier, identifier)
    )
  }

  test("confirmed absence maps to NoSuchTable without hiding other failures") {
    val identifier = Identifier.of(Array("database"), "collection")

    val missingCollection = new CollectionNotFoundException(
      "missing collection"
    )
    val wrappedMissingCollection = new IllegalArgumentException(
      "cannot resolve collection",
      missingCollection
    )
    val absent =
      new MilvusCatalogBase((_, _) => throw wrappedMissingCollection) {
        override def createTable(
            identifier: Identifier,
            columns: Array[Column],
            partitions: Array[Transform],
            properties: ju.Map[String, String]
        ): Table = unsupportedCreate()
      }
    absent.initialize("milvus", CaseInsensitiveStringMap.empty())
    assertThrows[NoSuchTableException](absent.loadTable(identifier))
    assert(!absent.tableExists(identifier))

    val missingSnapshot = new SnapshotNotFoundException("missing snapshot-7")
    val versioned = new MilvusCatalogBase((_, _) => throw missingSnapshot) {
      override def createTable(
          identifier: Identifier,
          columns: Array[Column],
          partitions: Array[Transform],
          properties: ju.Map[String, String]
      ): Table = unsupportedCreate()
    }
    versioned.initialize("milvus", CaseInsensitiveStringMap.empty())
    assert(
      intercept[SnapshotNotFoundException](
        versioned.loadTable(identifier, "snapshot-7")
      ) eq missingSnapshot
    )

    val infrastructureFailure = new IllegalStateException("storage unavailable")
    val milvus = new MilvusCatalogBase((_, _) => throw infrastructureFailure) {
      override def createTable(
          identifier: Identifier,
          columns: Array[Column],
          partitions: Array[Transform],
          properties: ju.Map[String, String]
      ): Table = unsupportedCreate()
    }
    milvus.initialize("milvus", CaseInsensitiveStringMap.empty())
    assert(
      intercept[IllegalStateException](milvus.loadTable(identifier)) eq
        infrastructureFailure
    )
  }

  test("Spark routes latest, version, and timestamp table loads") {
    RoutingMilvusCatalog.reset()
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("milvus-catalog-routing-test")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.catalog.milvus",
        classOf[MilvusCatalog].getName
      )
      .config(
        "spark.sql.catalog.routing",
        classOf[RoutingMilvusCatalog].getName
      )
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    try {
      spark.table("routing.database.collection").schema
      spark
        .sql(
          "SELECT * FROM routing.database.collection VERSION AS OF 'snapshot-7'"
        )
        .schema
      spark
        .sql(
          "SELECT * FROM routing.database.collection TIMESTAMP AS OF '1970-01-01 00:00:01'"
        )
        .schema

      assert(
        RoutingMilvusCatalog.references == Seq(
          SnapshotReference.Latest,
          SnapshotReference.Named("snapshot-7"),
          SnapshotReference.AtOrBefore((1000L << 18) | ((1L << 18) - 1L))
        )
      )

      val error = intercept[Exception](
        spark.table("milvus.database.collection").schema
      )
      val messages = Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .flatMap(e => Option(e.getMessage))
        .mkString(" | ")
      assert(messages.contains(MilvusOption.MilvusUri))
      assert(messages.contains("required"))
    } finally {
      spark.stop()
    }
  }
}
