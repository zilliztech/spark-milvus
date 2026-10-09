package com.zilliz.spark.connector.catalog

import java.{util => ju}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{
  NoSuchNamespaceException,
  TableAlreadyExistsException
}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{DataType => MilvusDataType}

private[catalog] object MilvusCatalogDdlTestSupport {
  def createRequest(
      identifier: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: ju.Map[String, String]
  ): MilvusCatalogCreate =
    MilvusCatalogCreate(
      identifier,
      Option(columns).getOrElse(Array.empty).toSeq.map { column =>
        MilvusCatalogColumn(
          column.name(),
          column.dataType(),
          column.nullable(),
          Option(column.comment()),
          column.defaultValue() != null,
          Option(column.generationExpression()),
          column.identityColumnSpec() != null
        )
      },
      Option(partitions).exists(_.nonEmpty),
      hasConstraints = false,
      Option(properties).map(_.asScala.toMap).getOrElse(Map.empty)
    )
}

/** A no-argument catalog that lets Spark SQL exercise the real Spark 4.0
  * adapter while recording only the remote DDL boundary.
  */
class DdlRoutingMilvusCatalog
    extends MilvusCatalogBase(
      ddlClientFactory = DdlRoutingMilvusCatalog.factory
    ) {
  override def createTable(
      identifier: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: ju.Map[String, String]
  ): Table =
    createTable(
      MilvusCatalogDdlTestSupport.createRequest(
        identifier,
        columns,
        partitions,
        properties
      )
    )
}

object DdlRoutingMilvusCatalog {
  private val opened = ArrayBuffer.empty[RecordingDdlClient]
  @volatile private var exists = false

  val factory: MilvusCatalogDdlClientFactory =
    new MilvusCatalogDdlClientFactory {
      override def open(
          options: CaseInsensitiveStringMap
      ): MilvusCatalogDdlClient = synchronized {
        val client = new RecordingDdlClient
        client.existsResult = exists
        opened += client
        client
      }
    }

  def reset(collectionExists: Boolean = false): Unit = synchronized {
    opened.clear()
    exists = collectionExists
  }

  def setCollectionExists(value: Boolean): Unit = exists = value

  def clients: Seq[RecordingDdlClient] = synchronized(opened.toSeq)
}

private[catalog] final class RecordingDdlClient extends MilvusCatalogDdlClient {
  val events = ArrayBuffer.empty[String]
  val collections = ArrayBuffer.empty[MilvusCatalogCollection]
  val indexes = ArrayBuffer.empty[MilvusCatalogIndex]
  var databaseExistsResult = true
  var databaseExistsFailure: Throwable = _
  var existsResult = false
  var existsFailure: Throwable = _
  var createFailure: Throwable = _
  var indexFailure: Throwable = _
  var dropFailure: Throwable = _
  var closeFailure: Throwable = _
  var closeCount = 0

  override def databaseExists(database: String): Boolean = {
    events += s"database:$database"
    if (databaseExistsFailure != null) throw databaseExistsFailure
    databaseExistsResult
  }

  override def collectionExists(
      database: String,
      collection: String
  ): Boolean = {
    events += s"exists:$database.$collection"
    if (existsFailure != null) throw existsFailure
    existsResult
  }

  override def createCollection(request: MilvusCatalogCollection): Unit = {
    events += s"create:${request.database}.${request.collection}"
    collections += request
    if (createFailure != null) throw createFailure
  }

  override def createIndex(
      request: MilvusCatalogCollection,
      index: MilvusCatalogIndex
  ): Unit = {
    events += s"index:${index.field}"
    indexes += index
    if (indexFailure != null) throw indexFailure
  }

  override def dropCollection(
      database: String,
      collection: String
  ): Unit = {
    events += s"drop:$database.$collection"
    if (dropFailure != null) throw dropFailure
  }

  override def close(): Unit = {
    closeCount += 1
    events += "close"
    if (closeFailure != null) throw closeFailure
  }
}

private[catalog] final class RecordingDdlFactory(
    nextClient: () => RecordingDdlClient
) extends MilvusCatalogDdlClientFactory {
  val options = ArrayBuffer.empty[Map[String, String]]
  val clients = ArrayBuffer.empty[RecordingDdlClient]

  override def open(
      catalogOptions: CaseInsensitiveStringMap
  ): MilvusCatalogDdlClient = {
    options += catalogOptions.asCaseSensitiveMap().asScala.toMap
    val client = nextClient()
    clients += client
    client
  }
}

class MilvusCatalogDdlTest extends AnyFunSuite {
  private val identifier = Identifier.of(Array("analytics"), "events")

  private final class InjectableCatalog(factory: MilvusCatalogDdlClientFactory)
      extends MilvusCatalogBase(ddlClientFactory = factory) {
    override def createTable(
        identifier: Identifier,
        columns: Array[Column],
        partitions: Array[Transform],
        properties: ju.Map[String, String]
    ): Table =
      createTable(
        MilvusCatalogDdlTestSupport.createRequest(
          identifier,
          columns,
          partitions,
          properties
        )
      )

    def create(request: MilvusCatalogCreate): Table = createTable(request)
  }

  private def request(
      columns: Seq[MilvusCatalogColumn],
      properties: Map[String, String],
      hasPartitionTransforms: Boolean = false,
      hasConstraints: Boolean = false
  ): MilvusCatalogCreate =
    MilvusCatalogCreate(
      identifier,
      columns,
      hasPartitionTransforms,
      hasConstraints,
      properties
    )

  private def column(
      name: String,
      dataType: DataType,
      nullable: Boolean = true,
      comment: Option[String] = None
  ): MilvusCatalogColumn =
    MilvusCatalogColumn(name, dataType, nullable, comment)

  private def initialize(catalog: MilvusCatalogBase): Unit = {
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.MilvusUri, "http://milvus:19530")
    options.put(MilvusOption.MilvusToken, "token")
    options.put("MILVUS.DATABASE.NAME", "configured-database")
    options.put("Milvus.Collection.Name", "configured-collection")
    catalog.initialize("milvus", new CaseInsensitiveStringMap(options))
  }

  private def validProperties: Map[String, String] =
    Map(MilvusCatalogDdl.PrimaryKeyProperty -> "id")

  test("DDL maps Spark scalar, varchar, and array fields exactly") {
    val definition = MilvusCatalogDdl.normalize(
      request(
        Seq(
          column("id", LongType, nullable = false, Some("row key")),
          column("flag", BooleanType),
          column("tiny", ByteType),
          column("small", ShortType),
          column("count", IntegerType),
          column("ratio", FloatType),
          column("score", DoubleType),
          column("title", StringType, comment = Some("display name")),
          column("tags", ArrayType(StringType, containsNull = false))
        ),
        validProperties ++ Map(
          "comment" -> "event collection",
          "milvus.field.title.data_type" -> "VaRcHaR",
          "milvus.field.title.max_length" -> "256",
          "milvus.field.tags.data_type" -> "array",
          "milvus.field.tags.max_capacity" -> "32",
          "milvus.field.tags.max_length" -> "64"
        )
      )
    )

    assert(definition.database == "analytics")
    assert(definition.collection == "events")
    assert(definition.description == "event collection")
    assert(definition.indexes.isEmpty)

    val fields = definition.fields.map(field => field.name -> field).toMap
    assert(fields("id").dataType == MilvusDataType.Int64)
    assert(fields("id").primaryKey)
    assert(!fields("id").nullable)
    assert(fields("id").description == "row key")
    assert(fields("flag").dataType == MilvusDataType.Bool)
    assert(fields("tiny").dataType == MilvusDataType.Int8)
    assert(fields("small").dataType == MilvusDataType.Int16)
    assert(fields("count").dataType == MilvusDataType.Int32)
    assert(fields("ratio").dataType == MilvusDataType.Float)
    assert(fields("score").dataType == MilvusDataType.Double)
    assert(fields("title").dataType == MilvusDataType.VarChar)
    assert(fields("title").description == "display name")
    assert(fields("title").typeParameters == Map("max_length" -> "256"))
    assert(fields("tags").dataType == MilvusDataType.Array)
    assert(fields("tags").elementType == MilvusDataType.VarChar)
    assert(
      fields("tags").typeParameters ==
        Map("max_capacity" -> "32", "max_length" -> "64")
    )
  }

  test("DDL validates vector dimensions and normalizes index JSON") {
    val definition = MilvusCatalogDdl.normalize(
      request(
        Seq(
          column("id", LongType, nullable = false),
          column("embedding", ArrayType(FloatType, containsNull = false)),
          column("bits", BinaryType),
          column("quantized", ArrayType(ShortType, containsNull = false)),
          column(
            "sparse",
            MapType(LongType, FloatType, valueContainsNull = false)
          )
        ),
        validProperties ++ Map(
          "milvus.field.embedding.data_type" -> "FlOaT_VeCtOr",
          "milvus.field.embedding.dim" -> "3",
          "milvus.index.embedding" ->
            """{"index_type":"HNSW","metric_type":"COSINE","index_name":"embedding_hnsw","M":16,"efConstruction":200,"cache":true}""",
          "milvus.field.bits.data_type" -> "binary_vector",
          "milvus.field.bits.dim" -> "16",
          "milvus.index.bits" ->
            """{"index_type":"BIN_FLAT","metric_type":"HAMMING"}""",
          "milvus.field.quantized.data_type" -> "int8_vector",
          "milvus.field.quantized.dim" -> "8",
          "milvus.index.quantized" ->
            """{"index_type":"IVF_FLAT","metric_type":"L2","nlist":128}""",
          "milvus.field.sparse.data_type" -> "sparse_float_vector",
          "milvus.index.sparse" ->
            """{"index_type":"SPARSE_INVERTED_INDEX","metric_type":"IP","drop_ratio_build":0.2}"""
        )
      )
    )

    val fields = definition.fields.map(field => field.name -> field).toMap
    assert(fields("embedding").dataType == MilvusDataType.FloatVector)
    assert(fields("embedding").typeParameters == Map("dim" -> "3"))
    assert(fields("bits").dataType == MilvusDataType.BinaryVector)
    assert(fields("bits").typeParameters == Map("dim" -> "16"))
    assert(fields("quantized").dataType == MilvusDataType.Int8Vector)
    assert(fields("quantized").typeParameters == Map("dim" -> "8"))
    assert(fields("sparse").dataType == MilvusDataType.SparseFloatVector)
    assert(fields("sparse").typeParameters.isEmpty)

    val indexes = definition.indexes.map(index => index.field -> index).toMap
    assert(indexes("embedding").name == "embedding_hnsw")
    assert(
      indexes("embedding").parameters == Map(
        "index_type" -> "HNSW",
        "metric_type" -> "COSINE",
        "M" -> "16",
        "efConstruction" -> "200",
        "cache" -> "true"
      )
    )
    assert(indexes("bits").name.isEmpty)
    assert(indexes("bits").parameters("metric_type") == "HAMMING")
    assert(indexes("quantized").parameters("nlist") == "128")
    assert(indexes("sparse").parameters("drop_ratio_build") == "0.2")
  }

  test("ambiguous and invalid definitions fail before a client is opened") {
    val factory = new RecordingDdlFactory(() => new RecordingDdlClient)
    val catalog = new InjectableCatalog(factory)
    initialize(catalog)

    val invalid = Seq(
      "string type" -> request(
        Seq(column("id", LongType, false), column("value", StringType)),
        validProperties
      ),
      "array type" -> request(
        Seq(
          column("id", LongType, false),
          column("value", ArrayType(IntegerType))
        ),
        validProperties
      ),
      "non-round-tripping byte array" -> request(
        Seq(
          column("id", LongType, false),
          column("value", ArrayType(ByteType))
        ),
        validProperties ++ Map(
          "milvus.field.value.data_type" -> "array",
          "milvus.field.value.max_capacity" -> "16"
        )
      ),
      "binary type" -> request(
        Seq(column("id", LongType, false), column("value", BinaryType)),
        validProperties
      ),
      "map type" -> request(
        Seq(
          column("id", LongType, false),
          column("value", MapType(LongType, FloatType))
        ),
        validProperties
      ),
      "unsupported Spark type" -> request(
        Seq(
          column("id", LongType, false),
          column("value", StructType(Seq(StructField("nested", LongType))))
        ),
        validProperties
      ),
      "missing primary key" -> request(
        Seq(column("id", LongType, false)),
        Map.empty
      ),
      "unknown primary key" -> request(
        Seq(column("id", LongType, false)),
        Map(MilvusCatalogDdl.PrimaryKeyProperty -> "missing")
      ),
      "nullable primary key" -> request(
        Seq(column("id", LongType, nullable = true)),
        validProperties
      ),
      "invalid primary key type" -> request(
        Seq(column("id", IntegerType, nullable = false)),
        validProperties
      ),
      "unknown field property" -> request(
        Seq(column("id", LongType, false)),
        validProperties + ("milvus.field.missing.dim" -> "4")
      ),
      "unknown Milvus property" -> request(
        Seq(column("id", LongType, false)),
        validProperties + ("milvus.unknown" -> "value")
      ),
      "missing vector dimension" -> request(
        Seq(
          column("id", LongType, false),
          column("embedding", ArrayType(FloatType, containsNull = false))
        ),
        validProperties ++ Map(
          "milvus.field.embedding.data_type" -> "float_vector",
          "milvus.index.embedding" ->
            """{"index_type":"FLAT","metric_type":"L2"}"""
        )
      ),
      "missing vector index" -> request(
        Seq(
          column("id", LongType, false),
          column("embedding", ArrayType(FloatType, containsNull = false))
        ),
        validProperties ++ Map(
          "milvus.field.embedding.data_type" -> "float_vector",
          "milvus.field.embedding.dim" -> "4"
        )
      ),
      "invalid binary vector dimension" -> request(
        Seq(column("id", LongType, false), column("bits", BinaryType)),
        validProperties ++ Map(
          "milvus.field.bits.data_type" -> "binary_vector",
          "milvus.field.bits.dim" -> "7",
          "milvus.index.bits" ->
            """{"index_type":"BIN_FLAT","metric_type":"HAMMING"}"""
        )
      ),
      "nested index parameter" -> request(
        Seq(
          column("id", LongType, false),
          column("embedding", ArrayType(FloatType, containsNull = false))
        ),
        validProperties ++ Map(
          "milvus.field.embedding.data_type" -> "float_vector",
          "milvus.field.embedding.dim" -> "4",
          "milvus.index.embedding" ->
            """{"index_type":"HNSW","metric_type":"L2","params":{"M":16}}"""
        )
      ),
      "partition transform" -> request(
        Seq(column("id", LongType, false)),
        validProperties,
        hasPartitionTransforms = true
      ),
      "table constraint" -> request(
        Seq(column("id", LongType, false)),
        validProperties,
        hasConstraints = true
      )
    )

    invalid.foreach { case (description, definition) =>
      withClue(description) {
        assertThrows[IllegalArgumentException](catalog.create(definition))
      }
    }

    val featureColumns = Seq(
      column("id", LongType, false).copy(hasDefault = true),
      column("id", LongType, false).copy(
        generationExpression = Some("id + 1")
      ),
      column("id", LongType, false).copy(hasIdentity = true)
    )
    featureColumns.foreach { feature =>
      assertThrows[IllegalArgumentException](
        catalog.create(request(Seq(feature), validProperties))
      )
    }

    assert(factory.clients.isEmpty)
    assert(factory.options.isEmpty)
  }

  test("create submits one validated collection then its vector indexes") {
    val client = new RecordingDdlClient
    val factory = new RecordingDdlFactory(() => client)
    val catalog = new InjectableCatalog(factory)
    initialize(catalog)
    val definition = request(
      Seq(
        column("id", LongType, false),
        column("embedding", ArrayType(FloatType, containsNull = false))
      ),
      validProperties ++ Map(
        "milvus.field.embedding.data_type" -> "float_vector",
        "milvus.field.embedding.dim" -> "4",
        "milvus.index.embedding" ->
          """{"index_type":"HNSW","metric_type":"COSINE","M":16}"""
      )
    )

    assert(catalog.create(definition) == null)
    assert(
      client.events == Seq(
        "database:analytics",
        "exists:analytics.events",
        "create:analytics.events",
        "index:embedding",
        "close"
      )
    )
    assert(client.collections.size == 1)
    assert(client.indexes.size == 1)
    assert(client.closeCount == 1)
    assert(factory.options.size == 1)
    val openedWith = factory.options.head
    assert(openedWith(MilvusOption.MilvusDatabaseName) == "analytics")
    assert(openedWith(MilvusOption.MilvusUri) == "http://milvus:19530")
    assert(openedWith(MilvusOption.MilvusToken) == "token")
    assert(
      !openedWith.keys.exists(
        _.equalsIgnoreCase(MilvusOption.MilvusCollectionName)
      )
    )
  }

  test("create preflight rejects a missing database or existing collection") {
    val definition = request(
      Seq(column("id", LongType, nullable = false)),
      validProperties
    )

    val missingDatabase = new RecordingDdlClient
    missingDatabase.databaseExistsResult = false
    val missingCatalog = new InjectableCatalog(
      new RecordingDdlFactory(() => missingDatabase)
    )
    initialize(missingCatalog)
    assertThrows[NoSuchNamespaceException](missingCatalog.create(definition))
    assert(
      missingDatabase.events == Seq(
        "database:analytics",
        "close"
      )
    )

    val existingCollection = new RecordingDdlClient
    existingCollection.existsResult = true
    val existingCatalog = new InjectableCatalog(
      new RecordingDdlFactory(() => existingCollection)
    )
    initialize(existingCatalog)
    assertThrows[TableAlreadyExistsException](
      existingCatalog.create(definition)
    )
    assert(
      existingCollection.events == Seq(
        "database:analytics",
        "exists:analytics.events",
        "close"
      )
    )
    assert(existingCollection.collections.isEmpty)
  }

  test("create preserves collection and index failures and always closes") {
    val collectionFailure = new IllegalStateException("collection rejected")
    val failedCollection = new RecordingDdlClient
    failedCollection.createFailure = collectionFailure
    val collectionFactory = new RecordingDdlFactory(() => failedCollection)
    val collectionCatalog = new InjectableCatalog(collectionFactory)
    initialize(collectionCatalog)
    val scalarDefinition = request(
      Seq(column("id", LongType, false)),
      validProperties
    )

    assert(
      intercept[IllegalStateException](
        collectionCatalog.create(scalarDefinition)
      ) eq collectionFailure
    )
    assert(
      failedCollection.events == Seq(
        "database:analytics",
        "exists:analytics.events",
        "create:analytics.events",
        "close"
      )
    )

    val indexFailure = new IllegalArgumentException("index rejected")
    val failedIndex = new RecordingDdlClient
    failedIndex.indexFailure = indexFailure
    val indexFactory = new RecordingDdlFactory(() => failedIndex)
    val indexCatalog = new InjectableCatalog(indexFactory)
    initialize(indexCatalog)
    val vectorDefinition = request(
      Seq(
        column("id", LongType, false),
        column("embedding", ArrayType(FloatType, containsNull = false))
      ),
      validProperties ++ Map(
        "milvus.field.embedding.data_type" -> "float_vector",
        "milvus.field.embedding.dim" -> "4",
        "milvus.index.embedding" ->
          """{"index_type":"FLAT","metric_type":"L2"}"""
      )
    )

    assert(
      intercept[IllegalArgumentException](
        indexCatalog.create(vectorDefinition)
      ) eq indexFailure
    )
    assert(
      failedIndex.events == Seq(
        "database:analytics",
        "exists:analytics.events",
        "create:analytics.events",
        "index:embedding",
        "close"
      )
    )
    assert(failedIndex.collections.size == 1)
    assert(failedIndex.closeCount == 1)
  }

  test("close failures preserve the operation failure as the primary error") {
    val operationFailure = new IllegalStateException("create failed")
    val closeFailure = new IllegalArgumentException("close failed")
    val client = new RecordingDdlClient
    client.createFailure = operationFailure
    client.closeFailure = closeFailure
    val catalog = new InjectableCatalog(
      new RecordingDdlFactory(() => client)
    )
    initialize(catalog)

    val error = intercept[IllegalStateException](
      catalog.create(
        request(Seq(column("id", LongType, false)), validProperties)
      )
    )
    assert(error eq operationFailure)
    assert(error.getSuppressed.toSeq == Seq(closeFailure))
    assert(client.closeCount == 1)

    val successfulOperation = new RecordingDdlClient
    successfulOperation.closeFailure = closeFailure
    val closeCatalog = new InjectableCatalog(
      new RecordingDdlFactory(() => successfulOperation)
    )
    initialize(closeCatalog)
    assert(
      intercept[IllegalArgumentException](
        closeCatalog.create(
          request(Seq(column("id", LongType, false)), validProperties)
        )
      ) eq closeFailure
    )
  }

  test("drop returns true or false only for confirmed presence or absence") {
    def dropWith(client: RecordingDdlClient): (Boolean, RecordingDdlClient) = {
      val catalog = new InjectableCatalog(
        new RecordingDdlFactory(() => client)
      )
      initialize(catalog)
      catalog.dropTable(identifier) -> client
    }

    val present = new RecordingDdlClient
    present.existsResult = true
    assert(dropWith(present)._1)
    assert(
      present.events == Seq(
        "database:analytics",
        "exists:analytics.events",
        "drop:analytics.events",
        "close"
      )
    )

    val absent = new RecordingDdlClient
    absent.existsResult = false
    assert(!dropWith(absent)._1)
    assert(
      absent.events == Seq(
        "database:analytics",
        "exists:analytics.events",
        "close"
      )
    )

    val missingDatabase = new RecordingDdlClient
    missingDatabase.databaseExistsResult = false
    assert(!dropWith(missingDatabase)._1)
    assert(
      missingDatabase.events == Seq(
        "database:analytics",
        "close"
      )
    )

    val lookupFailure = new IllegalStateException("lookup unavailable")
    val failedLookup = new RecordingDdlClient
    failedLookup.existsFailure = lookupFailure
    assert(
      intercept[IllegalStateException](dropWith(failedLookup)) eq lookupFailure
    )
    assert(failedLookup.closeCount == 1)

    val dropFailure = new IllegalArgumentException("drop denied")
    val failedDrop = new RecordingDdlClient
    failedDrop.existsResult = true
    failedDrop.dropFailure = dropFailure
    assert(
      intercept[IllegalArgumentException](dropWith(failedDrop)) eq dropFailure
    )
    assert(failedDrop.closeCount == 1)
  }

  test("Spark SQL routes CREATE TABLE and DROP TABLE through catalog DDL") {
    DdlRoutingMilvusCatalog.reset(collectionExists = false)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("milvus-catalog-ddl-routing-test")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.catalog.ddl",
        classOf[DdlRoutingMilvusCatalog].getName
      )
      .config("spark.sql.catalog.ddl.milvus.uri", "http://milvus:19530")
      .getOrCreate()

    try {
      spark
        .sql(
          """CREATE TABLE ddl.analytics.events (
            |  id BIGINT NOT NULL,
            |  embedding ARRAY<FLOAT>
            |) USING milvus
            |TBLPROPERTIES (
            |  'milvus.primary.key' = 'id',
            |  'milvus.field.embedding.data_type' = 'float_vector',
            |  'milvus.field.embedding.dim' = '4',
            |  'milvus.index.embedding' = '{"index_type":"HNSW","metric_type":"COSINE","M":16}'
            |)""".stripMargin
        )
        .collect()

      val createClients = DdlRoutingMilvusCatalog.clients
      assert(createClients.size == 2)
      assert(
        createClients.head.events == Seq(
          "database:analytics",
          "exists:analytics.events",
          "close"
        )
      )
      val createClient = createClients(1)
      assert(
        createClient.events == Seq(
          "database:analytics",
          "exists:analytics.events",
          "create:analytics.events",
          "index:embedding",
          "close"
        )
      )
      assert(
        createClient.collections.head.fields.find(_.name == "id").get.primaryKey
      )

      DdlRoutingMilvusCatalog.setCollectionExists(true)
      spark.sql("DROP TABLE ddl.analytics.events").collect()
      val dropClients = DdlRoutingMilvusCatalog.clients.drop(2)
      assert(dropClients.size == 2)
      assert(
        dropClients.head.events == Seq(
          "database:analytics",
          "exists:analytics.events",
          "close"
        )
      )
      val dropClient = dropClients(1)
      assert(
        dropClient.events == Seq(
          "database:analytics",
          "exists:analytics.events",
          "drop:analytics.events",
          "close"
        )
      )

      DdlRoutingMilvusCatalog.setCollectionExists(false)
      spark.sql("DROP TABLE IF EXISTS ddl.analytics.missing").collect()
      val absentClients = DdlRoutingMilvusCatalog.clients.drop(4)
      assert(absentClients.size == 1)
      val absentClient = absentClients.head
      assert(
        absentClient.events == Seq(
          "database:analytics",
          "exists:analytics.missing",
          "close"
        )
      )
      assert(DdlRoutingMilvusCatalog.clients.forall(_.closeCount == 1))
    } finally {
      spark.stop()
    }
  }
}
